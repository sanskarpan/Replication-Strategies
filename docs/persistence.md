# State Persistence

By default the simulator is purely in-memory: clusters exist only for the lifetime of
the server process. **SQLite persistence** makes cluster configuration and event history
survive server restarts.

---

## Enabling persistence

Set `sqlite_path` in `config.yaml`:

```yaml
persistence:
  sqlite_path: "./data/simulation.db"
```

Or use the environment variable:

```bash
SQLITE_PATH=./data/simulation.db go run ./cmd/server
```

The parent directory (`./data/`) is created automatically with `0750` permissions if it
doesn't exist.

To **disable** persistence, leave `sqlite_path` empty or unset.

---

## What is persisted

| Table | What |
|-------|------|
| `clusters` | Cluster ID, strategy config, node IDs, current leader, creation timestamp |
| `cluster_history` | Per-cluster event log: sequence number, event JSON, optional state snapshot JSON |

The `cluster_history` table has an `ON DELETE CASCADE` foreign key to `clusters`, so
deleting a cluster via the API also removes all its stored history.

---

## Restore on startup

When the server starts with a configured `sqlite_path`, it:

1. Opens the SQLite database (creates it with WAL mode if new).
2. Loads all cluster records.
3. For each cluster, loads the stored event history (up to `historyMaxSize` entries).
4. **Pre-populates the in-memory ring buffer** with the stored history so event
   sequence numbers continue from where they left off — not from zero.
5. Recreates the cluster in the orchestrator and starts all nodes.

This means a restart is transparent: the UI reconnects, the cluster is alive, and the
timeline scrubber shows the full history including events from before the restart.

---

## SQLite configuration

The driver is `modernc.org/sqlite` — a pure-Go CGo-free port. The DSN pragmas applied
at open time:

| Pragma | Value | Purpose |
|--------|-------|---------|
| `journal_mode` | `WAL` | Write-Ahead Logging for better concurrent read performance |
| `foreign_keys` | `ON` | Enforce the `cluster_history → clusters` cascade |
| `busy_timeout` | `5000` | 5-second retry window on write contention |

The connection pool is limited to **1 writer** (`db.SetMaxOpenConns(1)`) because WAL
mode handles concurrent reads but still requires a single writer.

---

## Ephemeral mode (tests and CI)

Use `:memory:` to get a fully functional but non-persistent SQLite database:

```bash
SQLITE_PATH=:memory: go run ./cmd/server
```

This is what the E2E test suite uses. The database lives only for the duration of the
process.

---

## Disabling persistence

Set `sqlite_path` to an empty string or leave it out of `config.yaml`:

```yaml
persistence:
  sqlite_path: ""
```

The orchestrator runs in pure in-memory mode — no SQLite dependency at all.
