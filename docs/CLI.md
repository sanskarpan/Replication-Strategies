# replsim — Command-Line Interface

`replsim` is a self-contained CLI for the replication-strategies simulator. It
embeds the orchestrator **in-process** (no HTTP server, no ports) using
`internal/simulation` + `internal/events`, so you can explore scenarios,
real-system presets, and correctness checks straight from the terminal.

It depends only on the Go standard library (`flag` + subcommand dispatch) — no
external CLI framework.

## Build & Run

```sh
go build ./cmd/replsim
./replsim <subcommand> [flags]
```

or run without building an artifact:

```sh
go run ./cmd/replsim <subcommand> [flags]
```

## Subcommands

### `list-scenarios`

Prints the built-in teaching scenarios (`simulation.Scenarios`): each scenario's
name, strategy, node count, and description.

```sh
replsim list-scenarios
```

### `list-presets`

Prints the real-system presets (`simulation.ListPresets()`) that map well-known
distributed systems (Cassandra, DynamoDB, PostgreSQL, etcd, Kafka) onto a
simulator cluster config: name, system, and description.

```sh
replsim list-presets
```

### `run -scenario NAME`

Creates and runs the named scenario via `Orchestrator.RunScenario`, waits for its
background setup to play out (default ~2s, tunable with `-wait`), then prints
three correctness reports:

- **Convergence** (`CheckConvergence`) — do all online replicas agree on every key?
- **Linearizability** (`CheckLinearizable`) — is the observed client history linearizable?
- **Invariants** (`CheckInvariants`) — the combined always-on correctness snapshot.

**Exit code:** `0` if the invariants hold, `1` if any invariant is violated
(`2` on usage/lookup errors).

```sh
replsim run -scenario ReplicationLag
replsim run -scenario MultiLeaderConflict -wait 3s
```

Flags:

| Flag        | Default | Description                                              |
| ----------- | ------- | -------------------------------------------------------- |
| `-scenario` | *(req)* | Scenario name (see `list-scenarios`).                    |
| `-wait`     | `2s`    | How long to let the scenario play out before checking.   |

### `check -strategy S -nodes N [-w W -r R]`

Provisions a cluster from the flags, runs a short write+read workload with a
single client, then asserts the always-on invariants and prints a `PASS`/`FAIL`
report.

For `leaderless` clusters the quorum config is echoed along with whether the
configuration is strongly consistent (`W+R>N`, with the guaranteed overlap
count) or eventually consistent (`W+R<=N`, with an estimated stale-read
probability).

**Exit code:** `0` on `PASS`, non-zero (`1`) on any invariant violation.

```sh
replsim check -strategy single_leader -nodes 3
replsim check -strategy leaderless -nodes 5 -w 3 -r 3
replsim check -strategy raft -nodes 5
```

Flags:

| Flag        | Default          | Description                                                       |
| ----------- | ---------------- | ---------------------------------------------------------------- |
| `-strategy` | `single_leader`  | `single_leader` \| `multi_leader` \| `leaderless` \| `raft`.     |
| `-nodes`    | `3`              | Number of nodes in the cluster.                                  |
| `-w`        | `0` (default)    | Write quorum W (leaderless only; `0` uses the strategy default). |
| `-r`        | `0` (default)    | Read quorum R (leaderless only; `0` uses the strategy default).  |

## Exit Codes

| Code | Meaning                                                        |
| ---- | ------------------------------------------------------------- |
| `0`  | Success — invariants held (`run`) or check passed (`check`). |
| `1`  | Invariant violation detected.                                |
| `2`  | Usage error (unknown subcommand, missing flag, bad lookup).  |
