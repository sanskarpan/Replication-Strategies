// Package persistence wraps a SQLite database for cluster and event-history
// durability.  It intentionally knows nothing about simulation types — all
// data is exchanged as raw JSON bytes so there is no import cycle.
package persistence

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // registers the "sqlite" driver
)

// Store is a persistence handle backed by a single SQLite file.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) a SQLite database at path.
// The parent directory is created when it does not exist.
// Pass ":memory:" for a transient in-process database (useful in tests).
func Open(path string) (*Store, error) {
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, fmt.Errorf("persistence: mkdir %s: %w", filepath.Dir(path), err)
		}
	}
	// Pragmas via DSN keep the connection tuned before any SQL runs.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
	if path == ":memory:" {
		dsn = ":memory:?_pragma=foreign_keys(ON)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("persistence: open %s: %w", path, err)
	}
	// SQLite WAL mode allows one writer at a time; cap to 1 to avoid SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database connection.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS clusters (
			id            TEXT PRIMARY KEY,
			config_json   TEXT NOT NULL,
			node_ids_json TEXT NOT NULL,
			leader_id     TEXT NOT NULL DEFAULT '',
			created_at    INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS cluster_history (
			cluster_id TEXT    NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
			seq        INTEGER NOT NULL,
			event_json TEXT    NOT NULL,
			state_json TEXT,
			PRIMARY KEY (cluster_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ch_cluster_seq ON cluster_history(cluster_id, seq)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("persistence: migrate: %w", err)
		}
	}
	return nil
}

// ─── Cluster records ─────────────────────────────────────────────────────────

// ClusterRecord is a row from the clusters table.
type ClusterRecord struct {
	ID        string
	Config    []byte // raw config JSON
	NodeIDs   []string
	LeaderID  string
	CreatedAt int64 // UnixNano
}

// SaveCluster upserts a cluster record (insert on first create, update on config/node change).
func (s *Store) SaveCluster(id string, configJSON []byte, nodeIDs []string, leaderID string, createdAt int64) error {
	nodeIDsJSON, err := json.Marshal(nodeIDs)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO clusters(id, config_json, node_ids_json, leader_id, created_at)
		 VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET
		     config_json   = excluded.config_json,
		     node_ids_json = excluded.node_ids_json,
		     leader_id     = excluded.leader_id`,
		id, string(configJSON), string(nodeIDsJSON), leaderID, createdAt,
	)
	return err
}

// DeleteCluster removes a cluster and all its history rows (CASCADE).
func (s *Store) DeleteCluster(id string) error {
	_, err := s.db.Exec(`DELETE FROM clusters WHERE id = ?`, id)
	return err
}

// DeleteAllClusters removes every cluster (and history via CASCADE).
func (s *Store) DeleteAllClusters() error {
	_, err := s.db.Exec(`DELETE FROM clusters`)
	return err
}

// LoadClusters returns all persisted cluster records, ordered by created_at ASC.
func (s *Store) LoadClusters() ([]ClusterRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, config_json, node_ids_json, leader_id, created_at
		 FROM clusters ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ClusterRecord
	for rows.Next() {
		var r ClusterRecord
		var nodeIDsJSON string
		if err := rows.Scan(&r.ID, &r.Config, &nodeIDsJSON, &r.LeaderID, &r.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(nodeIDsJSON), &r.NodeIDs); err != nil {
			return nil, err
		}
		// Config is already []byte (raw JSON), no further unmarshalling needed here.
		out = append(out, r)
	}
	return out, rows.Err()
}

// ─── History records ─────────────────────────────────────────────────────────

// HistoryRow is a raw row from cluster_history.
type HistoryRow struct {
	Seq       uint64
	EventJSON []byte
	StateJSON []byte // nil when no snapshot was captured
}

// AppendHistoryEntry inserts one history entry.
// stateJSON may be nil for entries without a cluster-state snapshot.
// INSERT OR IGNORE is used so re-importing a DB does not error on duplicates.
func (s *Store) AppendHistoryEntry(clusterID string, seq uint64, eventJSON, stateJSON []byte) error {
	var stateArg interface{}
	if stateJSON != nil {
		stateArg = string(stateJSON)
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO cluster_history(cluster_id, seq, event_json, state_json)
		 VALUES(?,?,?,?)`,
		clusterID, seq, string(eventJSON), stateArg,
	)
	return err
}

// LoadHistory returns up to limit of the most-recent entries for clusterID,
// sorted by seq ASC so callers can append them to a ring buffer in order.
func (s *Store) LoadHistory(clusterID string, limit int) ([]HistoryRow, error) {
	rows, err := s.db.Query(
		`SELECT seq, event_json, state_json FROM (
		     SELECT seq, event_json, state_json
		     FROM cluster_history
		     WHERE cluster_id = ?
		     ORDER BY seq DESC LIMIT ?
		 ) ORDER BY seq ASC`,
		clusterID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HistoryRow
	for rows.Next() {
		var r HistoryRow
		var stateJSON sql.NullString
		if err := rows.Scan(&r.Seq, &r.EventJSON, &stateJSON); err != nil {
			return nil, err
		}
		if stateJSON.Valid {
			r.StateJSON = []byte(stateJSON.String)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
