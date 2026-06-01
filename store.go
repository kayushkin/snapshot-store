// Package snapshotstore captures point-in-time file contents for tool-call
// before/after diffs.
//
// A Store has two backends:
//   - SQLite: metadata rows keyed by (session_id, tool_use_id, phase,
//     file_path), recording blob SHA, size, and flags (is_binary, too_large,
//     missing). The file_path is part of the PK so a single tool call (e.g.
//     a Bash command touching N files) can record many snapshots per phase.
//   - A bare git repo: raw file contents stored as content-addressed blobs
//     via `git hash-object -w`. Git handles delta compression and dedup;
//     identical content across many tool calls costs ~no additional disk.
//
// The store is a pure data layer. Deciding when to capture (which tools,
// which events) is the caller's responsibility.
package snapshotstore

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store wraps the snapshot SQLite database and the git blob sidecar.
type Store struct {
	db      *sql.DB
	gitDir  string
	maxSize int64
}

// Config configures a Store.
type Config struct {
	// DBPath is the SQLite database path. Parent dir is created if missing.
	DBPath string
	// GitDir is the bare git repo used as the blob store. Initialized on
	// first use if missing.
	GitDir string
	// MaxSize is the per-file capture cap in bytes. Files larger than this
	// are recorded with too_large=true and no blob. 0 means 1 MiB default.
	MaxSize int64
}

const defaultMaxSize = 1 << 20 // 1 MiB

// Open opens or creates a snapshot store.
func Open(cfg Config) (*Store, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("snapshotstore: DBPath is required")
	}
	if cfg.GitDir == "" {
		return nil, fmt.Errorf("snapshotstore: GitDir is required")
	}

	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite pragmas: %w", err)
	}

	max := cfg.MaxSize
	if max <= 0 {
		max = defaultMaxSize
	}

	s := &Store{db: db, gitDir: cfg.GitDir, maxSize: max}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.initBlobRepo(); err != nil {
		db.Close()
		return nil, fmt.Errorf("init blob repo: %w", err)
	}
	return s, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// DB returns the underlying database connection for advanced queries.
func (s *Store) DB() *sql.DB {
	return s.db
}

// GitDir returns the bare git directory used for blob storage.
func (s *Store) GitDir() string {
	return s.gitDir
}

// MaxSize returns the configured per-file capture cap in bytes.
func (s *Store) MaxSize() int64 {
	return s.maxSize
}

// migrate brings the schema to the current version. Steps must be idempotent
// and ordered — bumping user_version after each completed step lets fresh
// installs and existing DBs converge to the same state.
func (s *Store) migrate() error {
	// v1: initial schema with PK (session_id, tool_use_id, phase).
	if _, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS snapshots (
			session_id   TEXT NOT NULL,
			tool_use_id  TEXT NOT NULL,
			phase        TEXT NOT NULL,
			file_path    TEXT NOT NULL,
			blob_sha     TEXT NOT NULL DEFAULT '',
			size         INTEGER NOT NULL DEFAULT 0,
			is_binary    INTEGER NOT NULL DEFAULT 0,
			too_large    INTEGER NOT NULL DEFAULT 0,
			missing      INTEGER NOT NULL DEFAULT 0,
			created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (session_id, tool_use_id, phase),
			CHECK (phase IN ('before','after'))
		);
		CREATE INDEX IF NOT EXISTS idx_snapshots_created ON snapshots(created_at);
		CREATE INDEX IF NOT EXISTS idx_snapshots_sha ON snapshots(blob_sha);
	`); err != nil {
		return err
	}

	var version int
	if err := s.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read user_version: %w", err)
	}

	// v2: widen PK to include file_path so one tool call can record snapshots
	// of multiple files (e.g. a Bash command that touches N paths).
	if version < 2 {
		if err := s.migrateV2(); err != nil {
			return fmt.Errorf("migrate v2: %w", err)
		}
		if _, err := s.db.Exec(`PRAGMA user_version = 2`); err != nil {
			return fmt.Errorf("bump user_version to 2: %w", err)
		}
	}

	return nil
}

// migrateV2 rebuilds the snapshots table with file_path included in the PK.
// SQLite has no in-place way to alter a primary key, so we do the standard
// create-new / copy / drop / rename dance inside a transaction.
func (s *Store) migrateV2() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmts := []string{
		`CREATE TABLE snapshots_v2 (
			session_id   TEXT NOT NULL,
			tool_use_id  TEXT NOT NULL,
			phase        TEXT NOT NULL,
			file_path    TEXT NOT NULL,
			blob_sha     TEXT NOT NULL DEFAULT '',
			size         INTEGER NOT NULL DEFAULT 0,
			is_binary    INTEGER NOT NULL DEFAULT 0,
			too_large    INTEGER NOT NULL DEFAULT 0,
			missing      INTEGER NOT NULL DEFAULT 0,
			created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (session_id, tool_use_id, phase, file_path),
			CHECK (phase IN ('before','after'))
		)`,
		`INSERT INTO snapshots_v2
			SELECT session_id, tool_use_id, phase, file_path, blob_sha, size,
			       is_binary, too_large, missing, created_at
			FROM snapshots`,
		`DROP TABLE snapshots`,
		`ALTER TABLE snapshots_v2 RENAME TO snapshots`,
		`CREATE INDEX IF NOT EXISTS idx_snapshots_created ON snapshots(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_snapshots_sha ON snapshots(blob_sha)`,
	}
	for _, sqlStmt := range stmts {
		if _, err := tx.Exec(sqlStmt); err != nil {
			return fmt.Errorf("exec %q: %w", sqlStmt, err)
		}
	}
	return tx.Commit()
}
