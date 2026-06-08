package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// migration is one irreversible step in the schema's history.
//
// Migrations are an ordered slice rather than a directory of files: the schema
// is small enough to read in one sitting, and keeping it in the binary means
// there is no way to deploy a build whose code and schema disagree because
// somebody forgot to ship the .sql files.
//
// There is no Down. A rollback of a released migration is a new migration, so
// that the schema's history is append-only in exactly the way the workflow
// history it stores is.
type migration struct {
	version int
	name    string
	stmts   []string
}

// migrations must stay append-only and ordered. Editing a released migration
// changes the schema of new databases without changing existing ones, which is
// the single easiest way to produce a fleet that disagrees with itself.
var migrations = []migration{
	{
		version: 1,
		name:    "initial schema",
		stmts: []string{
			// -----------------------------------------------------------------
			// executions: one row per run.
			//
			// Times are unix nanoseconds. An integer keeps ordering, range
			// scans and the pagination cursor exact, where a text timestamp
			// would depend on formatting and a float would lose precision above
			// microseconds. Zero means "unset" -- the unix epoch is not a
			// plausible workflow timestamp, and reserving it saves a nullable
			// column on the hottest table.
			// -----------------------------------------------------------------
			`CREATE TABLE executions (
				namespace              TEXT    NOT NULL,
				workflow_id            TEXT    NOT NULL,
				run_id                 TEXT    NOT NULL,
				workflow_type          TEXT    NOT NULL,
				task_queue             TEXT    NOT NULL,
				status                 INTEGER NOT NULL,
				started_at             INTEGER NOT NULL,
				closed_at              INTEGER NOT NULL DEFAULT 0,
				version                INTEGER NOT NULL,
				last_event_id          INTEGER NOT NULL,
				-- last_event_time is denormalised from history_events so that
				-- validating an append's timestamps needs no second query on
				-- the engine's hottest path. It is also the column an operator
				-- means by "when did this workflow last do anything".
				last_event_time        INTEGER NOT NULL DEFAULT 0,
				first_execution_run_id TEXT    NOT NULL,
				request_id             TEXT    NOT NULL DEFAULT '',
				memo                   TEXT    NOT NULL DEFAULT '',
				search_attrs           TEXT    NOT NULL DEFAULT '',
				PRIMARY KEY (namespace, workflow_id, run_id)
			)`,

			// Serves the current-run lookup
			//   SELECT ... WHERE namespace = ? AND workflow_id = ?
			//   ORDER BY started_at DESC, run_id DESC LIMIT 1
			// and every visibility page filtered by workflow ID. The implicit
			// index behind the primary key cannot serve either, because it
			// orders by run_id and the API orders by start time.
			`CREATE INDEX executions_by_workflow
				ON executions (namespace, workflow_id, started_at DESC, run_id DESC)`,

			// Serves OpenExecutions -- WHERE namespace = ? AND status = 0 --
			// and status-filtered visibility. The recovery scan runs at every
			// startup over the whole table, so it is the one query that must
			// not degrade as closed runs accumulate.
			`CREATE INDEX executions_by_status
				ON executions (namespace, status, started_at DESC, run_id DESC)`,

			// Serves an unfiltered visibility page. The trailing columns are
			// exactly the keyset cursor, so a page is a bounded range scan and
			// never an OFFSET.
			//
			// A workflow_type filter has no index of its own on purpose: it is
			// answered by scanning this one and discarding non-matches. Adding
			// a fourth index to make an uncommon filter fast would slow down
			// every write, and a deployment that queries by type at scale wants
			// a real search index, not a fourth B-tree here.
			`CREATE INDEX executions_recent
				ON executions (namespace, started_at DESC, run_id DESC)`,

			// Enforces start deduplication. Partial, so that the overwhelmingly
			// common case -- no request ID -- costs nothing and, more
			// importantly, so that the empty string does not collide with
			// itself across every run of a workflow.
			`CREATE UNIQUE INDEX executions_request_id
				ON executions (namespace, workflow_id, request_id)
				WHERE request_id <> ''`,

			// -----------------------------------------------------------------
			// history_events: the durable truth.
			//
			// data holds the event exactly as pkg/history serialises it. The
			// type and time are duplicated into columns so that an operator can
			// answer "what happened around 04:12" in SQL without a JSON
			// extension, and so that a future retention job can delete by age
			// without decoding anything.
			//
			// This is a rowid table, not WITHOUT ROWID: an event carrying a
			// 2 MiB payload would push the index B-tree into overflow pages,
			// which is precisely the case WITHOUT ROWID is bad at.
			// -----------------------------------------------------------------
			`CREATE TABLE history_events (
				namespace   TEXT    NOT NULL,
				workflow_id TEXT    NOT NULL,
				run_id      TEXT    NOT NULL,
				event_id    INTEGER NOT NULL,
				event_type  INTEGER NOT NULL,
				event_time  INTEGER NOT NULL,
				data        BLOB    NOT NULL,
				PRIMARY KEY (namespace, workflow_id, run_id, event_id),
				FOREIGN KEY (namespace, workflow_id, run_id)
					REFERENCES executions (namespace, workflow_id, run_id)
					ON DELETE CASCADE
			)`,

			// -----------------------------------------------------------------
			// timers: the due-time index.
			//
			// A materialised view of state that could be recomputed by scanning
			// every open execution -- it exists so that "what is due now" is a
			// range scan instead of a full scan.
			// -----------------------------------------------------------------
			`CREATE TABLE timers (
				namespace   TEXT    NOT NULL,
				workflow_id TEXT    NOT NULL,
				run_id      TEXT    NOT NULL,
				event_id    INTEGER NOT NULL,
				kind        INTEGER NOT NULL,
				fire_at     INTEGER NOT NULL,
				attempt     INTEGER NOT NULL DEFAULT 0,
				task_queue  TEXT    NOT NULL DEFAULT '',
				PRIMARY KEY (namespace, workflow_id, run_id, event_id, kind),
				FOREIGN KEY (namespace, workflow_id, run_id)
					REFERENCES executions (namespace, workflow_id, run_id)
					ON DELETE CASCADE
			)`,

			// Serves DueTimers:
			//   WHERE fire_at <= ?
			//   ORDER BY fire_at, namespace, workflow_id, run_id, event_id, kind
			//   LIMIT ?
			// The ordering columns after fire_at are not there to be selective;
			// they are there so the scan needs no sort step and so two timers
			// sharing a nanosecond still come back in a stable order. The index
			// covers every column the query returns except attempt and
			// task_queue, which is a deliberate stop: adding them would double
			// the index size to save one page fetch per due timer.
			`CREATE INDEX timers_due
				ON timers (fire_at, namespace, workflow_id, run_id, event_id, kind)`,
		},
	},
}

// createMigrationsTable is applied before any migration, including on a brand
// new database, and is therefore the one statement that must stay idempotent
// forever.
const createMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations (
	version    INTEGER PRIMARY KEY,
	name       TEXT    NOT NULL,
	applied_at INTEGER NOT NULL
)`

// latestSchemaVersion is the version a freshly migrated database reports.
func latestSchemaVersion() int { return migrations[len(migrations)-1].version }

// migrate brings db up to latestSchemaVersion, doing nothing if it is already
// there.
func migrate(ctx context.Context, db *sql.DB, now int64) error {
	if _, err := db.ExecContext(ctx, createMigrationsTable); err != nil {
		return fmt.Errorf("sqlite: create schema_migrations: %w", err)
	}
	var current int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("sqlite: read schema version: %w", err)
	}
	if latest := latestSchemaVersion(); current > latest {
		// Refusing is the safe answer. A newer schema may have dropped a column
		// this build still writes to, and finding that out one row at a time is
		// worse than not starting.
		return fmt.Errorf("sqlite: database is at schema version %d but this build knows only %d; run the newer binary",
			current, latest)
	}
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := applyMigration(ctx, db, m, now); err != nil {
			return err
		}
	}
	return nil
}

// applyMigration runs one migration atomically. SQLite makes DDL
// transactional, so a statement that fails halfway leaves the database exactly
// as it was -- there is no half-migrated state to clean up by hand.
func applyMigration(ctx context.Context, db *sql.DB, m migration, now int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin migration %d (%s): %w", m.version, m.name, err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once the transaction commits

	for i, stmt := range m.stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite: migration %d (%s) statement %d: %w", m.version, m.name, i+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		m.version, m.name, now); err != nil {
		return fmt.Errorf("sqlite: record migration %d (%s): %w", m.version, m.name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit migration %d (%s): %w", m.version, m.name, err)
	}
	return nil
}
