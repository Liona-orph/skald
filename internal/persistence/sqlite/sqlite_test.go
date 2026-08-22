package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/internal/persistence/persistencetest"
	"github.com/Liona-orph/skald/internal/persistence/sqlite"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// newStore opens a store on a fresh file. A file, not ":memory:": WAL, the
// busy timeout and crash recovery are the things worth testing here, and an
// in-memory database has none of them.
func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "skald.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestConformance(t *testing.T) {
	persistencetest.RunSuite(t, func(t *testing.T) persistence.Store {
		return newStore(t)
	})
}

func TestMigrationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "skald.db")

	first, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	version, err := first.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Positive(t, version)

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	_, err = first.CreateExecution(ctx, b.Create())
	require.NoError(t, err)
	require.NoError(t, first.Close())

	// Re-opening an already-current database must apply nothing: the migration
	// runner is on the startup path of every process, so a migration that ran
	// twice would be a migration that ran during a rolling deploy.
	second, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	againVersion, err := second.SchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, version, againVersion)

	applied := appliedMigrations(t, path)
	require.Len(t, applied, version, "each version must be recorded exactly once")

	got, err := second.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, "run-1", got.RunID)
}

func TestMigrationRejectsANewerDatabase(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "skald.db")

	store, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	// Pretend a newer binary has been here. Starting anyway would mean writing
	// rows in a shape the newer schema may no longer accept.
	withRawDB(t, path, func(db *sql.DB) {
		_, err := db.ExecContext(ctx,
			`INSERT INTO schema_migrations (version, name, applied_at) VALUES (9999, 'from the future', 0)`)
		require.NoError(t, err)
	})

	_, err = sqlite.Open(ctx, path)
	require.ErrorContains(t, err, "9999")
}

// TestReopenRecoversWAL is the property that makes SQLite a real store rather
// than a cache: a committed transaction is still there after the process that
// wrote it is gone, even though WAL means it was never fsynced into the main
// database file.
func TestReopenRecoversWAL(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "skald.db")

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	b.WorkflowTask()
	timerEvent := b.Timer(time.Hour)
	timer := b.TimerFor(timerEvent, time.Date(2024, 3, 1, 13, 0, 0, 0, time.UTC))

	store, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	created, err := store.CreateExecution(ctx, b.Create(func(r *persistence.CreateExecutionRequest) {
		r.Timers = []persistence.TimerRecord{timer}
	}))
	require.NoError(t, err)

	b.Signal("approve")
	updated, err := store.AppendHistory(ctx, b.Append(created.Version))
	require.NoError(t, err)

	// Everything committed so far lives in the -wal file: WAL mode only folds
	// it into the main database at a checkpoint, and NORMAL synchronous means
	// no fsync happened either.
	requireFileExists(t, path+"-wal")

	// Closing runs SQLite's checkpoint-on-last-connection, so the harder test
	// is that reopening finds the data whether or not that succeeded.
	require.NoError(t, store.Close())

	reopened, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	got, err := reopened.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, updated, got)

	h, err := reopened.ReadHistory(ctx, "default", "order-1", "run-1", 1, 0)
	require.NoError(t, err)
	require.NoError(t, h.Validate())
	require.Equal(t, b.History(), h)

	due, err := reopened.DueTimers(ctx, time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC), 10)
	require.NoError(t, err)
	require.Equal(t, []persistence.TimerRecord{timer}, due)

	// And the reopened store is writable: the version it reports has to be the
	// one a writer can actually build on.
	b.Complete()
	closed, err := reopened.AppendHistory(ctx, b.Append(got.Version))
	require.NoError(t, err)
	require.Equal(t, skald.StatusCompleted, closed.Status)
}

// TestSchemaCascadesFromExecutions checks that history and timers are declared
// as children of their execution, so that deleting a run cannot leave orphans
// behind for a retention job to trip over.
//
// It runs against a raw connection because foreign key enforcement is
// per-connection in SQLite and cannot be observed from outside the store's own
// pool. What is observable is journal_mode, which is persisted in the file --
// see TestOptionsReachTheConnection, which proves the same DSN mechanism that
// carries foreign_keys is actually applied.
func TestSchemaCascadesFromExecutions(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "skald.db")

	store, err := sqlite.Open(ctx, path)
	require.NoError(t, err)
	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	b.WorkflowTask()
	timerEvent := b.Timer(time.Hour)
	_, err = store.CreateExecution(ctx, b.Create(func(r *persistence.CreateExecutionRequest) {
		r.Timers = []persistence.TimerRecord{b.TimerFor(timerEvent, time.Now())}
	}))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	withRawDB(t, path, func(db *sql.DB) {
		var on int
		require.NoError(t, db.QueryRowContext(ctx, `PRAGMA foreign_keys`).Scan(&on))
		require.Equal(t, 1, on, "the rest of this test is meaningless without enforcement")

		_, err := db.ExecContext(ctx,
			`INSERT INTO history_events (namespace, workflow_id, run_id, event_id, event_type, event_time, data)
			 VALUES ('default', 'nope', 'nope', 1, 1, 0, x'00')`)
		require.Error(t, err, "an event must not be able to outlive its execution")

		_, err = db.ExecContext(ctx, `DELETE FROM executions WHERE run_id = 'run-1'`)
		require.NoError(t, err)
		for _, table := range []string{"history_events", "timers"} {
			var remaining int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&remaining))
			require.Zero(t, remaining, "%s should have cascaded away with its execution", table)
		}
	})
}

// TestOptionsReachTheConnection proves the DSN plumbing works end to end. WAL
// is the one pragma that leaves a trace in the file, so it stands in for the
// per-connection ones that cannot be observed from outside: if journal_mode
// arrived, so did foreign_keys, busy_timeout and synchronous, because they
// travel in the same query string.
func TestOptionsReachTheConnection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "skald.db")
	migratedAt := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

	store, err := sqlite.Open(ctx, path,
		sqlite.WithMaxOpenConns(4),
		sqlite.WithBusyTimeout(2*time.Second),
		sqlite.WithSynchronous(sqlite.SynchronousFull),
		sqlite.WithClock(func() time.Time { return migratedAt }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Enough work that the pool has to open more than the first connection.
	for i := 0; i < 32; i++ {
		b := persistencetest.NewBuilder("default", fmt.Sprintf("wf-%02d", i), "run-1")
		_, err := store.CreateExecution(ctx, b.Create())
		require.NoError(t, err)
	}
	require.NoError(t, store.OpenExecutions(ctx, "default", func(rec persistence.ExecutionRecord) error {
		_, err := store.ReadHistory(ctx, rec.Namespace, rec.WorkflowID, rec.RunID, 1, 0)
		return err
	}))

	withRawDB(t, path, func(db *sql.DB) {
		var mode string
		require.NoError(t, db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode))
		require.Equal(t, "wal", mode)

		var appliedAt int64
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT applied_at FROM schema_migrations ORDER BY version LIMIT 1`).Scan(&appliedAt))
		require.Equal(t, migratedAt.UnixNano(), appliedAt, "the injected clock must be the one the migration recorded")
	})
}

// TestKeysetPaginationIsStableUnderInsertion is the property OFFSET does not
// have. A row created between two pages must not shift the pages that follow.
func TestKeysetPaginationIsStableUnderInsertion(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	base := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 6; i++ {
		b := persistencetest.NewBuilder("default", fmt.Sprintf("wf-%02d", i), fmt.Sprintf("run-%02d", i),
			persistencetest.WithStartTime(base.Add(time.Duration(i)*time.Minute)))
		_, err := store.CreateExecution(ctx, b.Create())
		require.NoError(t, err)
	}

	filter := persistence.ListFilter{Namespace: "default", PageSize: 2}
	first, err := store.ListExecutions(ctx, filter)
	require.NoError(t, err)
	require.Len(t, first.Records, 2)
	require.NotEmpty(t, first.NextPageToken)

	// Insert a run that sorts to the very front, where an OFFSET would push
	// everything down by one and make page two repeat a row from page one.
	newest := persistencetest.NewBuilder("default", "wf-99", "run-99",
		persistencetest.WithStartTime(base.Add(time.Hour)))
	_, err = store.CreateExecution(ctx, newest.Create())
	require.NoError(t, err)

	filter.PageToken = first.NextPageToken
	second, err := store.ListExecutions(ctx, filter)
	require.NoError(t, err)
	for _, r := range second.Records {
		for _, seen := range first.Records {
			require.NotEqual(t, seen.RunID, r.RunID, "a row inserted mid-scan must not repeat an earlier page")
		}
		require.NotEqual(t, "run-99", r.RunID)
	}

	// A token from one query must not be honoured by another.
	status := skald.StatusRunning
	_, err = store.ListExecutions(ctx, persistence.ListFilter{
		Namespace: "default", PageSize: 2, PageToken: first.NextPageToken, Status: &status,
	})
	require.Error(t, err)
}

// TestConstraintViolationsMapToSentinels covers the paths the conformance suite
// cannot reach, because they need a caller that ignores what a read just told
// it.
func TestConstraintViolationsMapToSentinels(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	_, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	t.Run("duplicate run id", func(t *testing.T) {
		again := persistencetest.NewBuilder("default", "order-1", "run-1")
		_, err := store.CreateExecution(ctx, again.Create(func(r *persistence.CreateExecutionRequest) {
			r.ReusePolicy = persistence.ReuseTerminateIfRunning
		}))
		require.ErrorIs(t, err, persistence.ErrAlreadyStarted)
	})

	t.Run("timer for a missing run", func(t *testing.T) {
		orphan := persistencetest.NewBuilder("default", "ghost", "ghost-run")
		id := orphan.Timer(time.Minute)
		_, err := store.CreateExecution(ctx, persistence.CreateExecutionRequest{
			Record: persistence.ExecutionRecord{
				Namespace: "default", WorkflowID: "ghost", RunID: "ghost-run",
			},
			Events: nil,
			Timers: []persistence.TimerRecord{orphan.TimerFor(id, time.Now())},
		})
		require.ErrorIs(t, err, history.ErrInvalidHistory,
			"a run with no start event is rejected before its timers are considered")
	})
}

// TestAppendRejectsAGapInEventIDs proves the store protects its own storage
// even when the caller passes the version check.
func TestAppendRejectsAGapInEventIDs(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	b := persistencetest.NewBuilder("default", "order-1", "run-1")
	created, err := store.CreateExecution(ctx, b.Create())
	require.NoError(t, err)

	_, err = store.AppendHistory(ctx, persistence.AppendHistoryRequest{
		Namespace: "default", WorkflowID: "order-1", RunID: "run-1",
		ExpectedVersion: created.Version,
		Events: history.History{{
			ID:    created.LastEventID + 5, // a gap
			Time:  b.TimeFor(created.LastEventID + 5),
			Attrs: history.WorkflowExecutionSignaledAttributes{SignalName: "gap"},
		}},
		Record: created,
	})
	require.ErrorIs(t, err, history.ErrInvalidHistory)

	after, err := store.GetExecution(ctx, "default", "order-1", "run-1")
	require.NoError(t, err)
	require.Equal(t, created, after, "a rejected append must leave the row alone")
}

func TestOpenRejectsAPathWithAQuery(t *testing.T) {
	_, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "skald.db?_pragma=foreign_keys(0)"))
	require.Error(t, err)
}

// withRawDB opens the same file with a plain driver connection so a test can
// inspect or corrupt it behind the store's back.
func withRawDB(t *testing.T, path string, fn func(*sql.DB)) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys%28ON%29")
	require.NoError(t, err)
	defer db.Close()
	fn(db)
}

func appliedMigrations(t *testing.T, path string) []int {
	t.Helper()
	var versions []int
	withRawDB(t, path, func(db *sql.DB) {
		rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var v int
			require.NoError(t, rows.Scan(&v))
			versions = append(versions, v)
		}
		require.NoError(t, rows.Err())
	})
	return versions
}

func requireFileExists(t *testing.T, path string) {
	t.Helper()
	_, err := os.Stat(path)
	require.False(t, errors.Is(err, os.ErrNotExist), "expected %s to exist", path)
}
