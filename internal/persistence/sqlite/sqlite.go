// Package sqlite implements the persistence contract on SQLite.
//
// It uses modernc.org/sqlite, a pure-Go translation of the SQLite amalgamation,
// so a Skald binary cross-compiles and ships without a C toolchain. The cost is
// perhaps a third of cgo SQLite's throughput, which is the right trade for a
// durable execution engine whose bottleneck is fsync rather than CPU.
//
// SQLite is a real answer here, not a toy one. A single-node Skald with a
// SQLite store survives process crashes, machine restarts and rolling upgrades;
// what it does not survive is the loss of the machine. Deployments that need
// that reach for a networked store, and the contract in the parent package is
// what makes the swap a configuration change.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// Query limits. They are the driver's, not the contract's: another driver is
// free to disagree.
const (
	defaultPageSize  = 100
	maxPageSize      = 1000
	defaultDueLimit  = 1000
	maxDueTimerLimit = 10000
	// scanPageSize bounds how many rows OpenExecutions holds at once. The scan
	// pages rather than streaming a single cursor because the callback belongs
	// to the engine and will call back into the store; a cursor held open
	// across it would occupy a pooled connection for the whole recovery.
	scanPageSize = 500
)

// reuseTerminationReason is written into the event the store synthesises when
// ReuseTerminateIfRunning displaces a running execution. The whole policy is
// resolved inside one transaction, because a window where neither run is open
// would let a third starter slip in and win.
const reuseTerminationReason = "terminated to start a new run under the terminate-if-running id reuse policy"

// Synchronous selects SQLite's durability setting.
type Synchronous string

const (
	// SynchronousNormal is the default. In WAL mode a commit appends to the
	// write-ahead log without an fsync; the log is fsynced when it is
	// checkpointed. A process crash, a kill -9 or an OOM loses nothing, because
	// the log is already in the page cache the OS owns. A power cut or a kernel
	// panic can lose the last few committed transactions -- the database is
	// never corrupted, but a workflow may appear to have un-happened.
	//
	// That is an honest trade for a workflow engine: the same transactions the
	// crash lost are ones no client was told about, because Skald acknowledges
	// a start only after commit returns, and commit returning is exactly the
	// signal NORMAL weakens.
	SynchronousNormal Synchronous = "NORMAL"
	// SynchronousFull fsyncs the log at every commit. Roughly an order of
	// magnitude fewer writes per second on spinning media and still noticeably
	// slower on SSDs, in exchange for losing nothing short of a disk that lies
	// about its own flushes.
	SynchronousFull Synchronous = "FULL"
)

type config struct {
	busyTimeout  time.Duration
	maxOpenConns int
	synchronous  Synchronous
	now          func() time.Time
}

// Option customises a Store.
type Option func(*config)

// WithBusyTimeout sets how long a connection waits for the write lock before
// giving up. It must exceed the longest transaction the process runs, or
// unrelated writers will fail under load rather than queue.
func WithBusyTimeout(d time.Duration) Option {
	return func(c *config) { c.busyTimeout = d }
}

// WithMaxOpenConns bounds the connection pool. SQLite serialises writers no
// matter how many connections exist, so the number is really about how many
// concurrent readers WAL mode should be allowed to serve.
func WithMaxOpenConns(n int) Option {
	return func(c *config) { c.maxOpenConns = n }
}

// WithSynchronous chooses the durability setting. See SynchronousNormal.
func WithSynchronous(s Synchronous) Option {
	return func(c *config) { c.synchronous = s }
}

// WithClock injects the clock the store reads for timestamps it invents rather
// than receives. That is exactly one column today -- schema_migrations.applied_at
// -- because every other timestamp comes from an event the caller supplied,
// which is what keeps this driver replayable.
func WithClock(now func() time.Time) Option {
	return func(c *config) { c.now = now }
}

// Store is a SQLite-backed persistence.Store.
type Store struct {
	db *sql.DB
	// closed is atomic rather than mutex-guarded so that a call racing Close
	// reports ErrClosed instead of database/sql's "sql: database is closed",
	// which callers would have to string-match to recognise.
	closed atomic.Bool
}

var _ persistence.Store = (*Store)(nil)

// Open connects to the database at path, creating and migrating it if needed.
//
// path is a filename; ":memory:" gives a private in-memory database, which is
// useful for a quick test but gets no WAL and therefore none of the concurrency
// this driver is tuned for.
func Open(ctx context.Context, path string, opts ...Option) (*Store, error) {
	cfg := config{
		busyTimeout:  5 * time.Second,
		maxOpenConns: 8,
		synchronous:  SynchronousNormal,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if strings.ContainsAny(path, "?#") {
		// The path becomes a SQLite URI, where these characters start the query
		// and fragment. Rejecting them is better than silently opening a
		// different file than the operator named.
		return nil, fmt.Errorf("sqlite: database path %q must not contain '?' or '#'", path)
	}

	db, err := sql.Open("sqlite", dsn(path, cfg))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %q: %w", path, err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConns)
	// Idle connections are kept to match, because closing one throws away the
	// per-connection PRAGMA state that dsn set up and the next query pays to
	// rebuild it.
	db.SetMaxIdleConns(cfg.maxOpenConns)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: connect to %q: %w", path, err)
	}
	if err := migrate(ctx, db, cfg.now().UnixNano()); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// dsn renders the connection string.
//
// Every PRAGMA goes in the DSN rather than being executed once after Open,
// because PRAGMAs are per-connection and database/sql opens connections
// whenever it likes. A foreign_keys setting applied to the first connection and
// nowhere else is worse than none at all: it fails in tests and silently
// permits orphans in production.
func dsn(path string, cfg config) string {
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", cfg.busyTimeout.Milliseconds()))
	// WAL lets readers proceed while a writer holds the lock, which is what
	// makes a long visibility query harmless to the hot path. It also makes
	// crash recovery a log replay instead of a rollback journal rewind.
	q.Add("_pragma", "journal_mode(WAL)")
	// SQLite disables foreign keys by default for backwards compatibility. The
	// history and timer tables cascade from executions, so this is load
	// bearing: without it a deleted execution leaves its events behind.
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", fmt.Sprintf("synchronous(%s)", cfg.synchronous))
	// Every transaction this driver opens is a write, so taking the write lock
	// at BEGIN is both honest and necessary: a deferred transaction that
	// upgrades mid-way cannot wait for the lock and fails with SQLITE_BUSY
	// immediately, no matter what busy_timeout says.
	q.Set("_txlock", "immediate")
	return "file:" + path + "?" + q.Encode()
}

// SchemaVersion reports the migration the database has been brought up to. It
// exists for tests and for an operator asking "did the migration actually run".
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	if err := s.check(ctx, "SchemaVersion"); err != nil {
		return 0, err
	}
	var v int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlite: read schema version: %w", err)
	}
	return v, nil
}

// Close implements persistence.Store.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("sqlite: close: %w", err)
	}
	return nil
}

// check runs the preconditions every method shares.
func (s *Store) check(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("sqlite: %s: %w", op, err)
	}
	if s.closed.Load() {
		return fmt.Errorf("sqlite: %s: %w", op, persistence.ErrClosed)
	}
	return nil
}

// ---------------------------------------------------------------------------
// CreateExecution
// ---------------------------------------------------------------------------

// CreateExecution implements persistence.Store.
func (s *Store) CreateExecution(ctx context.Context, req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	const op = "CreateExecution"
	var zero persistence.ExecutionRecord
	if err := s.check(ctx, op); err != nil {
		return zero, err
	}
	rec, err := validateCreate(req)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	timers, err := validateTimers(rec.Namespace, rec.WorkflowID, rec.RunID, req.Timers)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: begin: %w", op, s.mapClosed(err))
	}
	defer tx.Rollback() //nolint:errcheck // no-op once the transaction commits

	rec, _, err = s.createRunInTx(ctx, tx, op, req, rec, timers)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: %s: commit run %s: %w", op, rec.RunID, s.mapClosed(err))
	}
	return rec, nil
}

// createRunInTx inserts a run inside a caller-owned transaction.
//
// It is shared by CreateExecution and by the successor branch of
// AppendHistory. Sharing it is not merely tidy: the reuse policy, the request
// ID deduplication and the event insert are the parts a second implementation
// would drift on, and a successor created by subtly different rules than an
// ordinary start is a bug that would only show up months later, in a
// continue-as-new chain nobody was watching.
//
// The bool reports whether a row was actually inserted; false means an existing
// run was returned because its request ID matched.
func (s *Store) createRunInTx(
	ctx context.Context, tx *sql.Tx, op string,
	req persistence.CreateExecutionRequest,
	rec persistence.ExecutionRecord,
	timers []persistence.TimerRecord,
) (persistence.ExecutionRecord, bool, error) {
	var zero persistence.ExecutionRecord

	if req.RequestID != "" {
		row := tx.QueryRowContext(ctx,
			`SELECT `+executionColumns+` FROM executions
			 WHERE namespace = ? AND workflow_id = ? AND request_id = ?`,
			rec.Namespace, rec.WorkflowID, req.RequestID)
		switch prev, err := scanExecution(row); {
		case err == nil:
			// A retried start is not an error and must not create a second run.
			// This branch is the entire reason StartWorkflow is safe to retry
			// blindly after a timeout.
			return prev.ExecutionRecord, false, nil
		case !errors.Is(err, sql.ErrNoRows):
			return zero, false, fmt.Errorf("sqlite: %s: deduplicate request %q: %w", op, req.RequestID, err)
		}
	}

	current, err := currentRun(ctx, tx, rec.Namespace, rec.WorkflowID)
	switch {
	case err == nil:
		if err := s.applyReusePolicy(ctx, tx, req.ReusePolicy, current, rec); err != nil {
			return zero, false, fmt.Errorf("sqlite: %s: %w", op, err)
		}
	case !errors.Is(err, persistence.ErrNotFound):
		return zero, false, fmt.Errorf("sqlite: %s: resolve current run of %q: %w", op, rec.WorkflowID, err)
	}

	memo, searchAttrs, err := encodeAttrPair(rec)
	if err != nil {
		return zero, false, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	lastEventTime := req.Events[len(req.Events)-1].Time
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO executions (`+executionColumns+`, request_id)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.Namespace, rec.WorkflowID, rec.RunID, rec.WorkflowType, rec.TaskQueue,
		int64(rec.Status), nanos(rec.StartedAt), nanos(rec.ClosedAt), rec.Version,
		rec.LastEventID, nanos(lastEventTime), rec.FirstExecutionRunID, memo, searchAttrs,
		req.RequestID,
	); err != nil {
		if isConstraint(err) {
			return zero, false, fmt.Errorf("sqlite: %s: run %s/%s/%s: %w: %w",
				op, rec.Namespace, rec.WorkflowID, rec.RunID, persistence.ErrAlreadyStarted, err)
		}
		return zero, false, fmt.Errorf("sqlite: %s: insert execution %s: %w", op, rec.RunID, s.mapClosed(err))
	}
	if err := insertEvents(ctx, tx, rec.Namespace, rec.WorkflowID, rec.RunID, req.Events); err != nil {
		return zero, false, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	if err := upsertTimers(ctx, tx, timers); err != nil {
		return zero, false, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	return rec, true, nil
}

// applyReusePolicy decides what a colliding workflow ID means, and carries out
// the termination when the policy asks for one.
func (s *Store) applyReusePolicy(
	ctx context.Context, tx *sql.Tx,
	policy persistence.IDReusePolicy, prev executionRow, next persistence.ExecutionRecord,
) error {
	switch policy {
	case persistence.ReuseRejectDuplicate:
		return fmt.Errorf("workflow %q already ran as %s: %w", next.WorkflowID, prev.RunID, persistence.ErrAlreadyStarted)
	case persistence.ReuseAllowDuplicateFailedOnly:
		if prev.Open() || prev.Status == skald.StatusCompleted || prev.Status == skald.StatusContinuedAsNew {
			return fmt.Errorf("workflow %q last run %s is %s: %w",
				next.WorkflowID, prev.RunID, prev.Status, persistence.ErrAlreadyStarted)
		}
	case persistence.ReuseTerminateIfRunning:
		if prev.Open() {
			return terminateRun(ctx, tx, prev, next.StartedAt)
		}
	default: // ReuseAllowDuplicate
		if prev.Open() {
			return fmt.Errorf("workflow %q is still running as %s: %w",
				next.WorkflowID, prev.RunID, persistence.ErrAlreadyStarted)
		}
	}
	return nil
}

// terminateRun closes an open run in place so a replacement can take its
// workflow ID. It writes a real terminal event rather than only flipping the
// status column: a row that claims TERMINATED over a history with no terminal
// event would fail every replay that touched it afterwards.
func terminateRun(ctx context.Context, tx *sql.Tx, prev executionRow, at time.Time) error {
	at = at.UTC()
	if at.Before(prev.lastEventTime) {
		// History time must never go backwards, and the replacement run may
		// have been stamped by a different clock. Clamp rather than trust it.
		at = prev.lastEventTime
	}
	ev := history.Event{
		ID:   prev.LastEventID + 1,
		Time: at,
		Attrs: history.WorkflowExecutionTerminatedAttributes{
			Reason:   reuseTerminationReason,
			Identity: "skald-store",
		},
	}
	if err := insertEvents(ctx, tx, prev.Namespace, prev.WorkflowID, prev.RunID, history.History{ev}); err != nil {
		return fmt.Errorf("terminate run %s: %w", prev.RunID, err)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE executions
		    SET status = ?, closed_at = ?, version = version + 1,
		        last_event_id = ?, last_event_time = ?
		  WHERE namespace = ? AND workflow_id = ? AND run_id = ? AND version = ?`,
		int64(skald.StatusTerminated), nanos(at), ev.ID, nanos(at),
		prev.Namespace, prev.WorkflowID, prev.RunID, prev.Version)
	if err != nil {
		return fmt.Errorf("terminate run %s: %w", prev.RunID, err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("terminate run %s: %d rows updated: %w", prev.RunID, n, persistence.ErrVersionConflict)
	}
	// A terminated run cannot be woken again; leaving its timers behind would
	// hand the timer service work it must then learn to ignore.
	if err := deleteRunTimers(ctx, tx, prev.Namespace, prev.WorkflowID, prev.RunID); err != nil {
		return fmt.Errorf("terminate run %s: %w", prev.RunID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// GetExecution implements persistence.Store.
func (s *Store) GetExecution(ctx context.Context, namespace, workflowID, runID string) (persistence.ExecutionRecord, error) {
	const op = "GetExecution"
	if err := s.check(ctx, op); err != nil {
		return persistence.ExecutionRecord{}, err
	}
	row, err := lookupRun(ctx, s.db, namespace, workflowID, runID)
	if err != nil {
		return persistence.ExecutionRecord{}, fmt.Errorf("sqlite: %s: %s/%s/%s: %w", op, namespace, workflowID, runID, s.mapClosed(err))
	}
	return row.ExecutionRecord, nil
}

// ReadHistory implements persistence.Store.
func (s *Store) ReadHistory(ctx context.Context, namespace, workflowID, runID string, fromEventID, toEventID int64) (history.History, error) {
	const op = "ReadHistory"
	if err := s.check(ctx, op); err != nil {
		return nil, err
	}
	// The run has to be resolved even for an empty result, because "no events"
	// and "no such run" are different answers and the caller acts on each
	// differently.
	row, err := lookupRun(ctx, s.db, namespace, workflowID, runID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: %s: %s/%s/%s: %w", op, namespace, workflowID, runID, s.mapClosed(err))
	}
	if fromEventID < 1 {
		fromEventID = 1
	}
	if toEventID > 0 && toEventID < fromEventID {
		// An inverted range is empty, not an error: it is what a caller asking
		// for [n, n-1] after a no-op append naturally produces.
		return nil, nil
	}

	query := `SELECT data FROM history_events
	           WHERE namespace = ? AND workflow_id = ? AND run_id = ? AND event_id >= ?`
	args := []any{namespace, workflowID, row.RunID, fromEventID}
	if toEventID > 0 {
		query += ` AND event_id <= ?`
		args = append(args, toEventID)
	}
	query += ` ORDER BY event_id`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: %s: run %s: %w", op, row.RunID, s.mapClosed(err))
	}
	defer rows.Close()

	var out history.History
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("sqlite: %s: run %s: scan event: %w", op, row.RunID, err)
		}
		var ev history.Event
		if err := json.Unmarshal(data, &ev); err != nil {
			return nil, fmt.Errorf("sqlite: %s: run %s: decode event: %w", op, row.RunID, err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: %s: run %s: %w", op, row.RunID, s.mapClosed(err))
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// AppendHistory
// ---------------------------------------------------------------------------

// AppendHistory implements persistence.Store.
func (s *Store) AppendHistory(ctx context.Context, req persistence.AppendHistoryRequest) (persistence.ExecutionRecord, error) {
	const op = "AppendHistory"
	var zero persistence.ExecutionRecord
	if err := s.check(ctx, op); err != nil {
		return zero, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: begin: %w", op, s.mapClosed(err))
	}
	defer tx.Rollback() //nolint:errcheck // no-op once the transaction commits

	row, err := lookupRun(ctx, tx, req.Namespace, req.WorkflowID, req.RunID)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: %s/%s/%s: %w", op, req.Namespace, req.WorkflowID, req.RunID, s.mapClosed(err))
	}
	if row.Version != req.ExpectedVersion {
		// Reported before anything is written, which is the whole contract: a
		// losing writer must find the store exactly as it was so it can re-read
		// and retry.
		return zero, fmt.Errorf("sqlite: %s: run %s is at version %d, caller expected %d: %w",
			op, row.RunID, row.Version, req.ExpectedVersion, persistence.ErrVersionConflict)
	}
	if err := validateAppend(row, req.Events); err != nil {
		return zero, fmt.Errorf("sqlite: %s: run %s: %w", op, row.RunID, err)
	}
	timers, err := validateTimers(row.Namespace, row.WorkflowID, row.RunID, req.UpsertTimers)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	for _, k := range req.DeleteTimers {
		if err := checkTimerKey(row.Namespace, row.WorkflowID, row.RunID, k); err != nil {
			return zero, fmt.Errorf("sqlite: %s: delete timer: %w", op, err)
		}
	}

	// Validated before anything is written so that an unusable successor fails
	// the whole call rather than committing a close that strands the chain.
	var successorRec persistence.ExecutionRecord
	var successorTimers []persistence.TimerRecord
	if req.CreateSuccessor != nil {
		successorRec, err = validateCreate(*req.CreateSuccessor)
		if err != nil {
			return zero, fmt.Errorf("sqlite: %s: successor: %w", op, err)
		}
		if successorRec.Namespace != req.Namespace || successorRec.WorkflowID != req.WorkflowID {
			return zero, fmt.Errorf(
				"sqlite: %s: successor %s/%s does not continue %s/%s: %w",
				op, successorRec.Namespace, successorRec.WorkflowID,
				req.Namespace, req.WorkflowID, persistence.ErrNotFound)
		}
		successorTimers, err = validateTimers(
			successorRec.Namespace, successorRec.WorkflowID, successorRec.RunID,
			req.CreateSuccessor.Timers)
		if err != nil {
			return zero, fmt.Errorf("sqlite: %s: successor: %w", op, err)
		}
	}

	next := row
	next.Version++
	if n := len(req.Events); n > 0 {
		next.LastEventID = req.Events[n-1].ID
		next.lastEventTime = req.Events[n-1].Time.UTC()
	}
	applyMutableFields(&next.ExecutionRecord, req.Record, next.lastEventTime)
	memo, searchAttrs, err := encodeAttrPair(next.ExecutionRecord)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}

	// The version predicate is what makes the write conditional. The check
	// above already compared versions, and BEGIN IMMEDIATE means no one could
	// have moved since -- but the predicate costs nothing and keeps the
	// guarantee in the statement rather than in an argument about locking.
	res, err := tx.ExecContext(ctx,
		`UPDATE executions
		    SET status = ?, closed_at = ?, version = ?, last_event_id = ?,
		        last_event_time = ?, memo = ?, search_attrs = ?
		  WHERE namespace = ? AND workflow_id = ? AND run_id = ? AND version = ?`,
		int64(next.Status), nanos(next.ClosedAt), next.Version, next.LastEventID,
		nanos(next.lastEventTime), memo, searchAttrs,
		next.Namespace, next.WorkflowID, next.RunID, req.ExpectedVersion)
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: update run %s: %w", op, next.RunID, s.mapClosed(err))
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return zero, fmt.Errorf("sqlite: %s: update run %s: %w", op, next.RunID, err)
	}
	if affected != 1 {
		return zero, fmt.Errorf("sqlite: %s: run %s moved from version %d before the write landed: %w",
			op, next.RunID, req.ExpectedVersion, persistence.ErrVersionConflict)
	}

	if err := insertEvents(ctx, tx, next.Namespace, next.WorkflowID, next.RunID, req.Events); err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	if err := deleteTimers(ctx, tx, req.DeleteTimers); err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	if err := upsertTimers(ctx, tx, timers); err != nil {
		return zero, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	if !next.Open() {
		if err := deleteRunTimers(ctx, tx, next.Namespace, next.WorkflowID, next.RunID); err != nil {
			return zero, fmt.Errorf("sqlite: %s: %w", op, err)
		}
	}
	if req.CreateSuccessor != nil {
		// Created last, so the reuse check sees the predecessor already closed
		// by the update above. The policy is forced to AllowDuplicate: a
		// successor is the continuation of the run being closed, and applying
		// the original start's policy here would let a REJECT_DUPLICATE
		// workflow refuse its own continue-as-new.
		sub := *req.CreateSuccessor
		sub.ReusePolicy = persistence.ReuseAllowDuplicate
		if _, _, err := s.createRunInTx(ctx, tx, op, sub, successorRec, successorTimers); err != nil {
			return zero, fmt.Errorf("sqlite: %s: successor of run %s: %w", op, next.RunID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("sqlite: %s: commit run %s: %w", op, next.RunID, s.mapClosed(err))
	}
	return next.ExecutionRecord, nil
}

// applyMutableFields copies the fields an append may change. A run's identity
// -- namespace, IDs, type, task queue, start time -- is fixed at creation, so
// taking it from the request would let a buggy caller rewrite metadata other
// rows already reference.
func applyMutableFields(dst *persistence.ExecutionRecord, src persistence.ExecutionRecord, lastEvent time.Time) {
	if !dst.Status.Terminal() {
		// Terminal is absorbing: a caller replaying an old request must not be
		// able to reopen a finished execution.
		dst.Status = src.Status
	}
	switch {
	case !dst.Status.Terminal():
		dst.ClosedAt = time.Time{}
	case !src.ClosedAt.IsZero():
		dst.ClosedAt = src.ClosedAt.UTC()
	case dst.ClosedAt.IsZero():
		dst.ClosedAt = lastEvent
	}
	// A nil map means "unchanged"; an empty one means "cleared".
	if src.Memo != nil {
		dst.Memo = cloneAttrs(src.Memo)
	}
	if src.SearchAttrs != nil {
		dst.SearchAttrs = cloneAttrs(src.SearchAttrs)
	}
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

// ListExecutions implements persistence.Store.
func (s *Store) ListExecutions(ctx context.Context, filter persistence.ListFilter) (persistence.ListResult, error) {
	const op = "ListExecutions"
	if err := s.check(ctx, op); err != nil {
		return persistence.ListResult{}, err
	}
	after, err := decodePageToken(filter)
	if err != nil {
		return persistence.ListResult{}, fmt.Errorf("sqlite: %s: %w", op, err)
	}
	size := clampPageSize(filter.PageSize)

	where, args := visibilityPredicate(filter)
	if after != nil {
		// Keyset, not OFFSET. OFFSET makes the database count and discard every
		// skipped row, so page N costs N times page 1 and a row inserted
		// meanwhile shifts the whole result set. This predicate is a seek into
		// the same index the ORDER BY walks.
		where = append(where, `(started_at < ? OR (started_at = ? AND run_id < ?))`)
		args = append(args, after.startedAt, after.startedAt, after.runID)
	}

	query := `SELECT ` + executionColumns + ` FROM executions`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, ` AND `)
	}
	// One row beyond the page tells us whether a next page exists without a
	// second COUNT query.
	query += ` ORDER BY started_at DESC, run_id DESC LIMIT ?`
	args = append(args, size+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return persistence.ListResult{}, fmt.Errorf("sqlite: %s: %w", op, s.mapClosed(err))
	}
	defer rows.Close()

	records := make([]persistence.ExecutionRecord, 0, size)
	more := false
	for rows.Next() {
		if len(records) == size {
			more = true
			break
		}
		row, err := scanExecution(rows)
		if err != nil {
			return persistence.ListResult{}, fmt.Errorf("sqlite: %s: scan: %w", op, err)
		}
		records = append(records, row.ExecutionRecord)
	}
	if err := rows.Err(); err != nil {
		return persistence.ListResult{}, fmt.Errorf("sqlite: %s: %w", op, s.mapClosed(err))
	}

	res := persistence.ListResult{Records: records}
	if more {
		res.NextPageToken = encodePageToken(filter, records[len(records)-1])
	}
	return res, nil
}

// visibilityPredicate renders the filter as SQL. An empty namespace matches
// every tenant, which is what the operator CLI wants and what a per-namespace
// API must therefore never pass through unset.
func visibilityPredicate(f persistence.ListFilter) ([]string, []any) {
	var where []string
	var args []any
	if f.Namespace != "" {
		where = append(where, `namespace = ?`)
		args = append(args, f.Namespace)
	}
	if f.WorkflowID != "" {
		where = append(where, `workflow_id = ?`)
		args = append(args, f.WorkflowID)
	}
	if f.WorkflowType != "" {
		where = append(where, `workflow_type = ?`)
		args = append(args, f.WorkflowType)
	}
	if f.Status != nil {
		where = append(where, `status = ?`)
		args = append(args, int64(*f.Status))
	}
	if !f.StartedAfter.IsZero() {
		where = append(where, `started_at >= ?`)
		args = append(args, nanos(f.StartedAfter))
	}
	return where, args
}

// OpenExecutions implements persistence.Store.
func (s *Store) OpenExecutions(ctx context.Context, namespace string, fn func(persistence.ExecutionRecord) error) error {
	const op = "OpenExecutions"
	if err := s.check(ctx, op); err != nil {
		return err
	}
	// Non-terminal is exactly StatusRunning: skald.WorkflowStatus.Terminal
	// reports true for everything else, and the column stores the enum.
	where := []string{`status = ?`}
	args := []any{int64(skald.StatusRunning)}
	if namespace != "" {
		where = append(where, `namespace = ?`)
		args = append(args, namespace)
	}

	var cursor *pageCursor
	for {
		q := `SELECT ` + executionColumns + ` FROM executions WHERE ` + strings.Join(where, ` AND `)
		pageArgs := append([]any(nil), args...)
		if cursor != nil {
			q += ` AND (started_at < ? OR (started_at = ? AND run_id < ?))`
			pageArgs = append(pageArgs, cursor.startedAt, cursor.startedAt, cursor.runID)
		}
		q += ` ORDER BY started_at DESC, run_id DESC LIMIT ?`
		pageArgs = append(pageArgs, scanPageSize)

		page, err := s.scanPage(ctx, q, pageArgs)
		if err != nil {
			return fmt.Errorf("sqlite: %s: %w", op, err)
		}
		// The callback runs with no rows open. It belongs to the engine and
		// will read history as it goes; holding a cursor across it would pin a
		// pooled connection for the length of the whole recovery scan.
		for _, rec := range page {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("sqlite: %s: %w", op, err)
			}
			if err := fn(rec); err != nil {
				return fmt.Errorf("sqlite: %s: run %s: %w", op, rec.RunID, err)
			}
		}
		if len(page) < scanPageSize {
			return nil
		}
		last := page[len(page)-1]
		cursor = &pageCursor{startedAt: nanos(last.StartedAt), runID: last.RunID}
	}
}

func (s *Store) scanPage(ctx context.Context, query string, args []any) ([]persistence.ExecutionRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, s.mapClosed(err)
	}
	defer rows.Close()

	var out []persistence.ExecutionRecord
	for rows.Next() {
		row, err := scanExecution(rows)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, row.ExecutionRecord)
	}
	if err := rows.Err(); err != nil {
		return nil, s.mapClosed(err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// DueTimers implements persistence.Store.
func (s *Store) DueTimers(ctx context.Context, now time.Time, limit int) ([]persistence.TimerRecord, error) {
	const op = "DueTimers"
	if err := s.check(ctx, op); err != nil {
		return nil, err
	}
	switch {
	case limit <= 0:
		limit = defaultDueLimit
	case limit > maxDueTimerLimit:
		limit = maxDueTimerLimit
	}

	rows, err := s.db.QueryContext(ctx,
		`SELECT namespace, workflow_id, run_id, event_id, kind, fire_at, attempt, task_queue
		   FROM timers
		  WHERE fire_at <= ?
		  ORDER BY fire_at, namespace, workflow_id, run_id, event_id, kind
		  LIMIT ?`,
		nanos(now), limit)
	if err != nil {
		return nil, fmt.Errorf("sqlite: %s: %w", op, s.mapClosed(err))
	}
	defer rows.Close()

	var out []persistence.TimerRecord
	for rows.Next() {
		var (
			tr     persistence.TimerRecord
			kind   int64
			fireAt int64
		)
		if err := rows.Scan(&tr.Namespace, &tr.WorkflowID, &tr.RunID, &tr.EventID,
			&kind, &fireAt, &tr.Attempt, &tr.TaskQueue); err != nil {
			return nil, fmt.Errorf("sqlite: %s: scan: %w", op, err)
		}
		tr.Kind = persistence.TimerKind(kind)
		tr.FireAt = fromNanos(fireAt)
		out = append(out, tr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: %s: %w", op, s.mapClosed(err))
	}
	return out, nil
}

// DeleteTimers implements persistence.Store.
func (s *Store) DeleteTimers(ctx context.Context, keys []persistence.TimerKey) error {
	const op = "DeleteTimers"
	if err := s.check(ctx, op); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: %s: begin: %w", op, s.mapClosed(err))
	}
	defer tx.Rollback() //nolint:errcheck // no-op once the transaction commits

	if err := deleteTimers(ctx, tx, keys); err != nil {
		return fmt.Errorf("sqlite: %s: %w", op, s.mapClosed(err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: %s: commit: %w", op, s.mapClosed(err))
	}
	return nil
}

func deleteTimers(ctx context.Context, tx *sql.Tx, keys []persistence.TimerKey) error {
	if len(keys) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx,
		`DELETE FROM timers
		  WHERE namespace = ? AND workflow_id = ? AND run_id = ? AND event_id = ? AND kind = ?`)
	if err != nil {
		return fmt.Errorf("prepare timer delete: %w", err)
	}
	defer stmt.Close()

	for _, k := range keys {
		// Deleting a timer that is already gone is not an error: the engine
		// deletes each one as it processes it and may be replaying a batch it
		// crashed halfway through.
		if _, err := stmt.ExecContext(ctx, k.Namespace, k.WorkflowID, k.RunID, k.EventID, int64(k.Kind)); err != nil {
			return fmt.Errorf("delete timer %d of run %s: %w", k.EventID, k.RunID, err)
		}
	}
	return nil
}

func deleteRunTimers(ctx context.Context, tx *sql.Tx, namespace, workflowID, runID string) error {
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM timers WHERE namespace = ? AND workflow_id = ? AND run_id = ?`,
		namespace, workflowID, runID); err != nil {
		return fmt.Errorf("retire timers of run %s: %w", runID, err)
	}
	return nil
}

func upsertTimers(ctx context.Context, tx *sql.Tx, timers []persistence.TimerRecord) error {
	if len(timers) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO timers (namespace, workflow_id, run_id, event_id, kind, fire_at, attempt, task_queue)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (namespace, workflow_id, run_id, event_id, kind)
		 DO UPDATE SET fire_at = excluded.fire_at,
		               attempt = excluded.attempt,
		               task_queue = excluded.task_queue`)
	if err != nil {
		return fmt.Errorf("prepare timer upsert: %w", err)
	}
	defer stmt.Close()

	for _, tr := range timers {
		// An upsert rather than a delete-then-insert: an activity retry moves
		// the same timer forward on every attempt, and a window where the entry
		// does not exist is a window where a crash loses the wakeup entirely.
		if _, err := stmt.ExecContext(ctx, tr.Namespace, tr.WorkflowID, tr.RunID, tr.EventID,
			int64(tr.Kind), nanos(tr.FireAt), tr.Attempt, tr.TaskQueue); err != nil {
			if isConstraint(err) {
				return fmt.Errorf("timer %d of run %s references no execution: %w",
					tr.EventID, tr.RunID, persistence.ErrNotFound)
			}
			return fmt.Errorf("upsert timer %d of run %s: %w", tr.EventID, tr.RunID, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

// executionColumns is the projection every execution read shares, in the order
// scanExecution expects. Keeping it in one place is what stops a new column
// from being added to the table and silently missed by one of five queries.
const executionColumns = `namespace, workflow_id, run_id, workflow_type, task_queue, ` +
	`status, started_at, closed_at, version, last_event_id, last_event_time, ` +
	`first_execution_run_id, memo, search_attrs`

// executionRow is an ExecutionRecord plus the columns the driver needs but the
// contract does not expose.
type executionRow struct {
	persistence.ExecutionRecord
	// lastEventTime is denormalised onto the execution row so that validating
	// an append's timestamps costs nothing. Recomputing it would mean a second
	// query into history_events on the hottest path in the engine.
	lastEventTime time.Time
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

func scanExecution(row scanner) (executionRow, error) {
	var (
		out                                      executionRow
		status, startedAt, closedAt, lastEventAt int64
		memoJSON, searchJSON                     string
	)
	if err := row.Scan(
		&out.Namespace, &out.WorkflowID, &out.RunID, &out.WorkflowType, &out.TaskQueue,
		&status, &startedAt, &closedAt, &out.Version, &out.LastEventID, &lastEventAt,
		&out.FirstExecutionRunID, &memoJSON, &searchJSON,
	); err != nil {
		return out, err
	}
	out.Status = skald.WorkflowStatus(status)
	out.StartedAt = fromNanos(startedAt)
	out.ClosedAt = fromNanos(closedAt)
	out.lastEventTime = fromNanos(lastEventAt)

	var err error
	if out.Memo, err = decodeAttrs(memoJSON); err != nil {
		return out, fmt.Errorf("run %s: decode memo: %w", out.RunID, err)
	}
	if out.SearchAttrs, err = decodeAttrs(searchJSON); err != nil {
		return out, fmt.Errorf("run %s: decode search attributes: %w", out.RunID, err)
	}
	return out, nil
}

// querier is satisfied by *sql.DB and *sql.Tx, so the read helpers work both
// inside and outside a transaction.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// lookupRun resolves a run, treating an empty run ID as "the current run".
func lookupRun(ctx context.Context, q querier, namespace, workflowID, runID string) (executionRow, error) {
	if runID == "" {
		return currentRun(ctx, q, namespace, workflowID)
	}
	row := q.QueryRowContext(ctx,
		`SELECT `+executionColumns+` FROM executions
		  WHERE namespace = ? AND workflow_id = ? AND run_id = ?`,
		namespace, workflowID, runID)
	out, err := scanExecution(row)
	if errors.Is(err, sql.ErrNoRows) {
		return out, persistence.ErrNotFound
	}
	return out, err
}

// currentRun returns the newest run of a workflow ID.
//
// "Newest" is (started_at, run_id) descending -- the same total order
// ListExecutions pages in, so that the current run is always the first row an
// operator sees when they list a workflow ID. Deriving it from the ordering
// rather than storing an is_current flag removes a second row to keep in step
// on every continue-as-new.
func currentRun(ctx context.Context, q querier, namespace, workflowID string) (executionRow, error) {
	row := q.QueryRowContext(ctx,
		`SELECT `+executionColumns+` FROM executions
		  WHERE namespace = ? AND workflow_id = ?
		  ORDER BY started_at DESC, run_id DESC
		  LIMIT 1`,
		namespace, workflowID)
	out, err := scanExecution(row)
	if errors.Is(err, sql.ErrNoRows) {
		return out, persistence.ErrNotFound
	}
	return out, err
}

func insertEvents(ctx context.Context, tx *sql.Tx, namespace, workflowID, runID string, events history.History) error {
	if len(events) == 0 {
		return nil
	}
	// Prepared once per batch: a workflow task routinely produces a dozen
	// events, and re-parsing the same INSERT for each is pure overhead.
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO history_events (namespace, workflow_id, run_id, event_id, event_type, event_time, data)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare event insert: %w", err)
	}
	defer stmt.Close()

	for _, ev := range events {
		data, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("encode event %d (%s): %w", ev.ID, ev.Type(), err)
		}
		if _, err := stmt.ExecContext(ctx, namespace, workflowID, runID,
			ev.ID, int64(ev.Type()), nanos(ev.Time), data); err != nil {
			if isConstraint(err) {
				// The primary key already holds this event ID, which means
				// another writer got there first. The version check should have
				// caught it; reaching here means the row and the log disagreed,
				// and a conflict is the answer that makes the caller re-read.
				return fmt.Errorf("event %d of run %s already exists: %w",
					ev.ID, runID, persistence.ErrVersionConflict)
			}
			return fmt.Errorf("insert event %d of run %s: %w", ev.ID, runID, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

// nanos renders a timestamp for storage. Zero maps to 0, which the schema
// reserves for "unset".
func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

// fromNanos is the inverse of nanos. Every timestamp this driver returns is
// therefore in UTC, which keeps records comparable regardless of the writer's
// local zone.
func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func encodeAttrs(m map[string]string) (string, error) {
	if len(m) == 0 {
		// The empty string, not "{}": absence and emptiness are the same thing
		// to a caller, and storing one representation keeps a round trip
		// through any driver comparable.
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode attributes: %w", err)
	}
	return string(b), nil
}

func decodeAttrs(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

func encodeAttrPair(rec persistence.ExecutionRecord) (string, string, error) {
	memo, err := encodeAttrs(rec.Memo)
	if err != nil {
		return "", "", fmt.Errorf("run %s: memo: %w", rec.RunID, err)
	}
	searchAttrs, err := encodeAttrs(rec.SearchAttrs)
	if err != nil {
		return "", "", fmt.Errorf("run %s: search attributes: %w", rec.RunID, err)
	}
	return memo, searchAttrs, nil
}

func cloneAttrs(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

// ---------------------------------------------------------------------------
// Page tokens
// ---------------------------------------------------------------------------

// pageCursor is the keyset position a page resumes from.
type pageCursor struct {
	startedAt int64
	runID     string
}

// pageToken is the wire form of a cursor.
//
// It carries a fingerprint of the filter that produced it. Without one, a
// caller that changes a filter but keeps the token gets a page from the middle
// of a result set it never asked for -- a bug that looks like data loss and is
// close to impossible to reproduce.
type pageToken struct {
	StartedAt int64  `json:"s"`
	RunID     string `json:"r"`
	Filter    uint64 `json:"f"`
}

func filterFingerprint(f persistence.ListFilter) uint64 {
	h := fnv.New64a()
	status := "any"
	if f.Status != nil {
		status = f.Status.String()
	}
	// The separator cannot appear in an identifier, so two distinct filters
	// cannot collide by concatenation.
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%d",
		f.Namespace, f.WorkflowID, f.WorkflowType, status, f.StartedAfter.UnixNano())
	return h.Sum64()
}

func encodePageToken(f persistence.ListFilter, last persistence.ExecutionRecord) string {
	b, err := json.Marshal(pageToken{
		StartedAt: nanos(last.StartedAt),
		RunID:     last.RunID,
		Filter:    filterFingerprint(f),
	})
	if err != nil {
		// pageToken is three scalars; json.Marshal cannot fail on it. An empty
		// token degrades to "no more pages", which is wrong but safe.
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodePageToken(f persistence.ListFilter) (*pageCursor, error) {
	if f.PageToken == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(f.PageToken)
	if err != nil {
		return nil, fmt.Errorf("page token is not valid base64: %w", err)
	}
	var tok pageToken
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("page token is malformed: %w", err)
	}
	if tok.Filter != filterFingerprint(f) {
		return nil, errors.New("page token belongs to a different query")
	}
	return &pageCursor{startedAt: tok.StartedAt, runID: tok.RunID}, nil
}

func clampPageSize(n int) int {
	switch {
	case n <= 0:
		return defaultPageSize
	case n > maxPageSize:
		return maxPageSize
	}
	return n
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------
//
// The drivers validate independently rather than sharing a helper. These checks
// protect this driver's storage -- dense IDs, non-decreasing time, no writes
// past a terminal event -- and a driver that trusted another package to have
// done them would be one refactor away from storing garbage.

// validateCreate checks the request and returns the normalised row to store.
func validateCreate(req persistence.CreateExecutionRequest) (persistence.ExecutionRecord, error) {
	rec := req.Record
	switch {
	case rec.Namespace == "":
		return rec, fmt.Errorf("%w: namespace is empty", skald.ErrInvalidIdentifier)
	case rec.WorkflowID == "":
		return rec, fmt.Errorf("%w: workflow id is empty", skald.ErrInvalidIdentifier)
	case rec.RunID == "":
		return rec, fmt.Errorf("%w: run id is empty", skald.ErrInvalidIdentifier)
	case len(req.Events) == 0:
		return rec, fmt.Errorf("%w: a new run needs at least its %s event",
			history.ErrInvalidHistory, history.EventTypeWorkflowExecutionStarted)
	}
	// A run is created once, so full validation costs nothing measurable here
	// and rejects a malformed history at the only point where the caller still
	// has the context to fix it.
	if err := req.Events.Validate(); err != nil {
		return rec, err
	}
	if req.Events[0].ID != 1 {
		return rec, fmt.Errorf("%w: a new run must start at event 1, got %d",
			history.ErrInvalidHistory, req.Events[0].ID)
	}

	rec.StartedAt = rec.StartedAt.UTC()
	rec.ClosedAt = rec.ClosedAt.UTC()
	rec.Memo = cloneAttrs(rec.Memo)
	rec.SearchAttrs = cloneAttrs(rec.SearchAttrs)
	rec.Version = 1
	rec.LastEventID = req.Events.LastEventID()
	if rec.FirstExecutionRunID == "" {
		rec.FirstExecutionRunID = rec.RunID
	}
	if rec.StartedAt.IsZero() {
		rec.StartedAt = req.Events[0].Time.UTC()
	}
	switch {
	case !rec.Status.Terminal():
		rec.ClosedAt = time.Time{}
	case rec.ClosedAt.IsZero():
		rec.ClosedAt = req.Events[len(req.Events)-1].Time.UTC()
	}
	// Round-trip the timestamps through the storage encoding so that the record
	// this call returns is byte-identical to the one a later read produces.
	rec.StartedAt = fromNanos(nanos(rec.StartedAt))
	rec.ClosedAt = fromNanos(nanos(rec.ClosedAt))
	return rec, nil
}

// validateAppend checks that the batch is the next contiguous piece of the run.
//
// The version check has already proved the caller is not racing anyone, so a
// gap here is a caller bug rather than contention, and it is reported as the
// history-structure violation it is.
func validateAppend(row executionRow, events history.History) error {
	if len(events) == 0 {
		return nil
	}
	if !row.Open() {
		return fmt.Errorf("%w: cannot append to a %s execution", history.ErrInvalidHistory, row.Status)
	}
	want := row.LastEventID + 1
	prev := row.lastEventTime
	for i, ev := range events {
		if ev.ID != want+int64(i) {
			return fmt.Errorf("%w: event at index %d has ID %d, expected %d (IDs must be dense)",
				history.ErrInvalidHistory, i, ev.ID, want+int64(i))
		}
		if ev.Attrs == nil {
			return fmt.Errorf("%w: event %d has nil attributes", history.ErrInvalidHistory, ev.ID)
		}
		if ev.Time.IsZero() {
			return fmt.Errorf("%w: event %d has a zero timestamp", history.ErrInvalidHistory, ev.ID)
		}
		if ev.Time.Before(prev) {
			return fmt.Errorf("%w: event %d at %s precedes the previous event at %s",
				history.ErrInvalidHistory, ev.ID, ev.Time.UTC(), prev)
		}
		prev = ev.Time.UTC()
	}
	return nil
}

// validateTimers checks that every entry belongs to the run being written.
func validateTimers(namespace, workflowID, runID string, in []persistence.TimerRecord) ([]persistence.TimerRecord, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]persistence.TimerRecord, len(in))
	for i, tr := range in {
		if err := checkTimerKey(namespace, workflowID, runID, tr.TimerKey); err != nil {
			return nil, err
		}
		if tr.FireAt.IsZero() {
			return nil, fmt.Errorf("%w: timer %d of run %s has no fire time",
				skald.ErrInvalidIdentifier, tr.EventID, runID)
		}
		tr.FireAt = tr.FireAt.UTC()
		out[i] = tr
	}
	return out, nil
}

// checkTimerKey rejects a timer aimed at a different run. Letting one through
// would put an entry in the due index that no execution owns, and the timer
// service would spin on it forever.
func checkTimerKey(namespace, workflowID, runID string, k persistence.TimerKey) error {
	if k.Namespace != namespace || k.WorkflowID != workflowID || k.RunID != runID {
		return fmt.Errorf("%w: timer %s/%s/%s does not belong to run %s/%s/%s",
			skald.ErrInvalidIdentifier, k.Namespace, k.WorkflowID, k.RunID, namespace, workflowID, runID)
	}
	if k.EventID <= 0 {
		return fmt.Errorf("%w: timer of run %s has event id %d", skald.ErrInvalidIdentifier, runID, k.EventID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

// isConstraint reports whether err is a SQLite constraint violation.
//
// SQLite reports a primary key clash, a unique index clash and a foreign key
// violation with the same primary result code and differing extended codes. The
// caller decides what the violation means in its own terms, because the same
// SQLITE_CONSTRAINT is "this run already exists" on executions and "somebody
// wrote this event first" on history_events.
func isConstraint(err error) bool {
	var se *sqlite.Error
	if !errors.As(err, &se) {
		return false
	}
	// The low byte of an extended code is the primary result code.
	return se.Code()&0xff == sqlite3.SQLITE_CONSTRAINT
}

// mapClosed turns database/sql's post-Close errors into ErrClosed. A caller
// racing Close should get the sentinel it can match on rather than a message it
// would have to compare as a string.
func (s *Store) mapClosed(err error) error {
	if err == nil {
		return nil
	}
	if s.closed.Load() || errors.Is(err, sql.ErrConnDone) {
		return fmt.Errorf("%w (%w)", persistence.ErrClosed, err)
	}
	return err
}
