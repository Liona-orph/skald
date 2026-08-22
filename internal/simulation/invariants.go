package simulation

import (
	"fmt"
	"sort"

	"github.com/Liona-orph/skald/internal/persistence"
	"github.com/Liona-orph/skald/pkg/history"
	"github.com/Liona-orph/skald/pkg/skald"
)

// The invariants.
//
// Each one is a property the system claims and a class of bug it rules out. They
// are checked against the *store*, never against the simulator's own bookkeeping:
// an invariant that reads the harness's idea of the truth only proves the
// harness is self-consistent.
//
// Reads go through the raw store rather than the fault injector, because an
// injected read failure would turn a check into a coin flip. The simulator is
// the observer here; the system under test is everything on the other side of
// chaosStore.

// checkInvariants validates every run whose history has grown since it was last
// looked at.
//
// The alternative -- re-reading every history on every step -- is quadratic in
// the length of a run and turns a few hundred seeds into a few minutes. Tracking
// the newest validated event per run makes the sweep cost proportional to what
// actually changed, which is what makes "check after every step" affordable
// enough to be true rather than aspirational.
func (s *Simulation) checkInvariants() error {
	res, err := s.raw.ListExecutions(s.ctx, persistence.ListFilter{Namespace: simNamespace, PageSize: 1000})
	if err != nil {
		return s.violation("observability", fmt.Sprintf("listing executions failed: %v", err))
	}
	recs := res.Records
	// Map iteration in the memory driver's scan is unordered before it sorts, and
	// the sort is by recency; pin a total order on run ID so that two runs of the
	// same seed report the *same* violation first.
	sort.Slice(recs, func(i, j int) bool { return recs[i].RunID < recs[j].RunID })

	for _, rec := range recs {
		if s.checked[rec.RunID] == rec.LastEventID {
			continue
		}
		h, err := s.raw.ReadHistory(s.ctx, rec.Namespace, rec.WorkflowID, rec.RunID, 1, 0)
		if err != nil {
			return s.violation("observability",
				fmt.Sprintf("reading the history of %s/%s failed: %v", rec.WorkflowID, rec.RunID, err))
		}
		if err := s.checkRun(rec, h); err != nil {
			return err
		}
		s.checked[rec.RunID] = rec.LastEventID
	}
	return nil
}

// checkAll re-validates every run from scratch, ignoring the incremental cache.
// The final check uses it so that a bug in the caching cannot hide a violation.
func (s *Simulation) checkAll() error {
	s.checked = map[string]int64{}
	return s.checkInvariants()
}

func (s *Simulation) checkRun(rec persistence.ExecutionRecord, h history.History) error {
	where := fmt.Sprintf("%s/%s", rec.WorkflowID, rec.RunID)

	// 1. The structural contract of a history. This subsumes several of the
	//    checks below, and they are still written out separately: Validate is
	//    itself code under test, and an invariant nobody can find in the
	//    simulator is an invariant nobody trusts.
	if err := h.Validate(); err != nil {
		return s.violation("history.Validate", fmt.Sprintf("%s: %v", where, err))
	}

	// 2. A terminal event ends the history. Anything after it is state that was
	//    appended to an execution the world already considers finished.
	for i, ev := range h {
		if ev.Type().Terminal() && i != len(h)-1 {
			return s.violation("terminal event is last",
				fmt.Sprintf("%s: %s at event %d is followed by %s",
					where, ev.Type(), ev.ID, h[i+1].Type()))
		}
	}

	// 3. At most one workflow task in flight. This is the property that makes
	//    workflow code single-threaded from the author's point of view; two
	//    concurrent tasks would let two workers produce commands against the
	//    same state.
	if err := s.checkOneTaskInFlight(where, h); err != nil {
		return err
	}

	// 4. An activity resolves exactly once. A second result for one scheduled
	//    activity is the duplicate-execution failure durable execution exists to
	//    prevent, and it is what the duplicate-delivery fault aims at.
	if err := s.checkActivityResolvedOnce(where, h); err != nil {
		return err
	}

	// 5. The record and the history agree about whether the run is closed. A row
	//    that says RUNNING over a history with a terminal event -- or the reverse
	//    -- is a torn write, and every query in the system would then disagree
	//    with every replay.
	if rec.Status.Terminal() != h.Terminated() {
		return s.violation("record matches history",
			fmt.Sprintf("%s: the row says %s but the history %s terminated",
				where, rec.Status, map[bool]string{true: "is", false: "is not"}[h.Terminated()]))
	}
	if rec.LastEventID != h.LastEventID() {
		return s.violation("record matches history",
			fmt.Sprintf("%s: the row names event %d as the newest, the history ends at %d",
				where, rec.LastEventID, h.LastEventID()))
	}

	// 6. The result oracle.
	return s.checkResult(rec, h)
}

// checkOneTaskInFlight walks the workflow-task lifecycle.
func (s *Simulation) checkOneTaskInFlight(where string, h history.History) error {
	var startedAt int64
	for _, ev := range h {
		switch a := ev.Attrs.(type) {
		case history.WorkflowTaskStartedAttributes:
			if startedAt != 0 {
				return s.violation("at most one workflow task in flight",
					fmt.Sprintf("%s: task started at event %d while the task started at event %d was still open",
						where, ev.ID, startedAt))
			}
			startedAt = ev.ID
		case history.WorkflowTaskCompletedAttributes:
			startedAt = 0
		case history.WorkflowTaskFailedAttributes:
			startedAt = 0
		case history.WorkflowTaskTimedOutAttributes:
			startedAt = 0
		default:
			_ = a
		}
	}
	return nil
}

// checkActivityResolvedOnce counts the terminal events of each scheduled
// activity.
func (s *Simulation) checkActivityResolvedOnce(where string, h history.History) error {
	resolved := map[int64]history.Event{}
	note := func(scheduledEventID int64, ev history.Event) error {
		if prev, dup := resolved[scheduledEventID]; dup {
			return s.violation("an activity resolves at most once",
				fmt.Sprintf("%s: activity scheduled at event %d resolved twice: %s at %d and %s at %d",
					where, scheduledEventID, prev.Type(), prev.ID, ev.Type(), ev.ID))
		}
		resolved[scheduledEventID] = ev
		return nil
	}
	for _, ev := range h {
		var err error
		switch a := ev.Attrs.(type) {
		case history.ActivityTaskCompletedAttributes:
			err = note(a.ScheduledEventID, ev)
		case history.ActivityTaskFailedAttributes:
			err = note(a.ScheduledEventID, ev)
		case history.ActivityTaskTimedOutAttributes:
			err = note(a.ScheduledEventID, ev)
		case history.ActivityTaskCanceledAttributes:
			err = note(a.ScheduledEventID, ev)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// checkResult is the oracle: a finished execution must have produced the value
// its input determines, and an execution nobody cancelled must not have finished
// any other way.
//
// The second half is the more valuable of the two. Without it, a bug that made
// every workflow fail would pass every other invariant in this file: the
// histories would be structurally perfect and nothing would have resolved twice.
func (s *Simulation) checkResult(rec persistence.ExecutionRecord, h history.History) error {
	if !rec.Status.Terminal() {
		return nil
	}
	st, tracked := s.live[rec.WorkflowID]
	if !tracked {
		return s.violation("accounting",
			fmt.Sprintf("the store holds run %s/%s, which the simulator never started",
				rec.WorkflowID, rec.RunID))
	}
	where := fmt.Sprintf("%s/%s", rec.WorkflowID, rec.RunID)

	switch rec.Status {
	case skald.StatusContinuedAsNew:
		// An intermediate link in a chain owes no result; the successor does.
		return nil
	case skald.StatusCompleted:
		attrs, ok := history.AttributesAs[history.WorkflowExecutionCompletedAttributes](h[len(h)-1])
		if !ok {
			return s.violation("record matches history",
				fmt.Sprintf("%s: the row says COMPLETED but the last event is %s", where, h[len(h)-1].Type()))
		}
		var got int
		if err := s.conv.FromPayload(attrs.Result, &got); err != nil {
			return s.violation("result oracle",
				fmt.Sprintf("%s: decoding the result failed: %v", where, err))
		}
		if got != st.want {
			return s.violation("result oracle",
				fmt.Sprintf("%s (%s, n=%d) completed with %d, expected %d",
					where, st.kind.workflowType(), st.n, got, st.want))
		}
		st.verified = true
		return nil
	default:
		// Failed, canceled, terminated or timed out. Only a cancellation the
		// simulator asked for makes that acceptable.
		if !st.canceled {
			return s.violation("no unexplained failures",
				fmt.Sprintf("%s (%s, n=%d) ended as %s and nobody cancelled it; last event: %s",
					where, st.kind.workflowType(), st.n, rec.Status, describeEvent(h[len(h)-1])))
		}
		st.verified = true
		return nil
	}
}
