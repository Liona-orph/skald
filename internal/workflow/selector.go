package workflow

// Selector waits for the first of several events and runs the branch that fired.
//
// # Why this is not Go's select
//
// Go's select picks uniformly at random among the ready cases. That is the right
// choice for a concurrent program and exactly the wrong one for a replayable
// one: two replays of the same history would take different branches and produce
// different commands. Skald's selector is therefore *ordered* -- it always runs
// the first branch, in registration order, that is ready. Registration order is
// a property of the workflow code, so it is the same on every replay.
//
// The practical consequence for workflow authors is that a selector expresses a
// priority, not a race. A timeout registered before an activity result will
// always win a tie; register it after and the result wins. That is a feature: it
// makes "prefer the cached answer, fall back to the slow one" expressible, and
// it means a flaky test cannot be caused by branch selection.
type Selector interface {
	// AddFuture registers a branch that fires when f is ready.
	AddFuture(f Awaitable, fn func()) Selector
	// AddReceive registers a branch that fires when ch has a value or is closed.
	AddReceive(ch Awaitable, fn func()) Selector
	// AddDefault registers the branch taken when nothing else is ready. A
	// selector with a default never blocks.
	AddDefault(fn func()) Selector
	// Select runs exactly one branch, blocking until one is ready unless a
	// default was registered.
	Select(ctx Context)
	// HasPending reports whether any registered branch is ready right now.
	HasPending() bool
}

type selectorBranch struct {
	// ready is nil for the default branch.
	ready Awaitable
	fn    func()
}

type selectorImpl struct {
	dispatcher *Dispatcher
	name       string
	branches   []selectorBranch
	hasDefault bool
	defaultFn  func()
}

var _ Selector = (*selectorImpl)(nil)

// NewSelector returns an empty selector bound to ctx's dispatcher.
func NewSelector(ctx Context, name string) Selector {
	if name == "" {
		name = "selector"
	}
	return &selectorImpl{dispatcher: DispatcherFrom(ctx), name: name}
}

func (s *selectorImpl) AddFuture(f Awaitable, fn func()) Selector {
	s.branches = append(s.branches, selectorBranch{ready: f, fn: fn})
	return s
}

func (s *selectorImpl) AddReceive(ch Awaitable, fn func()) Selector {
	s.branches = append(s.branches, selectorBranch{ready: ch, fn: fn})
	return s
}

func (s *selectorImpl) AddDefault(fn func()) Selector {
	s.hasDefault = true
	s.defaultFn = fn
	return s
}

func (s *selectorImpl) HasPending() bool {
	for _, b := range s.branches {
		if b.ready != nil && b.ready.IsReady() {
			return true
		}
	}
	return false
}

func (s *selectorImpl) Select(ctx Context) {
	co := mustCoroutine(ctx, "Selector.Select")
	if s.dispatcher != nil {
		s.dispatcher.markProgress()
	}
	for {
		for _, b := range s.branches {
			if b.ready != nil && b.ready.IsReady() {
				if b.fn != nil {
					b.fn()
				}
				return
			}
		}
		if s.hasDefault {
			if s.defaultFn != nil {
				s.defaultFn()
			}
			return
		}
		co.yield(s.name)
	}
}
