// Package execution owns the derived, in-memory view of a workflow run and the
// rules for advancing it.
//
// The history is the source of truth; MutableState is a cache of it that makes
// the questions the engine asks constant time ("is activity 17 still pending?",
// "is a workflow task already in flight?"). Every mutation goes through
// AppendEvent so that state and history can never disagree: there is exactly
// one code path that changes state, and it always writes an event first.
package execution
