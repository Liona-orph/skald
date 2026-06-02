package skald

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Identifier limits. These are enforced at the API boundary so that an operator
// can reason about the maximum size of a persisted row without reading code.
const (
	MaxNamespaceLen  = 64
	MaxWorkflowIDLen = 512
	MaxTaskQueueLen  = 256
	MaxTypeNameLen   = 256
)

// DefaultNamespace is used when a caller does not specify one. Multi-tenancy is
// always on; the default namespace is simply the tenant every single-tenant
// deployment ends up using.
const DefaultNamespace = "default"

var (
	// ErrInvalidIdentifier is returned by the Validate* helpers.
	ErrInvalidIdentifier = errors.New("skald: invalid identifier")

	namespacePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)
)

// ValidateNamespace reports whether ns is usable as a namespace name.
func ValidateNamespace(ns string) error {
	switch {
	case ns == "":
		return fmt.Errorf("%w: namespace must not be empty", ErrInvalidIdentifier)
	case len(ns) > MaxNamespaceLen:
		return fmt.Errorf("%w: namespace exceeds %d bytes", ErrInvalidIdentifier, MaxNamespaceLen)
	case !namespacePattern.MatchString(ns):
		return fmt.Errorf("%w: namespace %q must match %s", ErrInvalidIdentifier, ns, namespacePattern)
	}
	return nil
}

// ValidateWorkflowID reports whether id is usable as a workflow identifier.
//
// Workflow IDs are chosen by the caller and are frequently natural keys such as
// "order-1234". The only hard rules are non-emptiness, a length bound and the
// absence of control characters, which would otherwise corrupt log output and
// terminal-based tooling.
func ValidateWorkflowID(id string) error {
	switch {
	case id == "":
		return fmt.Errorf("%w: workflow id must not be empty", ErrInvalidIdentifier)
	case len(id) > MaxWorkflowIDLen:
		return fmt.Errorf("%w: workflow id exceeds %d bytes", ErrInvalidIdentifier, MaxWorkflowIDLen)
	case strings.ContainsFunc(id, isControl):
		return fmt.Errorf("%w: workflow id must not contain control characters", ErrInvalidIdentifier)
	}
	return nil
}

// ValidateTaskQueue reports whether q is usable as a task queue name.
func ValidateTaskQueue(q string) error {
	switch {
	case q == "":
		return fmt.Errorf("%w: task queue must not be empty", ErrInvalidIdentifier)
	case len(q) > MaxTaskQueueLen:
		return fmt.Errorf("%w: task queue exceeds %d bytes", ErrInvalidIdentifier, MaxTaskQueueLen)
	case strings.ContainsFunc(q, isControl):
		return fmt.Errorf("%w: task queue must not contain control characters", ErrInvalidIdentifier)
	}
	return nil
}

// ValidateTypeName reports whether n is usable as a workflow or activity type.
func ValidateTypeName(n string) error {
	switch {
	case n == "":
		return fmt.Errorf("%w: type name must not be empty", ErrInvalidIdentifier)
	case len(n) > MaxTypeNameLen:
		return fmt.Errorf("%w: type name exceeds %d bytes", ErrInvalidIdentifier, MaxTypeNameLen)
	case strings.ContainsFunc(n, isControl):
		return fmt.Errorf("%w: type name must not contain control characters", ErrInvalidIdentifier)
	}
	return nil
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

// WorkflowExecution identifies one concrete run of a workflow.
//
// WorkflowID is caller-supplied and unique among *open* executions within a
// namespace. RunID is server-assigned and globally unique; it is what makes the
// history of a retried or continued-as-new workflow addressable forever.
type WorkflowExecution struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

func (e WorkflowExecution) String() string {
	if e.RunID == "" {
		return e.WorkflowID
	}
	return e.WorkflowID + "/" + e.RunID
}

// IsZero reports whether the execution carries no identity at all.
func (e WorkflowExecution) IsZero() bool { return e.WorkflowID == "" && e.RunID == "" }
