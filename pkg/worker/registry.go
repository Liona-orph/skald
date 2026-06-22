package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	internalwf "github.com/skald-io/skald/internal/workflow"
	"github.com/skald-io/skald/pkg/skald"
	"github.com/skald-io/skald/pkg/workflow"
)

// ErrNotRegistered is returned when a task names a type this worker does not
// know. It is wrapped, so callers can branch on it with errors.Is.
var ErrNotRegistered = errors.New("worker: type is not registered")

// RegisterOptions names a registration explicitly.
type RegisterOptions struct {
	// Name overrides the function's own name. Required for closures and methods,
	// whose runtime names are neither stable nor meaningful.
	Name string
}

// workflowDefinition is a validated, registered workflow.
type workflowDefinition struct {
	name string
	fn   reflect.Value
}

// activityDefinition is a validated, registered activity.
type activityDefinition struct {
	name string
	fn   reflect.Value
}

// Registry holds the workflow and activity implementations a worker serves.
//
// Registration validates signatures immediately, and that timing is the whole
// point. A workflow whose first parameter is a context.Context instead of a
// workflow.Context, or an activity that returns three values, is a mistake that
// costs nothing to catch at startup and costs an incident to catch when the
// first task for it is dispatched -- which, for a workflow type that only runs
// at month end, may be four weeks after the deploy.
type Registry struct {
	mu         sync.RWMutex
	workflows  map[string]*workflowDefinition
	activities map[string]*activityDefinition
	conv       skald.DataConverter
}

// NewRegistry returns an empty registry using conv for payloads.
func NewRegistry(conv skald.DataConverter) *Registry {
	if conv == nil {
		conv = skald.JSONConverter{}
	}
	return &Registry{
		workflows:  map[string]*workflowDefinition{},
		activities: map[string]*activityDefinition{},
		conv:       conv,
	}
}

// contextType is the reflect.Type of the standard library's context.Context.
var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()

// RegisterWorkflow validates and registers a workflow implementation.
//
// The signature must be:
//
//	func(workflow.Context, ...) (T, error)
//	func(workflow.Context, ...) error
func (r *Registry) RegisterWorkflow(fn any, opts RegisterOptions) error {
	v := reflect.ValueOf(fn)
	name, err := registrationName(fn, opts.Name, "workflow")
	if err != nil {
		return err
	}
	if err := validateWorkflowFunc(v, name); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.workflows[name]; dup {
		return fmt.Errorf("worker: workflow %q is already registered; two implementations "+
			"under one name would make which one runs depend on registration order", name)
	}
	r.workflows[name] = &workflowDefinition{name: name, fn: v}
	return nil
}

// RegisterActivity validates and registers an activity implementation.
//
// The signature must be:
//
//	func(context.Context, ...) (T, error)
//	func(context.Context, ...) error
func (r *Registry) RegisterActivity(fn any, opts RegisterOptions) error {
	v := reflect.ValueOf(fn)
	name, err := registrationName(fn, opts.Name, "activity")
	if err != nil {
		return err
	}
	if err := validateActivityFunc(v, name); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.activities[name]; dup {
		return fmt.Errorf("worker: activity %q is already registered", name)
	}
	r.activities[name] = &activityDefinition{name: name, fn: v}
	return nil
}

// ReplaceWorkflow registers fn under name, replacing any existing entry.
//
// It exists for one honest reason: proving that non-determinism detection works
// requires swapping a workflow's implementation between two tasks of the same
// execution, which is exactly what a bad deploy does.
func (r *Registry) ReplaceWorkflow(fn any, opts RegisterOptions) error {
	name, err := registrationName(fn, opts.Name, "workflow")
	if err != nil {
		return err
	}
	r.mu.Lock()
	delete(r.workflows, name)
	r.mu.Unlock()
	return r.RegisterWorkflow(fn, opts)
}

// WorkflowNames lists the registered workflow types, for diagnostics.
func (r *Registry) WorkflowNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedKeys(r.workflows)
}

// ActivityNames lists the registered activity types.
func (r *Registry) ActivityNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedKeys(r.activities)
}

// WorkflowFunc returns the registered workflow as a payload-level function the
// replay machinery can call without knowing anything about reflection.
func (r *Registry) WorkflowFunc(name string) (internalwf.WorkflowFunc, error) {
	r.mu.RLock()
	def, ok := r.workflows[name]
	known := sortedKeys(r.workflows)
	conv := r.conv
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: workflow %q; this worker knows %s",
			ErrNotRegistered, name, formatKnown(known))
	}
	fn := def.fn
	return func(ctx workflow.Context, input *skald.Payload) (*skald.Payload, error) {
		return internalwf.CallFunc(conv, fn, reflect.ValueOf(ctx), input)
	}, nil
}

// ActivityFunc returns the registered activity as a payload-level function.
func (r *Registry) ActivityFunc(name string) (func(context.Context, *skald.Payload) (*skald.Payload, error), error) {
	r.mu.RLock()
	def, ok := r.activities[name]
	known := sortedKeys(r.activities)
	conv := r.conv
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: activity %q; this worker knows %s",
			ErrNotRegistered, name, formatKnown(known))
	}
	fn := def.fn
	return func(ctx context.Context, input *skald.Payload) (*skald.Payload, error) {
		return internalwf.CallFunc(conv, fn, reflect.ValueOf(ctx), input)
	}, nil
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// registrationName resolves the name a function is registered under and refuses
// the cases where the runtime name is not a usable identifier.
func registrationName(fn any, override, kind string) (string, error) {
	if override != "" {
		return override, nil
	}
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return "", fmt.Errorf("worker: cannot register a %T as a %s; pass the function itself", fn, kind)
	}
	full := runtime.FuncForPC(v.Pointer()).Name()
	// A closure's runtime name is "pkg.Outer.func1": neither stable across an
	// edit to the enclosing function nor meaningful to anyone reading a history.
	if strings.Contains(full, ".func") {
		return "", fmt.Errorf("worker: %s %q is an anonymous function or closure; "+
			"register it with an explicit Name so that the type recorded in history "+
			"survives a refactor of the surrounding code", kind, full)
	}
	name := internalwf.FunctionName(fn)
	if name == "" {
		return "", fmt.Errorf("worker: could not derive a name for %s %v", kind, full)
	}
	if err := skald.ValidateTypeName(name); err != nil {
		return "", fmt.Errorf("worker: %s name %q: %w", kind, name, err)
	}
	return name, nil
}

func validateWorkflowFunc(v reflect.Value, name string) error {
	if v.Kind() != reflect.Func {
		return fmt.Errorf("worker: workflow %q is a %s, not a function", name, v.Kind())
	}
	t := v.Type()
	if t.NumIn() < 1 || t.In(0) != internalwf.ContextType {
		return fmt.Errorf("worker: workflow %q must take workflow.Context as its first parameter, got %s; "+
			"a workflow that takes a context.Context cannot block on the dispatcher and would "+
			"deadlock the first time it waited for anything",
			name, describeParams(t))
	}
	if t.IsVariadic() {
		return fmt.Errorf("worker: workflow %q is variadic; arguments are decoded positionally from "+
			"the recorded input, so the count must be fixed", name)
	}
	if err := validateResults(t, name, "workflow"); err != nil {
		return err
	}
	return validateArgTypes(t, name, "workflow")
}

func validateActivityFunc(v reflect.Value, name string) error {
	if v.Kind() != reflect.Func {
		return fmt.Errorf("worker: activity %q is a %s, not a function", name, v.Kind())
	}
	t := v.Type()
	if t.NumIn() < 1 || t.In(0) != contextType {
		return fmt.Errorf("worker: activity %q must take context.Context as its first parameter, got %s; "+
			"without it the activity cannot observe its deadline or a cancellation",
			name, describeParams(t))
	}
	if t.IsVariadic() {
		return fmt.Errorf("worker: activity %q is variadic; arguments are decoded positionally", name)
	}
	if err := validateResults(t, name, "activity"); err != nil {
		return err
	}
	return validateArgTypes(t, name, "activity")
}

func validateResults(t reflect.Type, name, kind string) error {
	switch t.NumOut() {
	case 1:
		if t.Out(0) != internalwf.ErrorType {
			return fmt.Errorf("worker: %s %q returns a single %s; a one-result %s must return error",
				kind, name, t.Out(0), kind)
		}
	case 2:
		if t.Out(1) != internalwf.ErrorType {
			return fmt.Errorf("worker: %s %q returns (%s, %s); the second result must be error",
				kind, name, t.Out(0), t.Out(1))
		}
	default:
		return fmt.Errorf("worker: %s %q returns %d values; it must return (T, error) or error",
			kind, name, t.NumOut())
	}
	return nil
}

// validateArgTypes rejects parameters that cannot survive a round trip through
// a payload. Catching them here turns a runtime encoding failure -- which
// surfaces as a failed workflow task at 3am -- into a startup error.
func validateArgTypes(t reflect.Type, name, kind string) error {
	for i := 1; i < t.NumIn(); i++ {
		if err := serializableType(t.In(i)); err != nil {
			return fmt.Errorf("worker: %s %q parameter %d: %w", kind, name, i, err)
		}
	}
	if t.NumOut() == 2 {
		if err := serializableType(t.Out(0)); err != nil {
			return fmt.Errorf("worker: %s %q result: %w", kind, name, err)
		}
	}
	return nil
}

func serializableType(t reflect.Type) error {
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return fmt.Errorf("type %s cannot be encoded into a payload", t)
	case reflect.Ptr, reflect.Slice, reflect.Array:
		return serializableType(t.Elem())
	case reflect.Map:
		if err := serializableType(t.Key()); err != nil {
			return err
		}
		return serializableType(t.Elem())
	}
	return nil
}

func describeParams(t reflect.Type) string {
	if t.NumIn() == 0 {
		return "a function with no parameters"
	}
	parts := make([]string, 0, t.NumIn())
	for i := 0; i < t.NumIn(); i++ {
		parts = append(parts, t.In(i).String())
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func formatKnown(names []string) string {
	if len(names) == 0 {
		return "nothing (its registry is empty, which usually means the worker was started " +
			"before the registrations ran)"
	}
	return strings.Join(names, ", ")
}

func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
