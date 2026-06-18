package workflow

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/skald-io/skald/pkg/skald"
)

// FunctionName derives the registered name of a workflow or activity from the
// function value itself.
//
// It is the reason `workflow.ExecuteActivity(ctx, ChargeCard, req)` works: the
// compiler will not let you misspell a symbol, so passing the function is
// strictly safer than passing a string, and this turns the symbol back into the
// name the registry uses. A closure or method value has no useful name, which is
// why registration validates the result rather than trusting it.
func FunctionName(fn any) string {
	if s, ok := fn.(string); ok {
		return s
	}
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return ""
	}
	full := runtime.FuncForPC(v.Pointer()).Name()
	// "github.com/acme/billing.ChargeCard" -> "ChargeCard"
	if i := strings.LastIndex(full, "."); i >= 0 {
		full = full[i+1:]
	}
	// Method values and generic instantiations pick up suffixes such as
	// "-fm" and "[...]"; strip them so the name stays stable across builds.
	full = strings.TrimSuffix(full, "-fm")
	if i := strings.Index(full, "["); i >= 0 {
		full = full[:i]
	}
	return full
}

// EncodeArgs packs a call's arguments into a single payload.
//
// Arguments are always encoded as a JSON array, even when there is exactly one.
// A uniform shape means the decoder never has to guess, and adding a second
// parameter to an activity does not silently change the encoding of the first.
func EncodeArgs(conv skald.DataConverter, args []any) (*skald.Payload, error) {
	if len(args) == 0 {
		return conv.ToPayload(nil)
	}
	return conv.ToPayload(args)
}

// DecodeArgs unpacks a payload into values of the given types.
//
// The array form is tried first. A payload that is not an array is accepted for
// a single-parameter function, which is what lets a client that encoded a bare
// value -- a perfectly reasonable thing for a caller in another language to do
// -- still start a workflow.
func DecodeArgs(conv skald.DataConverter, p *skald.Payload, types []reflect.Type) ([]reflect.Value, error) {
	out := make([]reflect.Value, len(types))
	ptrs := make([]any, len(types))
	for i, t := range types {
		v := reflect.New(t)
		out[i] = v
		ptrs[i] = v.Interface()
	}
	if len(types) == 0 || p.IsNil() {
		return derefAll(out), nil
	}

	// encoding/json decodes into the pointers already stored in the interface
	// elements rather than replacing them, which is what makes a heterogeneous
	// argument list decodable in one pass.
	if err := conv.FromPayload(p, &ptrs); err == nil {
		return derefAll(out), nil
	} else if len(types) != 1 {
		return nil, fmt.Errorf("decode %d argument(s): %w", len(types), err)
	}

	if err := conv.FromPayload(p, ptrs[0]); err != nil {
		return nil, fmt.Errorf("decode argument: %w", err)
	}
	return derefAll(out), nil
}

func derefAll(vals []reflect.Value) []reflect.Value {
	out := make([]reflect.Value, len(vals))
	for i, v := range vals {
		out[i] = v.Elem()
	}
	return out
}

// CallFunc invokes fn reflectively with a leading context-like value and the
// decoded arguments, and normalises the (T, error) / error return shapes.
//
// leading is the first parameter's value -- a context.Context for an activity, a
// workflow.Context for a workflow. Passing it in rather than constructing it
// here keeps this helper free of any opinion about which world it is in.
func CallFunc(conv skald.DataConverter, fn reflect.Value, leading reflect.Value, input *skald.Payload) (*skald.Payload, error) {
	t := fn.Type()
	types := make([]reflect.Type, 0, t.NumIn())
	for i := 1; i < t.NumIn(); i++ {
		types = append(types, t.In(i))
	}
	args, err := DecodeArgs(conv, input, types)
	if err != nil {
		return nil, err
	}
	results := fn.Call(append([]reflect.Value{leading}, args...))
	return normalizeResults(conv, results)
}

// normalizeResults turns a validated function's return values into a payload and
// an error.
func normalizeResults(conv skald.DataConverter, results []reflect.Value) (*skald.Payload, error) {
	switch len(results) {
	case 1:
		return nil, errorFrom(results[0])
	case 2:
		if err := errorFrom(results[1]); err != nil {
			return nil, err
		}
		p, err := conv.ToPayload(results[0].Interface())
		if err != nil {
			return nil, fmt.Errorf("encode result: %w", err)
		}
		return p, nil
	}
	return nil, fmt.Errorf("function returned %d values; registration should have rejected it", len(results))
}

func errorFrom(v reflect.Value) error {
	if v.IsNil() {
		return nil
	}
	err, _ := v.Interface().(error)
	return err
}

// ErrorType is the reflect.Type of the error interface, used by signature
// validation in the registry.
var ErrorType = reflect.TypeOf((*error)(nil)).Elem()

// ContextType is the reflect.Type of workflow.Context.
var ContextType = reflect.TypeOf((*Context)(nil)).Elem()
