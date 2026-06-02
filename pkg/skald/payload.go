package skald

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// EncodingJSON is the identifier of the built-in codec.
const EncodingJSON = "json/plain"

// EncodingNil marks the absence of a value, distinguishing "no argument" from
// "the JSON literal null".
const EncodingNil = "binary/null"

// MaxPayloadBytes bounds a single serialized value. The engine copies payloads
// into every history event that references them, so an unbounded payload turns
// into unbounded write amplification. Callers that need to move more data
// should pass a reference (an object storage key) instead.
const MaxPayloadBytes = 2 << 20 // 2 MiB

// ErrPayloadTooLarge is returned when a value exceeds MaxPayloadBytes.
var ErrPayloadTooLarge = errors.New("skald: payload exceeds maximum size")

// Payload is a self-describing, codec-agnostic value envelope.
//
// The engine never inspects Data. Keeping the encoding out of band means a
// deployment can switch from JSON to a schema-based codec without a history
// migration: old events keep declaring their original encoding and stay
// readable forever.
type Payload struct {
	Encoding string            `json:"encoding"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Data     []byte            `json:"data,omitempty"`
}

// Size reports the number of bytes the payload contributes to a history event.
func (p *Payload) Size() int {
	if p == nil {
		return 0
	}
	n := len(p.Data) + len(p.Encoding)
	for k, v := range p.Metadata {
		n += len(k) + len(v)
	}
	return n
}

// IsNil reports whether the payload represents the absence of a value.
func (p *Payload) IsNil() bool { return p == nil || p.Encoding == EncodingNil }

// DataConverter turns Go values into Payloads and back. Implementations must be
// deterministic: given the same input they must produce byte-identical output,
// because payload bytes participate in replay consistency checks.
type DataConverter interface {
	ToPayload(value any) (*Payload, error)
	FromPayload(p *Payload, valuePtr any) error
}

// JSONConverter is the default DataConverter.
//
// It disables HTML escaping so that byte output depends only on the value, and
// it refuses to silently truncate: an oversized payload is an error at the call
// site rather than a corrupt history event discovered days later.
type JSONConverter struct{}

var _ DataConverter = JSONConverter{}

// ToPayload implements DataConverter.
func (JSONConverter) ToPayload(value any) (*Payload, error) {
	if value == nil {
		return &Payload{Encoding: EncodingNil}, nil
	}
	if rv := reflect.ValueOf(value); rv.Kind() == reflect.Ptr && rv.IsNil() {
		return &Payload{Encoding: EncodingNil}, nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, fmt.Errorf("skald: encode payload: %w", err)
	}
	// json.Encoder appends a newline; strip it so equal values compare equal.
	data := bytes.TrimSuffix(buf.Bytes(), []byte{'\n'})
	if len(data) > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: %d > %d bytes", ErrPayloadTooLarge, len(data), MaxPayloadBytes)
	}
	return &Payload{Encoding: EncodingJSON, Data: data}, nil
}

// FromPayload implements DataConverter.
func (JSONConverter) FromPayload(p *Payload, valuePtr any) error {
	if valuePtr == nil {
		return nil
	}
	if p.IsNil() {
		// Leave the destination at its zero value: a workflow that declares an
		// input it was never given should observe the zero value, not an error.
		return nil
	}
	if p.Encoding != EncodingJSON {
		return fmt.Errorf("skald: cannot decode payload with encoding %q using the JSON converter", p.Encoding)
	}
	if err := json.Unmarshal(p.Data, valuePtr); err != nil {
		return fmt.Errorf("skald: decode payload: %w", err)
	}
	return nil
}

// MustPayload is a test and example helper that panics on encoding failure.
func MustPayload(value any) *Payload {
	p, err := JSONConverter{}.ToPayload(value)
	if err != nil {
		panic(err)
	}
	return p
}
