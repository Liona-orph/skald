// Package history defines Skald's event history: the append-only log that is
// the single source of truth for every workflow execution.
//
// Three rules govern everything in this package.
//
//  1. Events are immutable and their numeric codes are permanent. A code that
//     has shipped is never reused for a different meaning, because histories
//     written years ago must still decode.
//  2. Every observable effect a workflow has on the outside world is preceded
//     by an event that records the intent, and followed by an event that
//     records the outcome. Replay therefore never re-issues an effect that
//     already happened.
//  3. Nothing non-deterministic reaches workflow code except through an event.
//     Time, randomness and retry jitter are all drawn once by the server,
//     written to the history, and replayed from it.
package history
