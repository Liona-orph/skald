// Package skald contains the public, dependency-light vocabulary shared by the
// Skald server, the worker SDK and the command line tools.
//
// Nothing in this package performs I/O. It is deliberately free of transport
// concerns so that it can be embedded in user workflow code without dragging in
// the whole engine.
package skald
