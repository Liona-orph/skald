# 0002. HTTP/JSON transport instead of gRPC

- **Status**: Accepted
- **Date**: 2026-01-09

## Context

Skald needs a wire protocol between three parties: application code that starts
and inspects workflows, workers that poll for tasks and report results, and the
server that holds the truth.

The obvious default for a system of this shape is gRPC. Temporal and Cadence
both use it, and the reasons are good ones: a generated client per language,
binary framing, streaming, and a schema that cannot drift from the
implementation.

The traffic profile, though, is unusual. Two of the sixteen operations are long
polls held open for tens of seconds, and they are the ones that dominate request
count. A worker at rest issues one poll per task queue per fifty seconds and
nothing else. The bytes on the wire are dominated by one operation —
`PollWorkflowTask` returning a full history — which is highly repetitive JSON
that gzips roughly ten to one.

There is also a bootstrap problem. Skald has one SDK, in Go. Any second SDK, in
any language, is written by somebody who is not us, and the first thing they
have to do is speak the protocol.

## Decision

The protocol is **JSON over HTTP/1.1**, with long polling instead of streaming.
Every operation is a `POST` to a fixed path with a JSON request body and a JSON
response body. `DescribeWorkflow` also accepts `GET` with query parameters.

Supporting decisions that fall out of it:

- **`api.Service` is one Go interface implemented three times** — by the engine,
  by the HTTP handler, and by the HTTP client. A worker can be pointed at an
  in-process engine with no server at all.
- **Errors are a body, not a status.** `api.Error` carries a stable `code`
  string; the HTTP status is a compatibility layer for things that only speak
  HTTP. A proxy that rewrites the status cannot lie about the error.
- **Compression is negotiated normally.** The server gzips responses above 1 KiB
  at `BestSpeed`, which is where the history-read cost actually lives.
- **A protocol version header** (`Skald-Protocol-Version`) is sent and checked.
  Absent means "unversioned client", which is accepted; a version the server
  does not implement is refused loudly.

## Consequences

### What this buys

- **Any HTTP client is a Skald client.** `curl` a start request, poll a task from
  a shell script, write an SDK in a language with no gRPC support. There is no
  generated-code step between reading the protocol document and having something
  that works.
- **Debuggability.** A request is readable in a proxy log, a `tcpdump`, or a
  browser. During an incident that is worth more than a few microseconds per
  call.
- **The protocol is inspectable in the repository.** `pkg/api` is a hundred
  lines of plain structs with doc comments, not a `.proto` plus a generated file
  nobody reads.
- **No build-time codegen.** `go build ./...` is the whole build. No `protoc`, no
  plugin version matrix, no generated files in review diffs.
- **Standard infrastructure works.** Ingress controllers, WAFs, load balancers
  and observability tools all understand HTTP/1.1 JSON without special
  configuration for HTTP/2 trailers.

### What this costs

- **JSON encoding is the CPU floor.** A protobuf history would encode several
  times faster and be several times smaller before compression. For a system
  whose bottleneck is `fsync`, this has not mattered; for one whose bottleneck is
  request rate, it would.
- **No streaming.** A worker cannot hold one connection and receive tasks as they
  arrive; it re-polls. Each poll costs a round trip and a header set, and the
  50-second cap means an idle worker makes 72 requests an hour per queue.
- **Long polls interact badly with proxies.** The server caps polls at 50s
  specifically because ALBs, nginx and Envoy default to a 60s idle timeout, and a
  poll killed in transit is indistinguishable, from the worker's side, from a
  task accepted and dropped. That constraint is now load-bearing and is checked
  at startup against the connection idle timeout.
- **No schema enforcement across versions.** Nothing stops a field being renamed
  and silently ignored. This is mitigated by `DisallowUnknownFields` on decode —
  a client that sends `retry_polciy` gets an error rather than a workflow with no
  retry policy — but that is a boundary check, not a schema.
- **Duplicated request types.** `pkg/api` declares no `DescribeWorkflowRequest`
  because the service method takes three strings, so both `internal/frontend`
  and `pkg/client` declare their own copy of the wire shape. It is a wart, and
  it is called out where it appears.

### Reversibility

Adding gRPC later does not require changing anything above the transport.
`api.Service` is the seam: a gRPC server would implement it and a gRPC client
would too, and the engine, the worker and every test would be unaffected. That
is the main reason this decision was comfortable to make early.

## Alternatives considered

### gRPC with server streaming for task dispatch

The technically superior option for throughput, and the one a larger system
should take. A worker opens a stream and the server pushes tasks; no polling, no
proxy timeout dance, binary framing.

Rejected on cost of entry rather than merit. It adds a code generation step to
every build, a `.proto` file that has to stay in sync with the Go types that
already exist, and a hard dependency for any future SDK author. The throughput
advantage is real and, for a single-node engine whose hot path ends in an
`fsync`, unmeasurable. If Skald ever grows a sharded history service, this
decision should be revisited first.

### gRPC-web or Connect

A middle path: protobuf schemas with an HTTP/1.1-compatible framing.

Rejected because it keeps the codegen dependency, which is the part that was
actually objectionable, while giving up the "readable with curl" property that
motivated the choice.

### WebSockets for the poll path

Keeps the JSON, removes the polling.

Rejected because it introduces a second connection lifecycle — reconnect,
backoff, resumption, heartbeat — for exactly two of sixteen operations, and
because a WebSocket is not meaningfully easier to speak from a new language than
a long poll is. The long poll's failure mode is "you get an empty response and
poll again", which requires no recovery logic at all.

### Server-Sent Events for `GetHistory` follow

Attractive for the "tail a workflow's history" use case.

Rejected as premature: `GetHistory` with `wait_for_new` already gives a client an
event-driven follow loop with one line of client code and no new framing. If
history tailing becomes a common interactive operation, this is the cheapest
thing to add.
