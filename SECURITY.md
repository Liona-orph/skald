# Security policy

## Supported versions

Skald is pre-1.0. Only the latest minor release receives security fixes.

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |
| < 0.1 | No |

When 1.0 ships, this table becomes "the current minor and the one before it".

## Reporting a vulnerability

**Do not open a public issue.**

Report privately through either channel:

- **GitHub Security Advisories** — the [Report a
  vulnerability](https://github.com/skald-io/skald/security/advisories/new)
  form. Preferred: it creates a private fork for the fix and handles the CVE
  request.
- **Email** — `security@skald.io`.

Please include:

- The version or commit affected.
- What an attacker can do, and what access they need to do it.
- Reproduction steps or a proof of concept.
- Your assessment of severity, and any mitigation you know of.

You will get a reply whether or not we think it is a vulnerability, with the
reasoning.

## Response targets

| Stage | Target |
| --- | --- |
| Acknowledgement | 3 working days |
| Initial assessment, with a severity and a plan | 10 working days |
| Fix for a critical or high issue | 30 days from assessment |
| Fix for a medium or low issue | Next scheduled release |
| Public disclosure | 90 days from the report, or when a fix ships, whichever is first |

If we are going to miss a target we will tell you before it passes, with the
reason. If you disagree with our severity assessment, say so — we would rather
argue about it than be quietly wrong.

We support coordinated disclosure and will credit you in the advisory and the
changelog unless you ask us not to. There is no bug bounty.

## Scope

**In scope:**

- Authentication bypass on any protected route.
- Remote crashes, unbounded memory or CPU consumption triggerable by a
  well-formed request.
- History corruption: any input that makes the engine write a history that fails
  `History.Validate`, or that makes two replicas disagree.
- Cross-namespace data access.
- Injection or escape through payloads, identifiers, memo values or headers.
- Secrets leaking into logs, metrics, traces or error responses.
- Supply-chain issues in the dependency set.

**Out of scope**, because they are documented properties rather than defects
(see the limitations section of the [README](README.md)):

- **No TLS.** `skaldd` serves plain HTTP by design; terminate TLS at a proxy.
- **The bearer token is authentication, not authorization.** It answers "may you
  talk to this server" and nothing else. There is no per-caller identity and no
  per-namespace access control. Any authenticated caller can read, start,
  signal, cancel and terminate any workflow in any namespace.
- **Denial of service by an authenticated client.** A caller who can start
  workflows can exhaust a queue's backlog or fill the store. Rate limiting and
  quotas do not exist.
- **The in-memory store losing data on restart.** It warns loudly at startup.
- **`synchronous=NORMAL` losing the last few transactions on power loss.**
  Documented, and configurable with `sqlite.WithSynchronous`.
- Findings that require an attacker to already have filesystem access to the
  store, or the ability to run code in the server process.
- Missing security headers on an API that serves only JSON to non-browser
  clients.
- Anything in a third-party dependency that Skald does not actually reach.

If you are unsure whether something is in scope, report it. We would rather
triage a non-issue than miss a real one.

## Deployment guidance

The defaults are chosen to fail safe, and the two that matter most are:

- **`--addr` defaults to `127.0.0.1:7233`.** A durable execution engine with no
  authentication configured should not be reachable from the network by
  accident, so exposing it is a decision an operator makes explicitly.
- **`/health` and `/ready` are public; everything else, including `/metrics`, is
  authenticated** when a token is set.

For a networked deployment:

1. Set `SKALD_AUTH_TOKEN` (the environment variable, not the flag — a flag is
   visible in `ps`).
2. Terminate TLS at a proxy and keep `skaldd` on an interface only the proxy can
   reach.
3. Give the metrics scraper the bearer token.
4. Treat workflow inputs, memos and search attributes as data you will see in
   logs and in `skaldctl` output. Do not put secrets in them — payloads are
   copied into the history and kept forever.
5. Restrict filesystem access to the SQLite file. It contains every payload
   every workflow has ever handled.

## What Skald does defensively

Stated so you know what is already covered:

- Bearer tokens are compared in constant time.
- Panics in a handler are contained, logged with the stack, and returned to the
  client as a bare `internal` error — a stack trace names internal packages,
  paths and sometimes argument values, which is reconnaissance handed to whoever
  triggered the panic.
- Request bodies are capped and decoded strictly, with unknown fields rejected.
- Client-supplied `X-Request-Id` values are truncated to 128 bytes and stripped
  of control characters before they reach a log line.
- `X-Forwarded-For` is deliberately ignored: it is trivially forged, and treating
  it as truth without knowing the proxy topology puts an attacker-controlled
  string in every log line.
- The readiness probe logs the underlying store error and does not return it —
  a readiness body is scraped by infrastructure that is frequently less trusted
  than the log, and store errors can carry connection strings.
- `Config.LogValue` redacts the auth token on the type, so no call site can
  accidentally log it.
- CI runs CodeQL, `govulncheck`, dependency review on every pull request, and a
  scheduled fuzz run.
