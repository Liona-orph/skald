<!--
Thank you for the change. The checklist is short on purpose: everything on it
is something CI cannot check for you.

CI runs `make ci` -- gofmt, vet, golangci-lint, the tests under -race, a build
of the release matrix, and a 5,000-seed simulation sweep. Run `make ci` before
pushing and none of it will surprise you.
-->

## What this changes

<!-- One paragraph. What behaviour is different after this merges. -->

## Why

<!--
The problem, not the patch. Link the issue if there is one; if there is not,
the paragraph here is the issue.
-->

Closes #

## How it was verified

<!--
Beyond "the tests pass". A new test that fails without the change, a
simulation seed that used to break, a `skaldctl` transcript, a benchmark
delta -- whatever makes the change checkable by someone who did not write it.
-->

## Compatibility

- [ ] No change to the persisted history format, or the change is additive and
      old histories still decode. <!-- Event type codes are permanent. -->
- [ ] No change to the wire protocol, or old clients still work against a new
      server.
- [ ] No change to replay behaviour, or the change is behind a
      `workflow.GetVersion` gate and in-flight executions are safe.
- [ ] Public API changes are documented in `CHANGELOG.md` under Unreleased.

## Checklist

- [ ] `make ci` passes locally.
- [ ] Tests cover the new behaviour, including its failure path.
- [ ] Comments explain *why*, where the reason is not obvious from the code.
- [ ] Documentation is updated if this changes how Skald is used or operated.
- [ ] A decision with long-lived consequences has an ADR in `docs/adr/`.

<!--
On commit messages: the changelog is generated from them, grouped by
conventional-commit prefix (feat, fix, perf, docs). A squashed merge uses the
pull request title, so give this one a prefix.
-->
