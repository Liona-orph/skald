# Skald build tooling.
#
# `make` on its own prints the target list. Everything here runs from a clean
# checkout with nothing installed but Go: the single external tool this project
# uses, golangci-lint, is fetched into ./bin at a pinned version by the target
# that needs it, so a contributor's linter and CI's linter are the same binary.
#
# Overridable knobs are all `?=`; the ones worth knowing are printed by `help`.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

MODULE := github.com/Liona-orph/skald
BIN    := bin
DIST   := dist

GO       ?= go
# PKGS exists so a target can be pointed at part of the tree while a package is
# mid-rewrite: `make test PKGS=./internal/engine/...`.
PKGS     ?= ./...
TESTFLAGS ?=

# Build information, resolved from git and injected at link time. A checkout
# without tags, or an export with no .git at all, still builds -- it just says
# "dev", which is exactly what it is.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
# SOURCE_DATE_EPOCH is honoured so a rebuild of the same commit produces the
# same binary; see https://reproducible-builds.org/specs/source-date-epoch/.
BUILD_DATE ?= $(shell date -u -d "@$${SOURCE_DATE_EPOCH:-$$(date +%s)}" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
                || date -u +%Y-%m-%dT%H:%M:%SZ)

# `?=` defines a recursively expanded variable, so the shell above would run
# again at every reference -- once per binary, which is how the two binaries of
# one build end up stamped a second apart. Flattening pins them.
VERSION    := $(VERSION)
COMMIT     := $(COMMIT)
BUILD_DATE := $(BUILD_DATE)

# -s -w drop the symbol table and DWARF. That is roughly a third off the binary
# and nothing a production deployment reads; a panic still carries file, line
# and function because those live in the pclntab, which is not stripped. Build
# without them when you intend to attach a debugger.
LDFLAGS_COMMON   := -s -w
LDFLAGS_SKALDD   := $(LDFLAGS_COMMON) \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildDate=$(BUILD_DATE)
# skaldctl's build vars live in the commands package, not in main: main is eight
# lines and the version string is rendered next to the command that prints it.
LDFLAGS_SKALDCTL := $(LDFLAGS_COMMON) \
	-X $(MODULE)/cmd/skaldctl/commands.version=$(VERSION) \
	-X $(MODULE)/cmd/skaldctl/commands.commit=$(COMMIT) \
	-X $(MODULE)/cmd/skaldctl/commands.buildDate=$(BUILD_DATE)

# -trimpath keeps the build directory out of the binary. Without it the panic
# traces of a release build name the path on the release machine.
GOBUILDFLAGS ?= -trimpath

COVERPROFILE := coverage.out
COVERHTML    := coverage.html

GOLANGCI_VERSION := v1.62.2
GOLANGCI         := $(BIN)/golangci-lint

FUZZTIME  ?= 30s
SIM_SEEDS ?= 300
SIM_FLAGS ?=

IMAGE     ?= ghcr.io/Liona-orph/skald
IMAGE_TAG ?= $(VERSION)

.PHONY: help
help: ## Print this help
	@printf 'Skald. Run `make <target>`.\n\n'
	@awk 'BEGIN {FS = ":.*?## "} \
		/^[a-zA-Z0-9][a-zA-Z0-9_-]*:.*?## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\nOverrides: VERSION=%s COMMIT=%s PKGS=%s FUZZTIME=%s SIM_SEEDS=%s\n' \
		'$(VERSION)' '$(COMMIT)' '$(PKGS)' '$(FUZZTIME)' '$(SIM_SEEDS)'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Compile skaldd and skaldctl into ./bin
	@mkdir -p $(BIN)
	$(GO) build $(GOBUILDFLAGS) -ldflags '$(LDFLAGS_SKALDD)'   -o $(BIN)/skaldd   ./cmd/skaldd
	$(GO) build $(GOBUILDFLAGS) -ldflags '$(LDFLAGS_SKALDCTL)' -o $(BIN)/skaldctl ./cmd/skaldctl
	@$(BIN)/skaldd --version

.PHONY: install
install: ## go install both binaries into GOBIN
	$(GO) install $(GOBUILDFLAGS) -ldflags '$(LDFLAGS_SKALDD)'   ./cmd/skaldd
	$(GO) install $(GOBUILDFLAGS) -ldflags '$(LDFLAGS_SKALDCTL)' ./cmd/skaldctl

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

.PHONY: test
test: ## Run the unit tests
	$(GO) test $(TESTFLAGS) $(PKGS)

.PHONY: test-race
test-race: ## Run the unit tests under the race detector
	CGO_ENABLED=1 $(GO) test -race $(TESTFLAGS) $(PKGS)

.PHONY: test-short
test-short: ## Run the unit tests with -short (skips the slow suites)
	$(GO) test -short $(TESTFLAGS) $(PKGS)

# -coverpkg is the whole module rather than the package under test, because most
# of this codebase is exercised across package boundaries: the engine's tests
# drive execution/, persistence/ and matching/, and per-package coverage would
# report all three as untested.
.PHONY: cover
cover: ## Coverage profile, HTML report and the total
	$(GO) test -covermode=atomic -coverprofile=$(COVERPROFILE) -coverpkg=./... $(PKGS)
	$(GO) tool cover -html=$(COVERPROFILE) -o $(COVERHTML)
	@echo
	@$(GO) tool cover -func=$(COVERPROFILE) | tail -n 1
	@echo "HTML report: $(COVERHTML)"

.PHONY: bench
bench: ## Run the benchmarks
	$(GO) test -run '^$$' -bench . -benchmem $(PKGS)

.PHONY: fuzz
fuzz: ## Run every fuzz target for FUZZTIME each (default 30s)
	@found=0; \
	for pkg in $$($(GO) list $(PKGS)); do \
		dir=$$($(GO) list -f '{{.Dir}}' $$pkg); \
		targets=$$(grep -hoE '^func +Fuzz[A-Za-z0-9_]*' $$dir/*_test.go 2>/dev/null \
			| awk '{print $$2}' | sort -u || true); \
		for target in $$targets; do \
			found=1; \
			echo "==> $$pkg $$target for $(FUZZTIME)"; \
			$(GO) test $$pkg -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME); \
		done; \
	done; \
	if [ $$found -eq 0 ]; then \
		echo "no fuzz targets in $(PKGS)"; \
	fi

# -count=1 because a cached simulation result is worthless: the point of a sweep
# is that it ran, against this code, now.
.PHONY: sim
sim: ## Run the deterministic simulator (SIM_SEEDS=N, SIM_FLAGS=-skald.long)
	$(GO) test ./internal/simulation -run 'TestSimulation' -count=1 -v \
		-skald.seeds=$(SIM_SEEDS) $(SIM_FLAGS)

# The examples are self-contained programs; if one needs a server, export
# SKALD_ADDRESS before calling this target and it is passed through. The
# directory is absent in a checkout that predates it, which is not a build
# failure -- there is simply nothing to run.
.PHONY: examples
examples: ## Build and run every program under examples/
	@if [ ! -d examples ]; then \
		echo "examples/ is not present in this checkout; nothing to run"; \
		exit 0; \
	fi; \
	run=0; \
	for dir in examples/*/; do \
		[ -n "$$(ls $$dir*.go 2>/dev/null || true)" ] || continue; \
		run=1; \
		echo "==> $$dir"; \
		if command -v timeout >/dev/null 2>&1; then \
			timeout 120 $(GO) run "./$${dir%/}"; \
		else \
			$(GO) run "./$${dir%/}"; \
		fi; \
	done; \
	if [ $$run -eq 0 ]; then echo "examples/ holds no runnable programs"; fi

# ---------------------------------------------------------------------------
# Static analysis
# ---------------------------------------------------------------------------

$(GOLANGCI):
	@mkdir -p $(BIN)
	GOBIN="$(CURDIR)/$(BIN)" $(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_VERSION)

.PHONY: lint
lint: $(GOLANGCI) ## Run golangci-lint (fetches the pinned version into ./bin)
	$(GOLANGCI) run

.PHONY: fmt
fmt: ## Rewrite the tree with gofmt -s
	gofmt -s -w $$(find . -name '*.go' -not -path './.git/*')

.PHONY: fmt-check
fmt-check: ## Fail if anything is not gofmt -s clean
	@unformatted=$$(gofmt -s -l $$(find . -name '*.go' -not -path './.git/*')); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt -s clean:"; echo "$$unformatted"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKGS)

.PHONY: tidy
tidy: ## Tidy the module graph and verify the module cache
	$(GO) mod tidy
	$(GO) mod verify

# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------

.PHONY: docker
docker: ## Build the container image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		--tag $(IMAGE):$(IMAGE_TAG) \
		--tag $(IMAGE):latest \
		.

.PHONY: clean
clean: ## Remove build, coverage and release output
	rm -rf $(BIN) $(DIST) $(COVERPROFILE) $(COVERHTML)
	$(GO) clean -testcache

# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

.PHONY: ci
ci: fmt-check vet lint test-race build ## Everything CI gates on, in CI's order
	@echo "ci: ok"
