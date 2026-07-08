# syntax=docker/dockerfile:1.7
#
# Skald's container image.
#
# Two stages and a scratch-shaped runtime. The build stage is a full Go
# toolchain with two cache mounts, so a rebuild after a one-line change reuses
# the module download and the compiler's object cache and takes seconds. The
# runtime stage is distroless/static: no shell, no package manager, no libc, no
# setuid binaries -- a filesystem containing the two things this process needs
# and nothing an attacker who reaches RCE could pivot through.
#
#   docker build -t ghcr.io/skald-io/skald:dev .
#   docker run --rm -p 7233:7233 ghcr.io/skald-io/skald:dev
#
# BuildKit is required (the cache mounts and the heredocs below). It is the
# default in every Docker release since 23.0.

ARG GO_VERSION=1.23

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS builder

# Injected by `make docker` and by the release workflow. They are ARGs rather
# than a RUN that shells out to git because the build context deliberately does
# not contain .git -- see .dockerignore.
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG TARGETOS
ARG TARGETARCH

ENV CGO_ENABLED=0

WORKDIR /src

# go.mod and go.sum first, on their own layer. Dependencies change far less
# often than source does, so this layer survives almost every rebuild.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# -trimpath keeps /src out of the panic traces. -s -w drop the symbol table and
# DWARF, which nothing in production reads; a panic still carries file, line and
# function, because those live in the pclntab and are not stripped.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.buildDate=${BUILD_DATE}" \
      -o /out/skaldd ./cmd/skaldd && \
    GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/skald-io/skald/cmd/skaldctl/commands.version=${VERSION} \
        -X github.com/skald-io/skald/cmd/skaldctl/commands.commit=${COMMIT} \
        -X github.com/skald-io/skald/cmd/skaldctl/commands.buildDate=${BUILD_DATE}" \
      -o /out/skaldctl ./cmd/skaldctl

# The health probe.
#
# distroless/static has no shell and no curl, so HEALTHCHECK needs a binary that
# can be exec'd directly. This is that binary: 700 kB of static Go that does one
# GET and exits 0 or 1. It lives here rather than in the module because it is a
# property of the image, not of Skald -- nothing outside a container runtime
# ever runs it, and a package under ./cmd for it would show up in every
# `go build ./...` and every coverage report for no reason.
#
# /health is deliberately exempt from bearer authentication (see docs/operations.md),
# so the probe needs no credentials.
COPY <<'EOF' /probe/go.mod
module probe

go 1.23
EOF
COPY <<'EOF' /probe/main.go
package main

import (
	"net/http"
	"os"
	"time"
)

func main() {
	url := os.Getenv("SKALD_HEALTH_URL")
	if url == "" {
		url = "http://127.0.0.1:7233/health"
	}
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
EOF
RUN --mount=type=cache,target=/root/.cache/go-build \
    cd /probe && GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags '-s -w' -o /out/healthcheck .

# The data directory is created here so that the runtime stage can COPY it with
# the right ownership. Docker initialises an empty named volume from the image's
# contents at that path, ownership included; without this, the volume arrives
# owned by root and a nonroot process cannot create skald.db in it.
RUN mkdir -p /var/lib/skald

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

# https://github.com/opencontainers/image-spec/blob/main/annotations.md
LABEL org.opencontainers.image.title="skald" \
      org.opencontainers.image.description="A durable workflow execution engine for Go." \
      org.opencontainers.image.source="https://github.com/skald-io/skald" \
      org.opencontainers.image.documentation="https://github.com/skald-io/skald/blob/main/docs/operations.md" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="skald-io" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.base.name="gcr.io/distroless/static:nonroot"

COPY --from=builder /out/skaldd      /usr/local/bin/skaldd
COPY --from=builder /out/skaldctl    /usr/local/bin/skaldctl
COPY --from=builder /out/healthcheck /usr/local/bin/healthcheck
COPY --from=builder --chown=nonroot:nonroot /var/lib/skald /var/lib/skald

# 65532 is distroless's `nonroot`. Named rather than numeric so that a reader
# knows which it is; Kubernetes admission controllers that require
# runAsNonRoot resolve it from the image config either way.
USER nonroot:nonroot

WORKDIR /var/lib/skald

# The image binds every interface, unlike the binary's own default of
# 127.0.0.1. A loopback default is right for a process on a host, where an
# unauthenticated engine reachable from the network is an accident; inside a
# container it would mean the port publishes nothing at all. Set
# SKALD_AUTH_TOKEN before exposing this beyond a compose network.
ENV SKALD_ADDR=0.0.0.0:7233 \
    SKALD_STORE=sqlite \
    SKALD_SQLITE_PATH=/var/lib/skald/skald.db \
    SKALD_LOG_FORMAT=json

EXPOSE 7233

# 40s of start period covers the schema migration on a cold volume. Three
# missed probes at 10s is 30s of unreachable before the orchestrator acts,
# which is comfortably longer than a WAL checkpoint and shorter than any
# client's retry budget.
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=3 \
  CMD ["/usr/local/bin/healthcheck"]

ENTRYPOINT ["/usr/local/bin/skaldd"]
