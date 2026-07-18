# syntax=docker/dockerfile:1

# --- Builder stage -----------------------------------------------------------
# Compiles a fully static Go binary so it can run on a distroless base that
# ships no libc / shell.
FROM golang:1.22 AS builder

WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source tree and build.
COPY . .

# CGO_ENABLED=0 + netgo/osusergo yields a static, dependency-free binary.
RUN CGO_ENABLED=0 GOOS=linux \
    go build \
        -trimpath \
        -ldflags="-s -w" \
        -tags netgo,osusergo \
        -o /out/server \
        ./cmd/server

# --- Runtime stage -----------------------------------------------------------
# distroless/static contains only the binary's runtime prerequisites (CA certs,
# tzdata, /etc/passwd) and runs as an unprivileged user via the :nonroot tag.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

# Application binary and the config it loads at startup (see cmd/server/main.go,
# which calls config.Load("config.yaml") relative to the working directory).
COPY --from=builder /out/server ./server
COPY config.yaml ./config.yaml

EXPOSE 8080

# NOTE: No HEALTHCHECK is declared here. The distroless base has no shell and no
# curl/wget, so a Dockerfile HEALTHCHECK CMD cannot be expressed. Perform health
# checks externally instead, e.g. an orchestrator probe against:
#   GET http://<host>:8080/api/health
# (compose/k8s can run such HTTP probes without a shell inside the container).

ENTRYPOINT ["/app/server"]
