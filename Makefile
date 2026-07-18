.PHONY: build run test test-unit test-integration test-race tidy lint clean frontend-install frontend-dev cover bench fuzz fmt fmt-check vet typecheck ci

# Coverage: race-enabled profile + per-function summary + HTML report
cover:
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage.html written"

# Benchmarks (hot paths: store, vclock, quorum, anti-entropy)
bench:
	go test -bench=. -benchmem -run=^$$ ./...

# Short native fuzz smoke run for each fuzz target
fuzz:
	go test -run=^$$ -fuzz=^FuzzVectorClockMerge$$ -fuzztime=10s ./internal/storage/
	go test -run=^$$ -fuzz=^FuzzQuorum$$ -fuzztime=10s ./internal/quorum/
	go test -run=^$$ -fuzz=^FuzzCRDTMerge$$ -fuzztime=10s ./internal/conflict/

# Formatting
fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

vet:
	go vet ./...

typecheck:
	cd frontend && bunx tsc --noEmit

# Full local CI mirror
ci: fmt-check vet test-race typecheck

# Build the Go server
build:
	go build -o bin/server ./cmd/server

# Run the Go server
run:
	go run ./cmd/server

# Run all tests
test: test-unit test-integration

# Run unit tests only
test-unit:
	go test -v ./test/unit/...

# Run integration tests only
test-integration:
	go test -v -timeout 30s ./test/integration/...

# Run tests with race detector
test-race:
	go test -race -timeout 60s ./...

# Tidy dependencies
tidy:
	go mod tidy

# Lint (requires golangci-lint)
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	rm -rf bin/

# Frontend: install dependencies
frontend-install:
	cd frontend && bun install

# Frontend: start the BFF dev server
frontend-dev:
	cd frontend && bun run server/bff.ts

# Start both backend and frontend
dev:
	@echo "Starting backend on :8080 and frontend on :3001"
	@$(MAKE) run & $(MAKE) frontend-dev

# Format Go code
fmt:
	gofmt -w .
