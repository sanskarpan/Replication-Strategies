.PHONY: build run test test-unit test-integration test-race tidy lint clean frontend-install frontend-dev

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
