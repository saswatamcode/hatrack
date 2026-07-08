.PHONY: help build build-release clippy clippy-lib clippy-strict test clean docker-build docker-run bench bench-baseline bench-compare bench-profile bench-heap all check

BINARY_NAME := hatrack
DOCKER_IMAGE := hatrack
DOCKER_TAG := latest

help:
	@echo "Available targets:"
	@echo "  build          - Build debug binary"
	@echo "  build-release  - Build optimized release binary"
	@echo "  clippy         - Run clippy linter on all targets"
	@echo "  clippy-lib     - Run clippy on library only"
	@echo "  clippy-strict  - Run clippy with warnings as errors"
	@echo "  test           - Run tests"
	@echo "  bench          - Run all criterion benchmarks"
	@echo "  bench-baseline - Save current performance as baseline"
	@echo "  bench-compare  - Compare against saved baseline"
	@echo "  bench-profile  - Run benchmarks with CPU profiling"
	@echo "  check          - Run clippy, tests, and build (CI-style)"
	@echo "  clean          - Clean build artifacts"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-run     - Run Docker container"
	@echo "  all            - Run clippy, build release, and build Docker image"

build:
	cargo build

build-release:
	cargo build --release --locked

clippy:
	cargo clippy --all-targets

clippy-lib:
	cargo clippy --lib

clippy-strict:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test

bench:
	cargo bench --bench replica_selector
	cargo bench --bench proxy_handler

bench-baseline:
	@echo "Saving current performance as baseline..."
	cargo bench --bench replica_selector -- --save-baseline baseline
	cargo bench --bench proxy_handler -- --save-baseline baseline

bench-compare:
	@echo "Comparing against baseline..."
	cargo bench --bench replica_selector -- --baseline baseline
	cargo bench --bench proxy_handler -- --baseline baseline

bench-profile:
	@echo "Running benchmarks with CPU profiling..."
	@echo "Note: Running in profile mode - statistical analysis is disabled during profiling"
	@echo "Run 'make bench' or 'make bench-compare' for timing statistics"
	@echo ""
	cargo bench --bench replica_selector -- --profile-time=5
	cargo bench --bench proxy_handler -- --profile-time=5

check: clippy-strict test build
	@echo "All checks passed!"

clean:
	cargo clean

docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-run:
	docker run --rm -p 8080:8080 -p 8081:8081 $(DOCKER_IMAGE):$(DOCKER_TAG)

all: clippy build-release docker-build
