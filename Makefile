.PHONY: help build build-release clippy clippy-lib clippy-strict test test-e2e fmt fmt-check clean docker-build docker-push docker-buildx docker-run bench bench-baseline bench-compare bench-profile bench-heap all check

# Binary and Docker configuration
BINARY_NAME := hatrack
DOCKER_IMAGE_REPO ?= quay.io/saswatamcode/hatrack
DOCKER_IMAGE_TAG ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))-$(shell date +%Y-%m-%d)-$(shell git rev-parse --short HEAD)
IMG ?= ${DOCKER_IMAGE_REPO}:${DOCKER_IMAGE_TAG}
IMG_MAIN ?= ${DOCKER_IMAGE_REPO}:main
DOCKER_IMAGE := hatrack
DOCKER_TAG := latest

# Build variables for embedding in binary
VERSION ?= $(shell cat VERSION)
REVISION ?= $(shell git rev-parse HEAD)
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
BUILDUSER ?= $(shell whoami)@$(shell hostname)
BUILDDATE ?= $(shell date +%Y%m%d-%H:%M:%S)

# Container tool (docker or podman)
CONTAINER_TOOL ?= docker

# Multi-platform build support
PLATFORMS ?= linux/arm64,linux/amd64

help:
	@echo "Available targets:"
	@echo "  build          - Build debug binary"
	@echo "  build-release  - Build optimized release binary"
	@echo "  clippy         - Run clippy linter on all targets"
	@echo "  clippy-lib     - Run clippy on library only"
	@echo "  clippy-strict  - Run clippy with warnings as errors"
	@echo "  test           - Run tests"
	@echo "  test-e2e       - Build image and run e2e tests (requires Docker)"
	@echo "  fmt            - Format code using rustfmt"
	@echo "  fmt-check      - Check code formatting without modifying files"
	@echo "  bench          - Run all criterion benchmarks"
	@echo "  bench-baseline - Save current performance as baseline"
	@echo "  bench-compare  - Compare against saved baseline"
	@echo "  bench-profile  - Run benchmarks with CPU profiling"
	@echo "  check          - Run clippy, tests, and build (CI-style)"
	@echo "  clean          - Clean build artifacts"
	@echo "  docker-build   - Build Docker image (local, tagged as latest)"
	@echo "  docker-push    - Push Docker image to registry"
	@echo "  docker-buildx  - Build and push multi-platform image"
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
	cargo test --lib --bins

DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)

test-e2e: docker-build
	$(CONTAINER_TOOL) tag $(DOCKER_IMAGE):$(DOCKER_TAG) hatrack:e2e-test
	DOCKER_HOST=$(DOCKER_HOST) cargo test --test e2e -- --nocapture

fmt:
	cargo fmt

fmt-check:
	cargo fmt -- --check

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
	@echo "Building Docker image with version information..."
	@echo "  VERSION:   $(VERSION)"
	@echo "  REVISION:  $(REVISION)"
	@echo "  BRANCH:    $(BRANCH)"
	@echo "  BUILDUSER: $(BUILDUSER)"
	@echo "  BUILDDATE: $(BUILDDATE)"
	$(CONTAINER_TOOL) build \
		--build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(REVISION) \
		--build-arg BRANCH=$(BRANCH) \
		--build-arg BUILDUSER=$(BUILDUSER) \
		--build-arg BUILDDATE=$(BUILDDATE) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t ${IMG} .

docker-push: ## Push docker image to registry.
	$(CONTAINER_TOOL) push ${IMG}

docker-buildx: ## Build and push multi-platform image for cross-platform support
	@echo "Building and pushing multi-platform Docker image..."
	@echo "  VERSION:   $(VERSION)"
	@echo "  REVISION:  $(REVISION)"
	@echo "  BRANCH:    $(BRANCH)"
	@echo "  PLATFORMS: $(PLATFORMS)"
	# Copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name hatrack-builder
	$(CONTAINER_TOOL) buildx use hatrack-builder
	$(CONTAINER_TOOL) buildx build --push \
		--platform=$(PLATFORMS) \
		--build-arg VERSION=$(VERSION) \
		--build-arg REVISION=$(REVISION) \
		--build-arg BRANCH=$(BRANCH) \
		--build-arg BUILDUSER=$(BUILDUSER) \
		--build-arg BUILDDATE=$(BUILDDATE) \
		--tag ${IMG} --tag ${IMG_MAIN} \
		-f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm hatrack-builder
	rm Dockerfile.cross

docker-run:
	docker run --rm -p 8080:8080 -p 8081:8081 $(DOCKER_IMAGE):$(DOCKER_TAG)

all: clippy build-release docker-build
