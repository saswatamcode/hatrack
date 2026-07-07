.PHONY: help build build-release clippy test clean docker-build docker-run all

BINARY_NAME := hatrack
DOCKER_IMAGE := hatrack
DOCKER_TAG := latest

help:
	@echo "Available targets:"
	@echo "  build         - Build debug binary"
	@echo "  build-release - Build optimized release binary"
	@echo "  clippy        - Run clippy linter"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-run    - Run Docker container"
	@echo "  all           - Run clippy, build release, and build Docker image"

build:
	cargo build

build-release:
	cargo build --release --locked

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test

clean:
	cargo clean

docker-build:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-run:
	docker run --rm -p 8080:8080 -p 8081:8081 $(DOCKER_IMAGE):$(DOCKER_TAG)

all: clippy build-release docker-build
