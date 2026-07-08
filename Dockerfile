FROM rust:1-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /usr/src/app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies (cached layer)
FROM chef AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG REVISION
ARG BRANCH
ARG BUILDUSER
ARG BUILDDATE

COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN cargo chef cook --profile release-docker --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --profile release-docker --locked --bin hatrack

# Runtime image
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /
COPY --from=builder /usr/src/app/target/release-docker/hatrack .
USER 65532:65532
EXPOSE 8080 8081
ENTRYPOINT ["/hatrack"]
