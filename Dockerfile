# Build stage
FROM rust:alpine AS builder

RUN apk add --no-cache musl-dev make pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

ARG TARGETARCH
ARG TARGET_CPU=x86-64-v3
ARG FEATURES=""

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY benches ./benches
RUN mkdir src && echo "fn main() {}" > src/main.rs && touch src/lib.rs
RUN if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" -a "$(uname -m)" = "x86_64" ]; then \
        export RUSTFLAGS="-C target-cpu=${TARGET_CPU}"; \
    fi; \
    cargo build --release ${FEATURES:+--features $FEATURES}
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs && \
    if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" -a "$(uname -m)" = "x86_64" ]; then \
        export RUSTFLAGS="-C target-cpu=${TARGET_CPU}"; \
    fi; \
    cargo build --release ${FEATURES:+--features $FEATURES}

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates netcat-openbsd redis libssl3 libcrypto3 \
    && addgroup -S fyrodb && adduser -S fyrodb -G fyrodb \
    && mkdir -p /data && chown fyrodb:fyrodb /data

WORKDIR /data

COPY --from=builder /app/target/release/fyro_db /usr/local/bin/fyro_db

# Default runtime configuration. Every value remains overridable with
# `docker run -e NAME=value` or Compose environment settings.
ENV FYRODB_PORT=8000
ENV FYRODB_WORKERS=0
ENV FYRODB_SHARDS=0
ENV FYRODB_MAX_KEYS=0
ENV FYRODB_MAX_CLIENTS=10000
ENV FYRODB_RDB_PATH=/data/fyrodb.rdb
ENV FYRODB_RDB_INTERVAL=300

# mimalloc eagerly decommits pages — no special runtime config needed.
# Set MIMALLOC_PURGE_DELAY=0 for immediate purge (default is 1000ms in v3).
ENV MIMALLOC_PURGE_DELAY=0

EXPOSE 8000

USER fyrodb

CMD ["fyro_db"]
