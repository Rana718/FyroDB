# Build stage
FROM rust:1.99.1-alpine3.24 AS builder

# Pull only the musl security rebuild from the tagged Alpine repository.
RUN apk --no-cache upgrade \
    && echo '@muslfix https://dl-cdn.alpinelinux.org/alpine/v3.25/main' >> /etc/apk/repositories \
    && apk add --no-cache 'musl@muslfix>=1.2.6-r6' 'musl-dev@muslfix>=1.2.6-r6' 'musl-utils@muslfix>=1.2.6-r6' \
        llvm-libunwind-static make pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

ARG TARGET_CPU=x86-64-v3
ARG FEATURES=""
ENV CARGO_BUILD_RUSTFLAGS="-C link-self-contained=no" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-C link-self-contained=no -C target-cpu=${TARGET_CPU}"

COPY Cargo.toml Cargo.lock ./
COPY crates/customhash/Cargo.toml crates/customhash/
COPY crates/fyro-vector/Cargo.toml crates/fyro-vector/
COPY crates/rust-zmalloc/Cargo.toml crates/rust-zmalloc/
COPY crates/fyro-dbcache/Cargo.toml crates/fyro-dbcache/
COPY benches ./benches

# Cache registry dependencies separately from workspace source changes.
RUN mkdir src && touch src/lib.rs src/main.rs \
    && for crate in crates/*; do mkdir "$crate/src" && touch "$crate/src/lib.rs"; done \
    && cargo build --locked --release --lib --features "io-uring $FEATURES" \
    && cargo clean --release -p fyro_db -p customhash -p fyro-vector -p rust-zmalloc -p fyro-dbcache

COPY crates ./crates
RUN cargo build --locked --release --lib --features "io-uring $FEATURES" \
    && cargo clean --release -p fyro_db

COPY src ./src
RUN cargo build --locked --release --bin fyro_db --features "io-uring $FEATURES"

FROM alpine:3.24 AS runtime-root

RUN apk --no-cache upgrade \
    && echo '@muslfix https://dl-cdn.alpinelinux.org/alpine/v3.25/main' >> /etc/apk/repositories \
    && apk add --no-cache 'musl@muslfix>=1.2.6-r6' 'musl-utils@muslfix>=1.2.6-r6' \
        ca-certificates netcat-openbsd redis libssl3 libcrypto3 \
    && addgroup -S fyrodb && adduser -S fyrodb -G fyrodb \
    && mkdir -p /data && chown fyrodb:fyrodb /data

# Ship the patched filesystem once, without superseded Alpine package layers.
FROM scratch
COPY --from=runtime-root / /
WORKDIR /data

COPY --from=builder /app/target/release/fyro_db /usr/local/bin/fyro_db

# Default runtime configuration. Every value remains overridable with
# `docker run -e NAME=value` or Compose environment settings.
ENV FYRODB_PORT=8000
ENV FYRODB_WORKERS=0
ENV FYRODB_IO_URING=false
ENV FYRODB_SHARDS=0
ENV FYRODB_MAX_KEYS=0
ENV FYRODB_MAX_CLIENTS=10000
ENV FYRODB_RDB_PATH=/data/fyrodb.rdb
ENV FYRODB_RDB_INTERVAL=300

# mimalloc decommits unused pages with a 3,000ms delay to prevent
# MADV_DONTNEED page-fault storms on high-throughput bursts (Gregg Ch. 7).
ENV MIMALLOC_PURGE_DELAY=3000
ENV FYRODB_MALLOC_PURGE_DELAY_MS=3000

EXPOSE 8000

USER fyrodb

CMD ["fyro_db"]
