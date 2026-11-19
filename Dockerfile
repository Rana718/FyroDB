# Build stage
FROM rust:1.99.1-alpine3.24 AS builder

ENV RUSTFLAGS="-C link-self-contained=no"

# Pull only the musl security rebuild from the tagged Alpine repository.
RUN apk --no-cache upgrade \
    && echo '@muslfix https://dl-cdn.alpinelinux.org/alpine/v3.25/main' >> /etc/apk/repositories \
    && apk add --no-cache 'musl@muslfix>=1.2.6-r6' 'musl-dev@muslfix>=1.2.6-r6' 'musl-utils@muslfix>=1.2.6-r6' \
        llvm-libunwind-static make pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

ARG TARGETARCH
ARG TARGET_CPU=x86-64-v3
ARG FEATURES=""

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY benches ./benches
RUN mkdir src && echo "fn main() {}" > src/main.rs && touch src/lib.rs
RUN if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" -a "$(uname -m)" = "x86_64" ]; then \
        export RUSTFLAGS="$RUSTFLAGS -C target-cpu=${TARGET_CPU}"; \
    fi; \
    cargo build --locked --release ${FEATURES:+--features $FEATURES}
RUN rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs && \
    if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" -a "$(uname -m)" = "x86_64" ]; then \
        export RUSTFLAGS="$RUSTFLAGS -C target-cpu=${TARGET_CPU}"; \
    fi; \
    cargo build --locked --release ${FEATURES:+--features $FEATURES}

# Runtime stage
FROM alpine:3.24

RUN apk --no-cache upgrade \
    && echo '@muslfix https://dl-cdn.alpinelinux.org/alpine/v3.25/main' >> /etc/apk/repositories \
    && apk add --no-cache 'musl@muslfix>=1.2.6-r6' 'musl-utils@muslfix>=1.2.6-r6' \
        ca-certificates netcat-openbsd redis libssl3 libcrypto3 \
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

# mimalloc decommits unused pages with a 3,000ms delay to prevent
# MADV_DONTNEED page-fault storms on high-throughput bursts (Gregg Ch. 7).
ENV MIMALLOC_PURGE_DELAY=3000
ENV FYRODB_MALLOC_PURGE_DELAY_MS=3000

EXPOSE 8000

USER fyrodb

CMD ["fyro_db"]
