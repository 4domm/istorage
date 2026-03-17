FROM rust:1.90-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    clang \
    cmake \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY api-service ./api-service
COPY metadata ./metadata
COPY chunker ./chunker
COPY config.docker.yaml ./config.docker.yaml

RUN cargo build --release --locked -p api-service -p metadata -p chunker

FROM debian:bookworm-slim AS runtime-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

FROM runtime-base AS api-service
COPY --from=builder /app/target/release/api-service /usr/local/bin/api-service
COPY --from=builder /app/config.docker.yaml /app/config.docker.yaml
ENV CONFIG_PATH=/app/config.docker.yaml
EXPOSE 3000
CMD ["api-service"]

FROM runtime-base AS metadata-service
COPY --from=builder /app/target/release/metadata /usr/local/bin/metadata
COPY --from=builder /app/metadata/migrations /app/migrations
EXPOSE 3001
CMD ["metadata"]

FROM runtime-base AS chunker-service
COPY --from=builder /app/target/release/chunker /usr/local/bin/chunker
RUN mkdir -p /data/chunks
ENV CHUNK_DB_PATH=/data/chunks
EXPOSE 3002
CMD ["chunker"]
