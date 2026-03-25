FROM foundationdb/foundationdb:7.3.69 AS fdb-dist

FROM golang:1.25-bookworm AS go-builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fdb-dist /usr/include/foundationdb /usr/include/foundationdb
COPY --from=fdb-dist /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so

COPY config.docker.yaml ./config.docker.yaml
COPY metadata ./metadata
COPY api ./api
COPY chunk ./chunk

WORKDIR /app/metadata
ENV CGO_ENABLED=1
RUN go build -o /out/metadata ./cmd/metadata

WORKDIR /app/api
ENV CGO_ENABLED=0
RUN go build -o /out/api-service ./cmd/api-service

WORKDIR /app/chunk
ENV CGO_ENABLED=0
RUN go build -o /out/chunker ./cmd/chunker

FROM debian:bookworm-slim AS runtime-base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    libstdc++6 \
    libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

FROM runtime-base AS api-service
COPY --from=go-builder /out/api-service /usr/local/bin/api-service
COPY --from=go-builder /app/config.docker.yaml /app/config.docker.yaml
ENV CONFIG_PATH=/app/config.docker.yaml
EXPOSE 3000
CMD ["api-service"]

FROM runtime-base AS metadata-service
COPY --from=fdb-dist /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
COPY --from=go-builder /out/metadata /usr/local/bin/metadata
EXPOSE 3001
CMD ["metadata"]

FROM runtime-base AS chunker-service
COPY --from=go-builder /out/chunker /usr/local/bin/chunker
COPY --from=go-builder /app/config.docker.yaml /app/config.docker.yaml
RUN mkdir -p /data/chunks
ENV CHUNK_DB_PATH=/data/chunks
ENV CONFIG_PATH=/app/config.docker.yaml
EXPOSE 3002
CMD ["chunker"]
