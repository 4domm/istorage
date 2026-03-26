FROM foundationdb/foundationdb:7.3.69 AS fdb-dist

FROM golang:1.25-bookworm AS go-builder

WORKDIR /app
ENV GOPROXY=https://proxy.golang.org,direct
ENV GOSUMDB=sum.golang.org
ARG FDB_VERSION=7.3.69

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=fdb-dist /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
RUN mkdir -p /usr/include/foundationdb \
    && curl -fsSL "https://raw.githubusercontent.com/apple/foundationdb/${FDB_VERSION}/bindings/c/foundationdb/fdb_c.h" -o /usr/include/foundationdb/fdb_c.h \
    && curl -fsSL "https://raw.githubusercontent.com/apple/foundationdb/${FDB_VERSION}/bindings/c/foundationdb/fdb.options" -o /usr/include/foundationdb/fdb.options

COPY config.docker.yaml ./config.docker.yaml
COPY metadata ./metadata
COPY api ./api
COPY chunk ./chunk

WORKDIR /app/metadata
ENV CGO_ENABLED=1
RUN for i in 1 2 3; do go build -o /out/metadata ./cmd/metadata && exit 0; sleep 2; done; exit 1

WORKDIR /app/api
ENV CGO_ENABLED=0
RUN for i in 1 2 3; do go build -o /out/api-service ./cmd/api-service && exit 0; sleep 2; done; exit 1

WORKDIR /app/chunk
ENV CGO_ENABLED=0
RUN for i in 1 2 3; do go build -o /out/chunker ./cmd/chunker && exit 0; sleep 2; done; exit 1

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
