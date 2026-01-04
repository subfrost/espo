# Espo - Alkanes/Bitcoin Block Explorer with SSR UI
# Built from the subfrost espo fork

FROM rust:1.86-bookworm as builder

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    libssl-dev \
    clang \
    librocksdb-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy source code
COPY . .

# Build the espo binary in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    librocksdb7.8 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/espo /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && useradd -r -s /bin/false espo \
    && mkdir -p /data/espo /data/metashrew /data/bitcoin \
    && chown -R espo:espo /data

USER espo
VOLUME /data

# JSON-RPC port and Explorer port
EXPOSE 5778 8081

ENV PORT=5778 \
    BLOCK_SOURCE_MODE=rpc

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
