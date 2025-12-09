# ─── Stage 1: Build ──────────────────────────────────────────
FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# Build only the gateway example binary in release mode
RUN cargo build --release -p qail-gateway --example serve \
    && strip /build/target/release/examples/serve

# ─── Stage 2: Runtime ────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN useradd --create-home --shell /bin/false qail
USER qail

COPY --from=builder /build/target/release/examples/serve /usr/local/bin/qail-gateway

# Default env (override at runtime)
ENV BIND_ADDRESS=0.0.0.0:8080
ENV RUST_LOG=qail_gateway=info,tower_http=info

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

ENTRYPOINT ["qail-gateway"]
