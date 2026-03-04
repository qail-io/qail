#!/usr/bin/env bash
set -euo pipefail

echo "[panic-safety] qail-gateway (runtime lib)"
cargo clippy -p qail-gateway --lib --no-deps -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used

echo "[panic-safety] qail-pg (runtime lib)"
cargo clippy -p qail-pg --lib --no-deps -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used

echo "[panic-safety] qail-pg (runtime lib, enterprise-gssapi)"
cargo clippy -p qail-pg --lib --no-deps --features enterprise-gssapi -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used

echo "[panic-safety] passed"
