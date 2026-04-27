#!/usr/bin/env bash
set -euo pipefail

echo "[panic-safety] workspace runtime libs/bins"
cargo clippy --workspace --all-features --lib --bins --no-deps -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::panic \
  -D clippy::redundant_clone \
  -D clippy::clone_on_copy \
  -D clippy::clone_on_ref_ptr \
  -D clippy::undocumented_unsafe_blocks \
  -D unsafe_op_in_unsafe_fn

echo "[panic-safety] clone hygiene across tests/examples"
cargo clippy --workspace --all-features --all-targets --no-deps -- \
  -D clippy::redundant_clone \
  -D clippy::clone_on_copy \
  -D clippy::clone_on_ref_ptr

echo "[panic-safety] passed"
