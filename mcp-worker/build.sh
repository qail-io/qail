#!/usr/bin/env bash
# Build the qail-wasm artifact the Worker imports.
#
# Output lands in src/wasm/ which is gitignored — the binary is built in CI and
# never committed. Run this before `wrangler dev` or `wrangler deploy`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/mcp-worker/src/wasm"
WASM="$ROOT/target/wasm32-unknown-unknown/wasm-release/qail_wasm.wasm"

for tool in wasm-bindgen wasm-opt; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "error: $tool not found on PATH" >&2
        case "$tool" in
            wasm-bindgen) echo "  cargo install wasm-bindgen-cli" >&2 ;;
            wasm-opt) echo "  brew install binaryen  # or your package manager" >&2 ;;
        esac
        exit 1
    fi
done

echo "==> cargo build (wasm-release)"
cargo build --manifest-path "$ROOT/Cargo.toml" \
    -p qail-wasm --target wasm32-unknown-unknown --profile wasm-release

[ -f "$WASM" ] || { echo "error: expected artifact missing: $WASM" >&2; exit 1; }

# --target web, not bundler: Cloudflare Workers resolves a `.wasm` import to a
# WebAssembly.Module rather than instantiated exports, so the module must be
# instantiated explicitly via initSync. The bundler target assumes webpack-style
# WASM ESM integration and fails at runtime with
# "wasm.__wbindgen_add_to_stack_pointer is not a function".
echo "==> wasm-bindgen"
rm -rf "$OUT" && mkdir -p "$OUT"
wasm-bindgen "$WASM" --out-dir "$OUT" --target web --no-typescript

# -Oz needs bulk memory explicitly: current rustc emits memory.copy, which the
# default binaryen feature set rejects as invalid.
echo "==> wasm-opt -Oz"
# Write to a temp file rather than in place; binaryen reading and writing the
# same path is not guaranteed safe.
wasm-opt -Oz \
    --enable-bulk-memory-opt \
    --enable-nontrapping-float-to-int \
    --enable-sign-ext \
    "$OUT/qail_wasm_bg.wasm" -o "$OUT/qail_wasm_bg.opt.wasm"
mv "$OUT/qail_wasm_bg.opt.wasm" "$OUT/qail_wasm_bg.wasm"

for f in qail_wasm.js qail_wasm_bg.wasm; do
    [ -f "$OUT/$f" ] || { echo "error: wasm-bindgen did not emit $f" >&2; exit 1; }
done

raw=$(wc -c < "$OUT/qail_wasm_bg.wasm")
gz=$(gzip -c "$OUT/qail_wasm_bg.wasm" | wc -c)
printf '==> ok: %d KB raw, %d KB gzipped\n' "$((raw / 1024))" "$((gz / 1024))"
