#!/usr/bin/env bash
set -euo pipefail

if ! command -v zig >/dev/null 2>&1; then
  echo "zig is required for Linux cross-check fallback (install with: brew install zig)" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLCHAIN_DIR="${TMPDIR:-/tmp}/qail-zig-cross-toolchain"
LOCAL_CACHE_DIR="${TMPDIR:-/tmp}/qail-zig-local-cache"
GLOBAL_CACHE_DIR="${TMPDIR:-/tmp}/qail-zig-global-cache"

mkdir -p "${TOOLCHAIN_DIR}" "${LOCAL_CACHE_DIR}" "${GLOBAL_CACHE_DIR}"

cat > "${TOOLCHAIN_DIR}/zig-cc-linux-gnu" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args=()
for arg in "$@"; do
  if [[ "${arg}" == "--target=x86_64-unknown-linux-gnu" ]]; then
    args+=("--target=x86_64-linux-gnu")
  else
    args+=("${arg}")
  fi
done
exec zig cc "${args[@]}"
EOF

cat > "${TOOLCHAIN_DIR}/zig-cxx-linux-gnu" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args=()
for arg in "$@"; do
  if [[ "${arg}" == "--target=x86_64-unknown-linux-gnu" ]]; then
    args+=("--target=x86_64-linux-gnu")
  else
    args+=("${arg}")
  fi
done
exec zig c++ "${args[@]}"
EOF

chmod +x "${TOOLCHAIN_DIR}/zig-cc-linux-gnu" "${TOOLCHAIN_DIR}/zig-cxx-linux-gnu"

cd "${ROOT_DIR}"
ZIG_LOCAL_CACHE_DIR="${LOCAL_CACHE_DIR}" \
ZIG_GLOBAL_CACHE_DIR="${GLOBAL_CACHE_DIR}" \
CC_x86_64_unknown_linux_gnu="${TOOLCHAIN_DIR}/zig-cc-linux-gnu" \
CXX_x86_64_unknown_linux_gnu="${TOOLCHAIN_DIR}/zig-cxx-linux-gnu" \
  cargo check -p qail-pg --features io_uring --target x86_64-unknown-linux-gnu
