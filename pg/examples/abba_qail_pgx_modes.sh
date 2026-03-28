#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ROUNDS="${ROUNDS:-6}"
if ! [[ "${ROUNDS}" =~ ^[0-9]+$ ]] || [[ "${ROUNDS}" -lt 1 ]]; then
  echo "ROUNDS must be a positive integer" >&2
  exit 1
fi

WORKLOADS="${WORKLOADS:-point wide_rows many_params}"
MODES="${MODES:-single pipeline pool10}"

ZIG_REPO_ROOT="${ZIG_REPO_ROOT:-$(cd "${REPO_ROOT}/.." && pwd)/qail-zig}"
if [[ ! -d "${ZIG_REPO_ROOT}" ]]; then
  echo "ZIG_REPO_ROOT not found: ${ZIG_REPO_ROOT}" >&2
  exit 1
fi

ZIG_BIN="${ZIG_BIN:-/tmp/qail_zig_modes_once}"
ZIG_CACHE_DIR="${ZIG_CACHE_DIR:-/tmp/qail-zig-bench-cache}"
case "$(uname -s):$(uname -m)" in
  Darwin:arm64) DEFAULT_ZIG_TARGET="aarch64-macos.15.0" ;;
  Darwin:x86_64) DEFAULT_ZIG_TARGET="x86_64-macos.15.0" ;;
  *) DEFAULT_ZIG_TARGET="" ;;
esac
ZIG_TARGET="${ZIG_TARGET:-${DEFAULT_ZIG_TARGET}}"

calc_median() {
  printf '%s\n' "$@" | LC_ALL=C sort -n | awk '
    {a[NR]=$1}
    END {
      if (NR == 0) { printf "0"; exit }
      mid = int((NR + 1) / 2)
      if (NR % 2 == 1) {
        printf "%.6f", a[mid]
      } else {
        printf "%.6f", (a[mid] + a[mid + 1]) / 2
      }
    }
  '
}

calc_percentile() {
  local pct="$1"
  shift
  printf '%s\n' "$@" | LC_ALL=C sort -n | awk -v p="${pct}" '
    {a[NR]=$1}
    END {
      if (NR == 0) { printf "0"; exit }
      rank = int((p * NR) + 0.999999999)
      if (rank < 1) rank = 1
      if (rank > NR) rank = NR
      printf "%.6f", a[rank]
    }
  '
}

calc_delta() {
  local lhs="$1"
  local rhs="$2"
  awk -v l="${lhs}" -v r="${rhs}" '
    BEGIN {
      if (r == 0) {
        print "nan"
        exit
      }
      printf "%.2f", ((l / r) - 1) * 100
    }
  '
}

run_qail_rust_once() {
  local mode="$1"
  local workload="$2"
  "${REPO_ROOT}/target/release/examples/qail_pgx_modes_once" "${mode}" --workload "${workload}" --plain
}

run_pgx_once() {
  local mode="$1"
  local workload="$2"
  /tmp/pgx_modes_once -mode "${mode}" -workload "${workload}" -plain
}

run_qail_zig_once() {
  local mode="$1"
  local workload="$2"
  "${ZIG_BIN}" "${mode}" --workload "${workload}" --plain
}

run_once() {
  local runner="$1"
  local mode="$2"
  local workload="$3"
  case "${runner}" in
    pgx) run_pgx_once "${mode}" "${workload}" ;;
    qail_rs) run_qail_rust_once "${mode}" "${workload}" ;;
    qail_zig) run_qail_zig_once "${mode}" "${workload}" ;;
    *)
      echo "unknown runner: ${runner}" >&2
      return 1
      ;;
  esac
}

echo "🏁 PGX vs QAIL (Go/Rust/Zig)"
echo "============================"
echo "rounds=${ROUNDS} (order rotates: pgx -> qail-rs -> qail-zig)"
echo "modes=${MODES}"
echo "workloads=${WORKLOADS}"
echo

echo "Building QAIL Rust runner..."
(
  cd "${REPO_ROOT}"
  cargo build --release -p qail-pg --example qail_pgx_modes_once >/dev/null
)

echo "Building PGX runner..."
(
  cd "${REPO_ROOT}/pg/examples"
  GOCACHE=/tmp/go-build-cache GOFLAGS=-mod=readonly go build -o /tmp/pgx_modes_once ./pgx_benchmark.go
)

echo "Building QAIL Zig runner..."
(
  cd "${ZIG_REPO_ROOT}"
  zig_cmd=(
    zig build-exe
    src/qail_pgx_modes_once.zig
    -O ReleaseFast
    --cache-dir
    "${ZIG_CACHE_DIR}"
    "-femit-bin=${ZIG_BIN}"
  )
  if [[ -n "${ZIG_TARGET}" ]]; then
    zig_cmd+=(-target "${ZIG_TARGET}")
  fi
  "${zig_cmd[@]}" >/dev/null
)

echo
for workload in ${WORKLOADS}; do
  case "${workload}" in
    point) workload_label="Workload: point lookup (1 row, prepared)" ;;
    wide_rows) workload_label="Workload: wide rows (128-512 rows/query, mixed types)" ;;
    many_params) workload_label="Workload: many params (32 binds/query, scalar result)" ;;
    *) workload_label="Workload: ${workload}" ;;
  esac
  echo "${workload_label}"

  for mode in ${MODES}; do
    case "${mode}" in
      single) label="  Mode 1: multi single-query (1 conn, prepared)" ;;
      pipeline) label="  Mode 2: pipelined batch (1 conn, prepared pipeline)" ;;
      pool10) label="  Mode 3: pooling (10 open conns, per-conn prepared singles)" ;;
      *) label="  ${mode}" ;;
    esac

    echo "${label}"

    qail_rs_runs=()
    qail_zig_runs=()
    pgx_runs=()

    for ((i = 0; i < ROUNDS; i++)); do
      case $((i % 3)) in
        0) order=(pgx qail_rs qail_zig) ;;
        1) order=(qail_rs qail_zig pgx) ;;
        2) order=(qail_zig pgx qail_rs) ;;
      esac

      echo "    Round $((i + 1)) (${order[0]} -> ${order[1]} -> ${order[2]})"

      unset pgx_qps qail_rs_qps qail_zig_qps
      for runner in "${order[@]}"; do
        qps="$(run_once "${runner}" "${mode}" "${workload}")"
        case "${runner}" in
          pgx)
            pgx_qps="${qps}"
            pgx_runs+=("${qps}")
            ;;
          qail_rs)
            qail_rs_qps="${qps}"
            qail_rs_runs+=("${qps}")
            ;;
          qail_zig)
            qail_zig_qps="${qps}"
            qail_zig_runs+=("${qps}")
            ;;
        esac
      done

      printf "      pgx      : %8.0f q/s\n" "${pgx_qps}"
      printf "      qail-rs  : %8.0f q/s\n" "${qail_rs_qps}"
      printf "      qail-zig : %8.0f q/s\n" "${qail_zig_qps}"
    done

    pgx_median="$(calc_median "${pgx_runs[@]}")"
    pgx_p95="$(calc_percentile 0.95 "${pgx_runs[@]}")"
    qail_rs_median="$(calc_median "${qail_rs_runs[@]}")"
    qail_rs_p95="$(calc_percentile 0.95 "${qail_rs_runs[@]}")"
    qail_zig_median="$(calc_median "${qail_zig_runs[@]}")"
    qail_zig_p95="$(calc_percentile 0.95 "${qail_zig_runs[@]}")"
    delta_rs_vs_pgx="$(calc_delta "${qail_rs_median}" "${pgx_median}")"
    delta_zig_vs_pgx="$(calc_delta "${qail_zig_median}" "${pgx_median}")"
    delta_zig_vs_rs="$(calc_delta "${qail_zig_median}" "${qail_rs_median}")"

    printf "    pgx       median/p95: %8.0f / %8.0f q/s\n" "${pgx_median}" "${pgx_p95}"
    printf "    qail-rs   median/p95: %8.0f / %8.0f q/s\n" "${qail_rs_median}" "${qail_rs_p95}"
    printf "    qail-zig  median/p95: %8.0f / %8.0f q/s\n" "${qail_zig_median}" "${qail_zig_p95}"
    printf "    delta (qail-rs vs pgx, median): %+0.2f%%\n" "${delta_rs_vs_pgx}"
    printf "    delta (qail-zig vs pgx, median): %+0.2f%%\n" "${delta_zig_vs_pgx}"
    printf "    delta (qail-zig vs qail-rs, median): %+0.2f%%\n" "${delta_zig_vs_rs}"
    echo
  done
done
