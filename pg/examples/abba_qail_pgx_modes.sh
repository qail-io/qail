#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ROUNDS="${ROUNDS:-4}"
if ! [[ "${ROUNDS}" =~ ^[0-9]+$ ]] || [[ "${ROUNDS}" -lt 1 ]]; then
  echo "ROUNDS must be a positive integer" >&2
  exit 1
fi

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

run_qail_once() {
  local mode="$1"
  "${REPO_ROOT}/target/release/examples/qail_pgx_modes_once" "${mode}" --plain
}

run_pgx_once() {
  local mode="$1"
  /tmp/pgx_modes_once -mode "${mode}" -plain
}

echo "🏁 QAIL vs PGX ABBA (single/pipeline/pool10)"
echo "=============================================="
echo "rounds=${ROUNDS} (order pattern repeats ABBA)"
echo

echo "Building QAIL runner..."
cargo build --release -p qail-pg --example qail_pgx_modes_once >/dev/null

echo "Building PGX runner..."
(
  cd "${REPO_ROOT}/pg/examples"
  GOCACHE=/tmp/go-build-cache GOFLAGS=-mod=readonly go build -o /tmp/pgx_modes_once ./pgx_benchmark.go
)

echo
for mode in single pipeline pool10; do
  case "${mode}" in
    single) label="Mode 1: multi single-query (1 conn, prepared)" ;;
    pipeline) label="Mode 2: pipelined batch (1 conn, prepared pipeline)" ;;
    pool10) label="Mode 3: pooling (10 open conns, per-conn prepared singles)" ;;
    *) label="${mode}" ;;
  esac

  echo "${label}"

  qail_runs=()
  pgx_runs=()

  for ((i = 0; i < ROUNDS; i++)); do
    idx=$((i % 4))
    if [[ "${idx}" -eq 0 || "${idx}" -eq 3 ]]; then
      first="pgx"
      second="qail"
    else
      first="qail"
      second="pgx"
    fi

    echo "  Round $((i + 1)) (${first} -> ${second})"

    if [[ "${first}" == "pgx" ]]; then
      pgx_qps="$(run_pgx_once "${mode}")"
      qail_qps="$(run_qail_once "${mode}")"
    else
      qail_qps="$(run_qail_once "${mode}")"
      pgx_qps="$(run_pgx_once "${mode}")"
    fi

    qail_runs+=("${qail_qps}")
    pgx_runs+=("${pgx_qps}")

    printf "    pgx : %8.0f q/s\n" "${pgx_qps}"
    printf "    qail: %8.0f q/s\n" "${qail_qps}"
  done

  pgx_median="$(calc_median "${pgx_runs[@]}")"
  pgx_p95="$(calc_percentile 0.95 "${pgx_runs[@]}")"
  qail_median="$(calc_median "${qail_runs[@]}")"
  qail_p95="$(calc_percentile 0.95 "${qail_runs[@]}")"
  delta="$(awk -v q="${qail_median}" -v p="${pgx_median}" 'BEGIN { printf "%.2f", ((q / p) - 1) * 100 }')"

  printf "  pgx  median/p95: %8.0f / %8.0f q/s\n" "${pgx_median}" "${pgx_p95}"
  printf "  qail median/p95: %8.0f / %8.0f q/s\n" "${qail_median}" "${qail_p95}"
  printf "  delta (qail vs pgx, median): %+0.2f%%\n" "${delta}"
  echo
done

