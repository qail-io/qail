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
  local workload="$1"
  "${REPO_ROOT}/target/release/examples/qail_strict_once" "${workload}" --plain
}

run_tokio_postgres_once() {
  local workload="$1"
  "${REPO_ROOT}/target/release/examples/tokio_postgres_strict_once" "${workload}" --plain
}

echo "🏁 QAIL vs tokio-postgres strict ABBA orchestrator"
echo "=================================================="
echo "rounds=${ROUNDS} (order pattern repeats ABBA)"
echo

echo "Building runners..."
cargo build --release -p qail-pg --example qail_strict_once --example tokio_postgres_strict_once >/dev/null

echo
for workload in literal param; do
  if [[ "${workload}" == "literal" ]]; then
    echo "Workload A: template-cached literal LIMIT (0 bind params)"
  else
    echo "Workload B: template-cached parameterized filter (1 bind param)"
  fi

  qail_runs=()
  tokio_runs=()

  for ((i = 0; i < ROUNDS; i++)); do
    idx=$((i % 4))
    if [[ "${idx}" -eq 0 || "${idx}" -eq 3 ]]; then
      first="tokio-postgres"
      second="qail"
    else
      first="qail"
      second="tokio-postgres"
    fi

    echo "  Round $((i + 1)) (${first} -> ${second})"

    if [[ "${first}" == "tokio-postgres" ]]; then
      tokio_qps="$(run_tokio_postgres_once "${workload}")"
      qail_qps="$(run_qail_once "${workload}")"
    else
      qail_qps="$(run_qail_once "${workload}")"
      tokio_qps="$(run_tokio_postgres_once "${workload}")"
    fi

    qail_runs+=("${qail_qps}")
    tokio_runs+=("${tokio_qps}")

    printf "    tokio-postgres: %8.0f q/s\n" "${tokio_qps}"
    printf "    qail          : %8.0f q/s\n" "${qail_qps}"
  done

  tokio_median="$(calc_median "${tokio_runs[@]}")"
  tokio_p95="$(calc_percentile 0.95 "${tokio_runs[@]}")"
  qail_median="$(calc_median "${qail_runs[@]}")"
  qail_p95="$(calc_percentile 0.95 "${qail_runs[@]}")"
  delta="$(awk -v q="${qail_median}" -v t="${tokio_median}" 'BEGIN { printf "%.2f", ((q / t) - 1) * 100 }')"

  printf "  tokio-postgres median/p95: %8.0f / %8.0f q/s\n" "${tokio_median}" "${tokio_p95}"
  printf "  qail          median/p95: %8.0f / %8.0f q/s\n" "${qail_median}" "${qail_p95}"
  printf "  delta (qail vs tokio-postgres, median): %+0.2f%%\n" "${delta}"
  echo
done

