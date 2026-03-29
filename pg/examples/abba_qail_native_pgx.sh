#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ROUNDS="${ROUNDS:-6}"
if ! [[ "${ROUNDS}" =~ ^[0-9]+$ ]] || [[ "${ROUNDS}" -lt 1 ]]; then
  echo "ROUNDS must be a positive integer" >&2
  exit 1
fi

WORKLOADS="${WORKLOADS:-point wide_rows large_rows many_params aggregate}"
MODES="${MODES:-single pipeline pool10 latency}"
RUNNERS="${RUNNERS:-pgx qail_rs}"

runner_enabled() {
  local needle="$1"
  for runner in ${RUNNERS}; do
    if [[ "${runner}" == "${needle}" ]]; then
      return 0
    fi
  done
  return 1
}

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
  "${REPO_ROOT}/target/release/examples/qail_native_pgx_once" \
    "${mode}" \
    --workload "${workload}" \
    --plain
}

run_pgx_once() {
  local mode="$1"
  local workload="$2"
  /tmp/pgx_native_once -mode "${mode}" -workload "${workload}" -plain
}

run_once() {
  local runner="$1"
  local mode="$2"
  local workload="$3"
  case "${runner}" in
    pgx) run_pgx_once "${mode}" "${workload}" ;;
    qail_rs) run_qail_rust_once "${mode}" "${workload}" ;;
    *)
      echo "unknown runner: ${runner}" >&2
      return 1
      ;;
  esac
}

print_runner_summary() {
  local label="$1"
  local array_name="$2"
  local runs=()
  if ! declare -p "${array_name}" >/dev/null 2>&1; then
    printf "    %-9s median/p95: %8s / %8s q/s\n" "${label}" "n/a" "n/a"
    return
  fi
  local run_count
  eval "run_count=\${#${array_name}[@]}"
  if [[ "${run_count}" -eq 0 ]]; then
    printf "    %-9s median/p95: %8s / %8s q/s\n" "${label}" "n/a" "n/a"
    return
  fi
  eval "runs=(\"\${${array_name}[@]}\")"

  local median p95
  median="$(calc_median "${runs[@]}")"
  p95="$(calc_percentile 0.95 "${runs[@]}")"
  printf "    %-9s median/p95: %8.0f / %8.0f q/s\n" "${label}" "${median}" "${p95}"
}

print_delta_summary() {
  local label="$1"
  local lhs="${2:-}"
  local rhs="${3:-}"
  if [[ -z "${lhs}" || -z "${rhs}" ]]; then
    printf "    %s: n/a\n" "${label}"
    return
  fi
  local delta
  delta="$(calc_delta "${lhs}" "${rhs}")"
  printf "    %s: %+0.2f%%\n" "${label}" "${delta}"
}

print_latency_summary() {
  local label="$1"
  local p50_name="$2"
  local p95_name="$3"
  local p99_name="$4"

  local p50_count p95_count p99_count
  eval "p50_count=\${#${p50_name}[@]}"
  eval "p95_count=\${#${p95_name}[@]}"
  eval "p99_count=\${#${p99_name}[@]}"
  if [[ "${p50_count}" -eq 0 || "${p95_count}" -eq 0 || "${p99_count}" -eq 0 ]]; then
    printf "    %-9s p50/p95/p99: %8s / %8s / %8s ms\n" "${label}" "n/a" "n/a" "n/a"
    return
  fi

  local p50_runs=() p95_runs=() p99_runs=()
  eval "p50_runs=(\"\${${p50_name}[@]}\")"
  eval "p95_runs=(\"\${${p95_name}[@]}\")"
  eval "p99_runs=(\"\${${p99_name}[@]}\")"

  local p50_med p95_med p99_med
  p50_med="$(calc_median "${p50_runs[@]}")"
  p95_med="$(calc_median "${p95_runs[@]}")"
  p99_med="$(calc_median "${p99_runs[@]}")"
  printf "    %-9s p50/p95/p99: %8.3f / %8.3f / %8.3f ms\n" "${label}" "${p50_med}" "${p95_med}" "${p99_med}"
}

echo "🏁 PGX SQL vs QAIL Native DSL"
echo "============================="
echo "rounds=${ROUNDS}"
echo "runners=${RUNNERS}"
echo "modes=${MODES}"
echo "workloads=${WORKLOADS}"
if [[ -n "${DATABASE_URL:-${QAIL_BENCH_DATABASE_URL:-}}" ]]; then
  echo "db_target=env-configured"
else
  echo "db_target=local defaults"
fi
echo

echo "Building QAIL native DSL runner..."
(
  cd "${REPO_ROOT}"
  cargo build --release -p qail-pg --example qail_native_pgx_once >/dev/null
)

echo "Building PGX SQL runner..."
(
  cd "${REPO_ROOT}/pg/examples"
  GOCACHE=/tmp/go-build-cache GOFLAGS=-mod=readonly go build -o /tmp/pgx_native_once ./pgx_native_benchmark.go
)

echo
for workload in ${WORKLOADS}; do
  case "${workload}" in
    point) workload_label="Workload: point lookup (1 row)" ;;
    wide_rows) workload_label="Workload: wide rows (128-512 rows/query, mixed types)" ;;
    large_rows) workload_label="Workload: large rows (10k-16k rows/query, table-backed)" ;;
    many_params) workload_label="Workload: many params (32 predicates/query, scalar result)" ;;
    aggregate) workload_label="Workload: aggregate (server-heavier grouped scalar result)" ;;
    *) workload_label="Workload: ${workload}" ;;
  esac
  echo "${workload_label}"

  for mode in ${MODES}; do
    case "${mode}" in
      single) label="  Mode 1: repeated single-query execution" ;;
      pipeline) label="  Mode 2: pipelined / batched execution" ;;
      pool10) label="  Mode 3: pooled execution (10 open conns)" ;;
      latency) label="  Mode 4: single-query latency" ;;
      *) label="  ${mode}" ;;
    esac
    echo "${label}"

    active_runners=()
    for runner in ${RUNNERS}; do
      active_runners+=("${runner}")
    done
    if [[ "${#active_runners[@]}" -eq 0 ]]; then
      echo "    no active runners for this slice"
      echo
      continue
    fi

    qail_rs_runs=()
    pgx_runs=()
    qail_rs_p50_runs=()
    qail_rs_p95_runs=()
    qail_rs_p99_runs=()
    pgx_p50_runs=()
    pgx_p95_runs=()
    pgx_p99_runs=()

    for ((i = 0; i < ROUNDS; i++)); do
      case "${#active_runners[@]}" in
        1)
          order=("${active_runners[0]}")
          ;;
        2)
          case $((i % 2)) in
            0) order=("${active_runners[0]}" "${active_runners[1]}") ;;
            1) order=("${active_runners[1]}" "${active_runners[0]}") ;;
          esac
          ;;
        *)
          echo "unsupported runner count: ${#active_runners[@]}" >&2
          exit 1
          ;;
      esac

      order_desc="$(printf '%s -> ' "${order[@]}")"
      order_desc="${order_desc% -> }"
      echo "    Round $((i + 1)) (${order_desc})"

      unset pgx_qps qail_rs_qps
      unset pgx_p50 pgx_p95 pgx_p99 qail_rs_p50 qail_rs_p95 qail_rs_p99
      for runner in "${order[@]}"; do
        output="$(run_once "${runner}" "${mode}" "${workload}")"
        if [[ "${mode}" == "latency" ]]; then
          IFS=, read -r p50 p95 p99 _avg <<<"${output}"
          case "${runner}" in
            pgx)
              pgx_p50="${p50}"
              pgx_p95="${p95}"
              pgx_p99="${p99}"
              pgx_p50_runs+=("${p50}")
              pgx_p95_runs+=("${p95}")
              pgx_p99_runs+=("${p99}")
              ;;
            qail_rs)
              qail_rs_p50="${p50}"
              qail_rs_p95="${p95}"
              qail_rs_p99="${p99}"
              qail_rs_p50_runs+=("${p50}")
              qail_rs_p95_runs+=("${p95}")
              qail_rs_p99_runs+=("${p99}")
              ;;
          esac
        else
          qps="${output}"
          case "${runner}" in
            pgx)
              pgx_qps="${qps}"
              pgx_runs+=("${qps}")
              ;;
            qail_rs)
              qail_rs_qps="${qps}"
              qail_rs_runs+=("${qps}")
              ;;
          esac
        fi
      done

      if [[ "${mode}" == "latency" ]]; then
        for runner in "${active_runners[@]}"; do
          case "${runner}" in
            pgx) printf "      pgx      : p50=%8.3f ms | p95=%8.3f ms | p99=%8.3f ms\n" "${pgx_p50}" "${pgx_p95}" "${pgx_p99}" ;;
            qail_rs) printf "      qail-rs  : p50=%8.3f ms | p95=%8.3f ms | p99=%8.3f ms\n" "${qail_rs_p50}" "${qail_rs_p95}" "${qail_rs_p99}" ;;
          esac
        done
      else
        for runner in "${active_runners[@]}"; do
          case "${runner}" in
            pgx) printf "      pgx      : %8.0f q/s\n" "${pgx_qps}" ;;
            qail_rs) printf "      qail-rs  : %8.0f q/s\n" "${qail_rs_qps}" ;;
          esac
        done
      fi
    done

    if [[ "${mode}" == "latency" ]]; then
      unset pgx_p50_median pgx_p99_median qail_rs_p50_median qail_rs_p99_median
      if [[ "${#pgx_p50_runs[@]}" -gt 0 ]]; then
        pgx_p50_median="$(calc_median "${pgx_p50_runs[@]}")"
        pgx_p99_median="$(calc_median "${pgx_p99_runs[@]}")"
      fi
      if [[ "${#qail_rs_p50_runs[@]}" -gt 0 ]]; then
        qail_rs_p50_median="$(calc_median "${qail_rs_p50_runs[@]}")"
        qail_rs_p99_median="$(calc_median "${qail_rs_p99_runs[@]}")"
      fi

      print_latency_summary "pgx" pgx_p50_runs pgx_p95_runs pgx_p99_runs
      print_latency_summary "qail-rs" qail_rs_p50_runs qail_rs_p95_runs qail_rs_p99_runs
      print_delta_summary "delta p50 (qail-rs vs pgx, median)" "${qail_rs_p50_median:-}" "${pgx_p50_median:-}"
      print_delta_summary "delta p99 (qail-rs vs pgx, median)" "${qail_rs_p99_median:-}" "${pgx_p99_median:-}"
    else
      unset pgx_median qail_rs_median
      if [[ "${#pgx_runs[@]}" -gt 0 ]]; then
        pgx_median="$(calc_median "${pgx_runs[@]}")"
      fi
      if [[ "${#qail_rs_runs[@]}" -gt 0 ]]; then
        qail_rs_median="$(calc_median "${qail_rs_runs[@]}")"
      fi

      print_runner_summary "pgx" pgx_runs
      print_runner_summary "qail-rs" qail_rs_runs
      print_delta_summary "delta (qail-rs vs pgx, median)" "${qail_rs_median:-}" "${pgx_median:-}"
    fi
    echo
  done
done
