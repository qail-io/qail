#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ROUNDS="${ROUNDS:-6}"
if ! [[ "${ROUNDS}" =~ ^[0-9]+$ ]] || [[ "${ROUNDS}" -lt 1 ]]; then
  echo "ROUNDS must be a positive integer" >&2
  exit 1
fi

WORKLOADS="${WORKLOADS:-point wide_rows large_rows many_params monster_cte}"
MODES="${MODES:-single pipeline pool10 latency}"
STMT_MODES="${STMT_MODES:-prepared unprepared}"
RUNNERS="${RUNNERS:-pgx qail_rs qail_zig}"

ZIG_REPO_ROOT="${ZIG_REPO_ROOT:-$(cd "${REPO_ROOT}/.." && pwd)/qail-zig}"
ZIG_BIN="${ZIG_BIN:-/tmp/qail_zig_modes_once}"
ZIG_CACHE_DIR="${ZIG_CACHE_DIR:-/tmp/qail-zig-bench-cache}"
case "$(uname -s):$(uname -m)" in
  Darwin:arm64) DEFAULT_ZIG_TARGET="aarch64-macos.15.0" ;;
  Darwin:x86_64) DEFAULT_ZIG_TARGET="x86_64-macos.15.0" ;;
  *) DEFAULT_ZIG_TARGET="" ;;
esac
ZIG_TARGET="${ZIG_TARGET:-${DEFAULT_ZIG_TARGET}}"

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
  local stmt_mode="$3"
  "${REPO_ROOT}/target/release/examples/qail_pgx_modes_once" \
    "${mode}" \
    --workload "${workload}" \
    --statement-mode "${stmt_mode}" \
    --plain
}

run_pgx_once() {
  local mode="$1"
  local workload="$2"
  local stmt_mode="$3"
  /tmp/pgx_modes_once -mode "${mode}" -workload "${workload}" -stmt-mode "${stmt_mode}" -plain
}

run_qail_zig_once() {
  local mode="$1"
  local workload="$2"
  local stmt_mode="$3"
  if [[ "${stmt_mode}" != "prepared" ]]; then
    echo "qail-zig only supports prepared mode in this repo layout" >&2
    return 1
  fi
  if [[ "${mode}" == "latency" ]]; then
    echo "qail-zig latency mode is not implemented in this repo layout" >&2
    return 1
  fi
  case "${workload}" in
    large_rows|monster_cte)
      echo "qail-zig workload '${workload}' is not implemented in this repo layout" >&2
      return 1
      ;;
  esac
  "${ZIG_BIN}" "${mode}" --workload "${workload}" --plain
}

run_once() {
  local runner="$1"
  local mode="$2"
  local workload="$3"
  local stmt_mode="$4"
  case "${runner}" in
    pgx) run_pgx_once "${mode}" "${workload}" "${stmt_mode}" ;;
    qail_rs) run_qail_rust_once "${mode}" "${workload}" "${stmt_mode}" ;;
    qail_zig) run_qail_zig_once "${mode}" "${workload}" "${stmt_mode}" ;;
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

echo "🏁 PGX vs QAIL (Go/Rust/Zig)"
echo "============================"
echo "rounds=${ROUNDS}"
echo "runners=${RUNNERS}"
echo "modes=${MODES}"
echo "statement_modes=${STMT_MODES}"
echo "workloads=${WORKLOADS}"
if [[ -n "${DATABASE_URL:-${QAIL_BENCH_DATABASE_URL:-}}" ]]; then
  echo "db_target=env-configured"
else
  echo "db_target=local defaults"
fi
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

ZIG_AVAILABLE=0
if runner_enabled qail_zig; then
  if [[ -d "${ZIG_REPO_ROOT}" ]]; then
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
    ZIG_AVAILABLE=1
  else
    echo "Skipping qail-zig: ZIG_REPO_ROOT not found: ${ZIG_REPO_ROOT}"
  fi
fi

echo
for workload in ${WORKLOADS}; do
  case "${workload}" in
    point) workload_label="Workload: point lookup (1 row)" ;;
    wide_rows) workload_label="Workload: wide rows (128-512 rows/query, mixed types)" ;;
    large_rows) workload_label="Workload: large rows (10k-16k rows/query, table-backed)" ;;
    many_params) workload_label="Workload: many params (32 binds/query, scalar result)" ;;
    monster_cte) workload_label="Workload: monster CTE (server-heavy, scalar result)" ;;
    *) workload_label="Workload: ${workload}" ;;
  esac
  echo "${workload_label}"

  for mode in ${MODES}; do
    case "${mode}" in
      single) label="  Mode 1: multi single-query (1 conn)" ;;
      pipeline) label="  Mode 2: pipelined batch (1 conn)" ;;
      pool10) label="  Mode 3: pooling (10 open conns)" ;;
      latency) label="  Mode 4: single-query latency (1 conn)" ;;
      *) label="  ${mode}" ;;
    esac

    for stmt_mode in ${STMT_MODES}; do
      echo "${label} [${stmt_mode}]"

      active_runners=()
      for runner in ${RUNNERS}; do
        case "${runner}" in
          qail_zig)
            if [[ "${stmt_mode}" != "prepared" ]]; then
              continue
            fi
            if [[ "${ZIG_AVAILABLE}" -ne 1 ]]; then
              continue
            fi
            if [[ "${mode}" == "latency" ]]; then
              continue
            fi
            case "${workload}" in
              large_rows|monster_cte) continue ;;
            esac
            ;;
        esac
        active_runners+=("${runner}")
      done

      if [[ "${#active_runners[@]}" -eq 0 ]]; then
        echo "    no active runners for this slice"
        echo
        continue
      fi

      if [[ "${stmt_mode}" == "unprepared" ]] && runner_enabled qail_zig; then
        echo "    note: qail-zig skipped for unprepared mode"
      fi
      if [[ "${mode}" == "latency" ]] && runner_enabled qail_zig; then
        echo "    note: qail-zig skipped for latency mode"
      fi
      case "${workload}" in
        large_rows|monster_cte)
          if runner_enabled qail_zig; then
            echo "    note: qail-zig skipped for workload '${workload}'"
          fi
          ;;
      esac

      qail_rs_runs=()
      qail_zig_runs=()
      pgx_runs=()
      qail_rs_p50_runs=()
      qail_rs_p95_runs=()
      qail_rs_p99_runs=()
      qail_zig_p50_runs=()
      qail_zig_p95_runs=()
      qail_zig_p99_runs=()
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
          3)
            case $((i % 3)) in
              0) order=("${active_runners[0]}" "${active_runners[1]}" "${active_runners[2]}") ;;
              1) order=("${active_runners[1]}" "${active_runners[2]}" "${active_runners[0]}") ;;
              2) order=("${active_runners[2]}" "${active_runners[0]}" "${active_runners[1]}") ;;
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

        unset pgx_qps qail_rs_qps qail_zig_qps
        unset pgx_p50 pgx_p95 pgx_p99 qail_rs_p50 qail_rs_p95 qail_rs_p99 qail_zig_p50 qail_zig_p95 qail_zig_p99
        for runner in "${order[@]}"; do
          output="$(run_once "${runner}" "${mode}" "${workload}" "${stmt_mode}")"
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
              qail_zig)
                qail_zig_p50="${p50}"
                qail_zig_p95="${p95}"
                qail_zig_p99="${p99}"
                qail_zig_p50_runs+=("${p50}")
                qail_zig_p95_runs+=("${p95}")
                qail_zig_p99_runs+=("${p99}")
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
              qail_zig)
                qail_zig_qps="${qps}"
                qail_zig_runs+=("${qps}")
                ;;
            esac
          fi
        done

        if [[ "${mode}" == "latency" ]]; then
          for runner in "${active_runners[@]}"; do
            case "${runner}" in
              pgx) printf "      pgx      : p50=%8.3f ms | p95=%8.3f ms | p99=%8.3f ms\n" "${pgx_p50}" "${pgx_p95}" "${pgx_p99}" ;;
              qail_rs) printf "      qail-rs  : p50=%8.3f ms | p95=%8.3f ms | p99=%8.3f ms\n" "${qail_rs_p50}" "${qail_rs_p95}" "${qail_rs_p99}" ;;
              qail_zig) printf "      qail-zig : p50=%8.3f ms | p95=%8.3f ms | p99=%8.3f ms\n" "${qail_zig_p50}" "${qail_zig_p95}" "${qail_zig_p99}" ;;
            esac
          done
        else
          for runner in "${active_runners[@]}"; do
            case "${runner}" in
              pgx) printf "      pgx      : %8.0f q/s\n" "${pgx_qps}" ;;
              qail_rs) printf "      qail-rs  : %8.0f q/s\n" "${qail_rs_qps}" ;;
              qail_zig) printf "      qail-zig : %8.0f q/s\n" "${qail_zig_qps}" ;;
            esac
          done
        fi
      done

      if [[ "${mode}" == "latency" ]]; then
        unset pgx_p50_median pgx_p99_median qail_rs_p50_median qail_rs_p99_median qail_zig_p50_median qail_zig_p99_median
        if [[ "${#pgx_p50_runs[@]}" -gt 0 ]]; then
          pgx_p50_median="$(calc_median "${pgx_p50_runs[@]}")"
          pgx_p99_median="$(calc_median "${pgx_p99_runs[@]}")"
        fi
        if [[ "${#qail_rs_p50_runs[@]}" -gt 0 ]]; then
          qail_rs_p50_median="$(calc_median "${qail_rs_p50_runs[@]}")"
          qail_rs_p99_median="$(calc_median "${qail_rs_p99_runs[@]}")"
        fi
        if [[ "${#qail_zig_p50_runs[@]}" -gt 0 ]]; then
          qail_zig_p50_median="$(calc_median "${qail_zig_p50_runs[@]}")"
          qail_zig_p99_median="$(calc_median "${qail_zig_p99_runs[@]}")"
        fi

        print_latency_summary "pgx" pgx_p50_runs pgx_p95_runs pgx_p99_runs
        print_latency_summary "qail-rs" qail_rs_p50_runs qail_rs_p95_runs qail_rs_p99_runs
        print_latency_summary "qail-zig" qail_zig_p50_runs qail_zig_p95_runs qail_zig_p99_runs
        print_delta_summary "delta p50 (qail-rs vs pgx, median)" "${qail_rs_p50_median:-}" "${pgx_p50_median:-}"
        print_delta_summary "delta p99 (qail-rs vs pgx, median)" "${qail_rs_p99_median:-}" "${pgx_p99_median:-}"
        print_delta_summary "delta p50 (qail-zig vs pgx, median)" "${qail_zig_p50_median:-}" "${pgx_p50_median:-}"
      else
        unset pgx_median qail_rs_median qail_zig_median
        if [[ "${#pgx_runs[@]}" -gt 0 ]]; then
          pgx_median="$(calc_median "${pgx_runs[@]}")"
        fi
        if [[ "${#qail_rs_runs[@]}" -gt 0 ]]; then
          qail_rs_median="$(calc_median "${qail_rs_runs[@]}")"
        fi
        if [[ "${#qail_zig_runs[@]}" -gt 0 ]]; then
          qail_zig_median="$(calc_median "${qail_zig_runs[@]}")"
        fi

        print_runner_summary "pgx" pgx_runs
        print_runner_summary "qail-rs" qail_rs_runs
        print_runner_summary "qail-zig" qail_zig_runs
        print_delta_summary "delta (qail-rs vs pgx, median)" "${qail_rs_median:-}" "${pgx_median:-}"
        print_delta_summary "delta (qail-zig vs pgx, median)" "${qail_zig_median:-}" "${pgx_median:-}"
        print_delta_summary "delta (qail-zig vs qail-rs, median)" "${qail_zig_median:-}" "${qail_rs_median:-}"
      fi
      echo
    done
  done
done
