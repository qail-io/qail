#!/usr/bin/env python3
"""Run or parse the 3-way pgx/qail-rs/qail-zig benchmark and emit CSV + SVG.

Examples:
  python3 qail_pgx_modes_graph.py --rounds 12 --output-dir /tmp/qail-graphs
  python3 qail_pgx_modes_graph.py --input /tmp/qail_3way_round12.txt --output-dir /tmp/qail-graphs
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


RUNNERS = ("pgx", "qail-rs", "qail-zig")
COLORS = {
    "pgx": "#4c566a",
    "qail-rs": "#c26d00",
    "qail-zig": "#0f8b6d",
}
MODE_KEYS = {
    "Mode 1": "single",
    "Mode 2": "pipeline",
    "Mode 3": "pool10",
}
MODE_TITLES = {
    "single": "Single prepared query",
    "pipeline": "Prepared pipeline",
    "pool10": "Pool10 prepared singles",
}


@dataclass
class RoundSample:
    mode: str
    round_index: int
    runner: str
    qps: int
    order_position: int
    order_label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, help="Existing benchmark log to parse")
    parser.add_argument("--rounds", type=int, default=12, help="Rounds to run when --input is not provided")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/qail-pgx-modes-graphs"),
        help="Directory for log/csv/svg output",
    )
    return parser.parse_args()


def run_benchmark(rounds: int, output_dir: Path) -> Path:
    script = Path(__file__).with_name("abba_qail_pgx_modes.sh")
    log_path = output_dir / f"benchmark_rounds_{rounds}.log"
    cmd = ["bash", "-lc", f"ROUNDS={rounds} {script}"]
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    log_path.write_text(completed.stdout)
    return log_path


def parse_log(path: Path) -> List[RoundSample]:
    mode_re = re.compile(r"^(Mode \d):")
    round_re = re.compile(r"^  Round (\d+) \(([^)]+)\)$")
    runner_re = re.compile(r"^\s+(pgx|qail-rs|qail-zig)\s*: +([0-9]+) q/s$")

    current_mode: str | None = None
    current_round = 0
    current_order: Dict[str, int] = {}
    current_order_label = ""
    rows: List[RoundSample] = []

    for raw_line in path.read_text().splitlines():
        line = raw_line.rstrip()
        mode_match = mode_re.match(line)
        if mode_match:
            current_mode = MODE_KEYS[mode_match.group(1)]
            current_round = 0
            current_order = {}
            current_order_label = ""
            continue

        round_match = round_re.match(line)
        if round_match:
            current_round = int(round_match.group(1))
            ordered = [part.strip().replace("qail_rs", "qail-rs").replace("qail_zig", "qail-zig") for part in round_match.group(2).split("->")]
            current_order = {runner: idx + 1 for idx, runner in enumerate(ordered)}
            current_order_label = " -> ".join(ordered)
            continue

        runner_match = runner_re.match(line)
        if runner_match and current_mode and current_round:
            runner = runner_match.group(1)
            rows.append(
                RoundSample(
                    mode=current_mode,
                    round_index=current_round,
                    runner=runner,
                    qps=int(runner_match.group(2)),
                    order_position=current_order[runner],
                    order_label=current_order_label,
                )
            )

    if not rows:
        raise SystemExit(f"no benchmark samples parsed from {path}")
    return rows


def percentile(values: List[int], p: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    rank = math.ceil(p * len(ordered))
    rank = max(1, min(rank, len(ordered)))
    return float(ordered[rank - 1])


def summarize(rows: List[RoundSample]) -> List[dict]:
    grouped: Dict[tuple[str, str], List[int]] = {}
    for row in rows:
        grouped.setdefault((row.mode, row.runner), []).append(row.qps)

    out = []
    for mode in ("single", "pipeline", "pool10"):
        for runner in RUNNERS:
            vals = grouped[(mode, runner)]
            mean = statistics.mean(vals)
            median = statistics.median(vals)
            stdev = statistics.stdev(vals) if len(vals) > 1 else 0.0
            cv = stdev / mean * 100.0 if mean else 0.0
            out.append(
                {
                    "mode": mode,
                    "runner": runner,
                    "rounds": len(vals),
                    "mean_qps": mean,
                    "median_qps": median,
                    "p95_qps": percentile(vals, 0.95),
                    "min_qps": min(vals),
                    "max_qps": max(vals),
                    "stdev_qps": stdev,
                    "cv_percent": cv,
                }
            )
    return out


def write_rounds_csv(rows: List[RoundSample], out_path: Path) -> None:
    with out_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["mode", "round", "runner", "qps", "order_position", "order_label"])
        for row in rows:
            writer.writerow([row.mode, row.round_index, row.runner, row.qps, row.order_position, row.order_label])


def write_summary_csv(summary: List[dict], out_path: Path) -> None:
    fields = [
        "mode",
        "runner",
        "rounds",
        "mean_qps",
        "median_qps",
        "p95_qps",
        "min_qps",
        "max_qps",
        "stdev_qps",
        "cv_percent",
    ]
    with out_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summary)


def fmt_qps(value: float) -> str:
    return f"{value:,.0f}"


def svg_header(width: int, height: int) -> List[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<style>',
        "text { font-family: Menlo, Monaco, monospace; fill: #111827; }",
        ".title { font-size: 22px; font-weight: 700; }",
        ".subtitle { font-size: 12px; fill: #374151; }",
        ".axis { stroke: #9ca3af; stroke-width: 1; }",
        ".grid { stroke: #e5e7eb; stroke-width: 1; }",
        ".label { font-size: 12px; }",
        ".small { font-size: 10px; fill: #4b5563; }",
        "</style>",
    ]


def build_medians_svg(summary: List[dict], out_path: Path) -> None:
    width, height = 1100, 520
    left, right, top, bottom = 90, 40, 80, 80
    chart_w = width - left - right
    chart_h = height - top - bottom
    max_qps = max(item["median_qps"] for item in summary) * 1.1
    steps = 5
    parts = svg_header(width, height)
    parts.append(f'<rect width="{width}" height="{height}" fill="#f8fafc"/>')
    parts.append('<text x="40" y="38" class="title">Median Throughput by Mode</text>')
    parts.append('<text x="40" y="58" class="subtitle">qail-pg vs pgx vs qail-zig</text>')

    for i in range(steps + 1):
        y = top + chart_h - (chart_h * i / steps)
        value = max_qps * i / steps
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + chart_w}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 12}" y="{y + 4:.1f}" text-anchor="end" class="label">{fmt_qps(value)}</text>')

    group_width = chart_w / 3
    bar_width = 60
    offsets = {"pgx": -90, "qail-rs": 0, "qail-zig": 90}
    for idx, mode in enumerate(("single", "pipeline", "pool10")):
        center_x = left + group_width * (idx + 0.5)
        parts.append(f'<text x="{center_x:.1f}" y="{height - 28}" text-anchor="middle" class="label">{MODE_TITLES[mode]}</text>')
        mode_rows = [item for item in summary if item["mode"] == mode]
        for item in mode_rows:
            runner = item["runner"]
            bar_h = chart_h * item["median_qps"] / max_qps
            x = center_x + offsets[runner] - bar_width / 2
            y = top + chart_h - bar_h
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width}" height="{bar_h:.1f}" fill="{COLORS[runner]}" rx="6"/>')
            parts.append(f'<text x="{center_x + offsets[runner]:.1f}" y="{y - 8:.1f}" text-anchor="middle" class="small">{fmt_qps(item["median_qps"])}</text>')

    legend_x = width - 280
    legend_y = 34
    for idx, runner in enumerate(RUNNERS):
        y = legend_y + idx * 18
        parts.append(f'<rect x="{legend_x}" y="{y - 10}" width="12" height="12" fill="{COLORS[runner]}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 20}" y="{y}" class="label">{runner}</text>')

    parts.append("</svg>")
    out_path.write_text("\n".join(parts))


def build_rounds_svg(rows: List[RoundSample], out_path: Path) -> None:
    width, height = 1200, 900
    left, right, top = 90, 40, 70
    panel_h = 240
    panel_gap = 35
    max_round = max(row.round_index for row in rows)
    parts = svg_header(width, height)
    parts.append(f'<rect width="{width}" height="{height}" fill="#f8fafc"/>')
    parts.append('<text x="40" y="38" class="title">Per-Round Throughput</text>')
    parts.append('<text x="40" y="58" class="subtitle">Each panel uses its own y-scale; dots show individual rounds</text>')

    for panel_idx, mode in enumerate(("single", "pipeline", "pool10")):
        panel_top = top + panel_idx * (panel_h + panel_gap)
        panel_bottom = panel_top + panel_h
        panel_rows = [row for row in rows if row.mode == mode]
        max_qps = max(row.qps for row in panel_rows) * 1.08
        min_qps = min(row.qps for row in panel_rows) * 0.96
        x0 = left
        x1 = width - right
        parts.append(f'<text x="{left}" y="{panel_top - 14}" class="label">{MODE_TITLES[mode]}</text>')
        for i in range(5):
            frac = i / 4
            y = panel_bottom - panel_h * frac
            value = min_qps + (max_qps - min_qps) * frac
            parts.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" class="grid"/>')
            parts.append(f'<text x="{left - 12}" y="{y + 4:.1f}" text-anchor="end" class="small">{fmt_qps(value)}</text>')
        parts.append(f'<line x1="{x0}" y1="{panel_bottom}" x2="{x1}" y2="{panel_bottom}" class="axis"/>')
        parts.append(f'<line x1="{x0}" y1="{panel_top}" x2="{x0}" y2="{panel_bottom}" class="axis"/>')

        for r in range(1, max_round + 1):
            x = x0 + (x1 - x0) * (r - 1) / max(1, max_round - 1)
            parts.append(f'<text x="{x:.1f}" y="{panel_bottom + 18}" text-anchor="middle" class="small">{r}</text>')

        for runner in RUNNERS:
            pts = []
            runner_rows = sorted((row for row in panel_rows if row.runner == runner), key=lambda row: row.round_index)
            for row in runner_rows:
                x = x0 + (x1 - x0) * (row.round_index - 1) / max(1, max_round - 1)
                y = panel_bottom - panel_h * ((row.qps - min_qps) / (max_qps - min_qps))
                pts.append((x, y, row.qps))
            parts.append(
                '<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{pts}"/>'.format(
                    color=COLORS[runner],
                    pts=" ".join(f"{x:.1f},{y:.1f}" for x, y, _ in pts),
                )
            )
            for x, y, qps in pts:
                parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{COLORS[runner]}"/>')

    legend_x = width - 280
    legend_y = 34
    for idx, runner in enumerate(RUNNERS):
        y = legend_y + idx * 18
        parts.append(f'<rect x="{legend_x}" y="{y - 10}" width="12" height="12" fill="{COLORS[runner]}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 20}" y="{y}" class="label">{runner}</text>')

    parts.append("</svg>")
    out_path.write_text("\n".join(parts))


def write_index(summary: List[dict], output_dir: Path, log_path: Path) -> None:
    table_rows = []
    for item in summary:
        table_rows.append(
            "<tr>"
            f"<td>{item['mode']}</td>"
            f"<td>{item['runner']}</td>"
            f"<td>{fmt_qps(item['median_qps'])}</td>"
            f"<td>{fmt_qps(item['p95_qps'])}</td>"
            f"<td>{item['cv_percent']:.2f}%</td>"
            "</tr>"
        )

    html = f"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<title>qail/pgx benchmark graphs</title>
<style>
body {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 24px; background: #f8fafc; color: #111827; }}
h1, h2 {{ margin: 0 0 12px 0; }}
img {{ max-width: 100%; border: 1px solid #d1d5db; background: white; }}
table {{ border-collapse: collapse; margin: 18px 0; background: white; }}
th, td {{ border: 1px solid #d1d5db; padding: 8px 10px; text-align: right; }}
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
a {{ color: #0f766e; }}
</style>
<h1>qail / pgx benchmark graphs</h1>
<p>Source log: <a href="{log_path.name}">{log_path.name}</a></p>
<h2>Summary</h2>
<table>
<tr><th>mode</th><th>runner</th><th>median q/s</th><th>p95 q/s</th><th>cv</th></tr>
{''.join(table_rows)}
</table>
<h2>Medians</h2>
<img src="medians.svg" alt="Median throughput graph">
<h2>Rounds</h2>
<img src="rounds.svg" alt="Per-round throughput graph">
</html>
"""
    (output_dir / "index.html").write_text(html)


def print_summary(summary: List[dict], output_dir: Path) -> None:
    print("mode      runner     median_qps   p95_qps   cv")
    for item in summary:
        print(
            f"{item['mode']:<9} {item['runner']:<9} {item['median_qps']:>10.0f} {item['p95_qps']:>9.0f} {item['cv_percent']:>6.2f}%"
        )
    print(f"\nArtifacts: {output_dir}")


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    log_path = args.input if args.input else run_benchmark(args.rounds, args.output_dir)
    if args.input:
        copied = args.output_dir / log_path.name
        copied.write_text(log_path.read_text())
        log_path = copied

    rows = parse_log(log_path)
    summary = summarize(rows)

    write_rounds_csv(rows, args.output_dir / "rounds.csv")
    write_summary_csv(summary, args.output_dir / "summary.csv")
    build_medians_svg(summary, args.output_dir / "medians.svg")
    build_rounds_svg(rows, args.output_dir / "rounds.svg")
    write_index(summary, args.output_dir, log_path)
    print_summary(summary, args.output_dir)


if __name__ == "__main__":
    main()
