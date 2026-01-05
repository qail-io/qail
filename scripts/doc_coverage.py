#!/usr/bin/env python3
"""
Qail Documentation Coverage Tracker
====================================
Detects missing and incomplete doc comments across all Rust crates
by running `cargo doc` with the `missing_docs` lint enabled.

Usage:
    python3 scripts/doc_coverage.py              # Full report
    python3 scripts/doc_coverage.py --crate pg   # Single crate (pg, core, gateway)
    python3 scripts/doc_coverage.py --json        # Machine-readable JSON output
    python3 scripts/doc_coverage.py --brief       # Summary only
    python3 scripts/doc_coverage.py --diff        # Compare with last saved baseline
    python3 scripts/doc_coverage.py --save        # Save current state as baseline
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = WORKSPACE_ROOT / "scripts" / ".doc_baseline.json"

CRATES = {
    "pg": "qail-pg",
    "core": "qail-core",
    "gateway": "qail-gateway",
    "qdrant": "qail-qdrant",
    "workflow": "qail-workflow",
    "cli": "qail-cli",
}

# Items that are intentionally undocumented (internal, generated, etc.)
# Add paths here to suppress from the report.
SUPPRESSED: set[str] = set()

# ANSI colors
BOLD = "\033[1m"
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
CYAN = "\033[96m"
DIM = "\033[2m"
RESET = "\033[0m"


# ── Core logic ─────────────────────────────────────────────────────────────

def scan_crate(package_name: str) -> list[dict]:
    """Run cargo doc with missing_docs lint and parse the output."""
    env = os.environ.copy()
    env["RUSTDOCFLAGS"] = "-W missing_docs"

    result = subprocess.run(
        ["cargo", "doc", "-p", package_name, "--no-deps"],
        capture_output=True,
        text=True,
        cwd=WORKSPACE_ROOT,
        env=env,
    )

    output = result.stderr + result.stdout
    items = []
    current_item = None

    for line in output.splitlines():
        # Match: error: missing documentation for a struct field
        m_type = re.search(
            r"(?:error|warning): missing documentation for (?:a |an )?(.+?)$", line
        )
        if m_type:
            current_item = {"type": m_type.group(1).strip(), "file": "", "line": 0, "name": ""}
            items.append(current_item)
            continue

        # Match: --> pg/src/driver/mod.rs:85:1
        m_loc = re.search(r"-->\s*(.+?):(\d+):\d+", line)
        if m_loc and current_item and not current_item["file"]:
            current_item["file"] = m_loc.group(1)
            current_item["line"] = int(m_loc.group(2))
            continue

        # Match: 85 | pub host: String,
        # Try to extract the name of the undocumented item
        m_name = re.search(r"^\d+\s*\|\s*(?:pub(?:\(crate\))?\s+)?(.+?)$", line)
        if m_name and current_item and not current_item["name"]:
            raw = m_name.group(1).strip().rstrip(",").rstrip("{")
            # Clean up: extract just the identifier
            # "fn connect(" -> "connect"
            # "host: String" -> "host"
            # "Timeout(String)" -> "Timeout"
            for pattern in [
                r"(?:async\s+)?fn\s+(\w+)",      # functions/methods
                r"(\w+)\s*:",                      # struct fields
                r"(\w+)\s*\(",                     # enum variants with data
                r"(\w+)\s*\{",                     # enum variants with named fields
                r"(\w+)\s*$",                      # simple variants, consts, etc.
                r"mod\s+(\w+)",                    # modules
                r"struct\s+(\w+)",                 # structs
                r"enum\s+(\w+)",                   # enums
                r"trait\s+(\w+)",                  # traits
                r"type\s+(\w+)",                   # type aliases
                r"const\s+(\w+)",                  # constants
            ]:
                nm = re.search(pattern, raw)
                if nm:
                    current_item["name"] = nm.group(1)
                    break
            if not current_item["name"]:
                current_item["name"] = raw[:40]

    # Filter suppressed items
    return [i for i in items if i["file"] not in SUPPRESSED]


def scan_incomplete_docs(crate_dir: str) -> list[dict]:
    """Find doc comments that exist but are likely too short or are stubs."""
    incomplete = []
    src_dir = WORKSPACE_ROOT / crate_dir / "src"

    if not src_dir.exists():
        return incomplete

    for rs_file in src_dir.rglob("*.rs"):
        try:
            lines = rs_file.read_text().splitlines()
        except Exception:
            continue

        rel_path = str(rs_file.relative_to(WORKSPACE_ROOT))
        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Check for doc comments
            if line.startswith("///"):
                doc_start = i
                doc_lines = []
                while i < len(lines) and lines[i].strip().startswith("///"):
                    content = lines[i].strip().removeprefix("///").strip()
                    if content:
                        doc_lines.append(content)
                    i += 1

                # Look at the next non-empty line to see if it's a public item
                while i < len(lines) and not lines[i].strip():
                    i += 1

                if i < len(lines):
                    next_line = lines[i].strip()

                    # Only care about public items
                    is_pub = next_line.startswith("pub ")
                    is_pub_fn = "pub fn " in next_line or "pub async fn " in next_line

                    if is_pub or is_pub_fn:
                        doc_text = " ".join(doc_lines)

                        # Detect stub/incomplete docs
                        issues = []
                        if len(doc_lines) == 0:
                            issues.append("empty doc comment")
                        elif len(doc_text) < 10:
                            issues.append(f"very short ({len(doc_text)} chars)")
                        if doc_text.lower().startswith("todo"):
                            issues.append("TODO stub")
                        if doc_text.lower().startswith("fixme"):
                            issues.append("FIXME stub")
                        if "..." in doc_text and len(doc_lines) == 1:
                            issues.append("placeholder (contains ...)")

                        # Functions without parameter docs (only flag if >2 params)
                        if is_pub_fn and not any("# " in d for d in doc_lines):
                            param_count = next_line.count(",") + (1 if "(" in next_line and ")" not in next_line else 0)
                            if param_count >= 3 and "# Arguments" not in doc_text and "# Parameters" not in doc_text:
                                issues.append(f"complex fn ({param_count}+ params) without # Arguments")

                        if issues:
                            incomplete.append({
                                "file": rel_path,
                                "line": doc_start + 1,
                                "name": extract_item_name(next_line),
                                "issues": issues,
                            })
            else:
                i += 1

    return incomplete


def extract_item_name(line: str) -> str:
    """Extract the item name from a Rust declaration line."""
    for pattern in [
        r"(?:pub\s+)?(?:async\s+)?fn\s+(\w+)",
        r"(?:pub\s+)?struct\s+(\w+)",
        r"(?:pub\s+)?enum\s+(\w+)",
        r"(?:pub\s+)?trait\s+(\w+)",
        r"(?:pub\s+)?type\s+(\w+)",
        r"(?:pub\s+)?const\s+(\w+)",
        r"(?:pub\s+)?mod\s+(\w+)",
    ]:
        m = re.search(pattern, line)
        if m:
            return m.group(1)
    return line.strip()[:30]


# ── Reporting ──────────────────────────────────────────────────────────────

def grade(missing: int, total_loc: int) -> str:
    """Assign a letter grade based on missing docs per 1000 LOC."""
    if total_loc == 0:
        return "N/A"
    ratio = missing / (total_loc / 1000)
    if ratio <= 2:
        return f"{GREEN}A+{RESET}"
    elif ratio <= 5:
        return f"{GREEN}A{RESET}"
    elif ratio <= 10:
        return f"{GREEN}B+{RESET}"
    elif ratio <= 20:
        return f"{YELLOW}B{RESET}"
    elif ratio <= 40:
        return f"{YELLOW}C{RESET}"
    elif ratio <= 60:
        return f"{RED}D{RESET}"
    else:
        return f"{RED}F{RESET}"


def count_loc(crate_dir: str) -> int:
    """Count lines of Rust code in a crate."""
    src_dir = WORKSPACE_ROOT / crate_dir / "src"
    if not src_dir.exists():
        return 0
    total = 0
    for f in src_dir.rglob("*.rs"):
        try:
            total += len(f.read_text().splitlines())
        except Exception:
            pass
    return total


def print_report(results: dict[str, list[dict]], incomplete: dict[str, list[dict]], brief: bool = False):
    """Print a formatted doc coverage report."""
    print(f"\n{BOLD}{'═' * 60}{RESET}")
    print(f"{BOLD}  Qail Documentation Coverage Report{RESET}")
    print(f"{DIM}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'═' * 60}{RESET}\n")

    grand_total = 0
    grand_incomplete = 0

    # ── Summary table ──
    print(f"  {BOLD}{'Crate':<18} {'Missing':>8} {'Incomplete':>11} {'LOC':>7} {'Grade':>8}{RESET}")
    print(f"  {'─' * 54}")

    for short_name, pkg_name in CRATES.items():
        missing = results.get(short_name, [])
        inc = incomplete.get(short_name, [])
        loc = count_loc(short_name)
        g = grade(len(missing), loc)
        color = RED if len(missing) > 50 else YELLOW if len(missing) > 10 else GREEN
        print(f"  {pkg_name:<18} {color}{len(missing):>8}{RESET} {len(inc):>11} {loc:>7} {g}")
        grand_total += len(missing)
        grand_incomplete += len(inc)

    print(f"  {'─' * 54}")
    print(f"  {BOLD}{'TOTAL':<18} {grand_total:>8} {grand_incomplete:>11}{RESET}\n")

    if brief:
        return

    # ── Per-crate breakdown ──
    for short_name, pkg_name in CRATES.items():
        missing = results.get(short_name, [])
        inc = incomplete.get(short_name, [])

        if not missing and not inc:
            continue

        print(f"\n{BOLD}{CYAN}── {pkg_name} ──{RESET}")

        if missing:
            # Group by file
            by_file: dict[str, list[dict]] = defaultdict(list)
            for item in missing:
                by_file[item["file"]].append(item)

            # Group by type
            by_type = Counter(item["type"] for item in missing)

            print(f"\n  {BOLD}Missing by file:{RESET}")
            for f, items in sorted(by_file.items(), key=lambda x: -len(x[1])):
                print(f"    {YELLOW}{len(items):>3}{RESET}  {f}")
                if not brief:
                    for item in items[:5]:
                        name = item.get("name", "?")
                        print(f"         {DIM}L{item['line']}: {item['type']} `{name}`{RESET}")
                    if len(items) > 5:
                        print(f"         {DIM}... and {len(items) - 5} more{RESET}")

            print(f"\n  {BOLD}Missing by type:{RESET}")
            for typ, cnt in by_type.most_common():
                print(f"    {cnt:>3}  {typ}")

        if inc:
            print(f"\n  {BOLD}Incomplete docs:{RESET}")
            for item in inc[:10]:
                issues_str = ", ".join(item["issues"])
                print(f"    {YELLOW}{item['file']}:{item['line']}{RESET} `{item['name']}` — {issues_str}")
            if len(inc) > 10:
                print(f"    {DIM}... and {len(inc) - 10} more{RESET}")

    print()


def save_baseline(results: dict[str, list[dict]]):
    """Save current state as a baseline for future diff comparisons."""
    baseline = {
        "timestamp": datetime.now().isoformat(),
        "crates": {},
    }
    for short_name, items in results.items():
        by_file = Counter(i["file"] for i in items)
        by_type = Counter(i["type"] for i in items)
        baseline["crates"][short_name] = {
            "total": len(items),
            "by_file": dict(by_file),
            "by_type": dict(by_type),
        }

    BASELINE_PATH.write_text(json.dumps(baseline, indent=2) + "\n")
    print(f"{GREEN}✓ Baseline saved to {BASELINE_PATH}{RESET}")


def print_diff(results: dict[str, list[dict]]):
    """Compare current state with saved baseline."""
    if not BASELINE_PATH.exists():
        print(f"{RED}No baseline found. Run with --save first.{RESET}")
        return

    baseline = json.loads(BASELINE_PATH.read_text())
    ts = baseline.get("timestamp", "unknown")
    print(f"\n{BOLD}Comparing with baseline from {ts}{RESET}\n")

    print(f"  {BOLD}{'Crate':<18} {'Baseline':>9} {'Current':>9} {'Delta':>8}{RESET}")
    print(f"  {'─' * 46}")

    total_before = 0
    total_after = 0

    for short_name, pkg_name in CRATES.items():
        current = len(results.get(short_name, []))
        prev = baseline.get("crates", {}).get(short_name, {}).get("total", 0)
        delta = current - prev

        total_before += prev
        total_after += current

        if delta < 0:
            delta_str = f"{GREEN}{delta:>+8}{RESET}"
        elif delta > 0:
            delta_str = f"{RED}{delta:>+8}{RESET}"
        else:
            delta_str = f"{DIM}{delta:>+8}{RESET}"

        print(f"  {pkg_name:<18} {prev:>9} {current:>9} {delta_str}")

    total_delta = total_after - total_before
    if total_delta < 0:
        delta_str = f"{GREEN}{total_delta:>+8}{RESET}"
    elif total_delta > 0:
        delta_str = f"{RED}{total_delta:>+8}{RESET}"
    else:
        delta_str = f"{DIM}{total_delta:>+8}{RESET}"

    print(f"  {'─' * 46}")
    print(f"  {BOLD}{'TOTAL':<18} {total_before:>9} {total_after:>9} {delta_str}{RESET}\n")


def output_json(results: dict[str, list[dict]], incomplete: dict[str, list[dict]]):
    """Output machine-readable JSON."""
    output = {
        "timestamp": datetime.now().isoformat(),
        "crates": {},
    }
    for short_name in CRATES:
        output["crates"][short_name] = {
            "missing": results.get(short_name, []),
            "incomplete": incomplete.get(short_name, []),
            "total_missing": len(results.get(short_name, [])),
            "total_incomplete": len(incomplete.get(short_name, [])),
        }
    print(json.dumps(output, indent=2))


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Qail Documentation Coverage Tracker")
    parser.add_argument("--crate", "-c", help="Scan a single crate (pg, core, gateway, ...)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--brief", action="store_true", help="Summary only")
    parser.add_argument("--diff", action="store_true", help="Compare with saved baseline")
    parser.add_argument("--save", action="store_true", help="Save current state as baseline")
    parser.add_argument("--no-incomplete", action="store_true", help="Skip incomplete doc check")
    args = parser.parse_args()

    crates_to_scan = CRATES
    if args.crate:
        if args.crate not in CRATES:
            print(f"{RED}Unknown crate '{args.crate}'. Available: {', '.join(CRATES.keys())}{RESET}")
            sys.exit(1)
        crates_to_scan = {args.crate: CRATES[args.crate]}

    # Scan for missing docs
    results: dict[str, list[dict]] = {}
    incomplete: dict[str, list[dict]] = {}

    for short_name, pkg_name in crates_to_scan.items():
        if not args.json:
            print(f"{DIM}Scanning {pkg_name}...{RESET}", end=" ", flush=True)

        items = scan_crate(pkg_name)
        results[short_name] = items

        if not args.no_incomplete:
            inc = scan_incomplete_docs(short_name)
            incomplete[short_name] = inc

        if not args.json:
            color = GREEN if len(items) == 0 else YELLOW if len(items) < 20 else RED
            print(f"{color}{len(items)} missing{RESET}", end="")
            if not args.no_incomplete:
                print(f", {len(incomplete.get(short_name, []))} incomplete", end="")
            print()

    # Output
    if args.json:
        output_json(results, incomplete)
    elif args.diff:
        print_diff(results)
    elif args.save:
        print_report(results, incomplete, brief=True)
        save_baseline(results)
    else:
        print_report(results, incomplete, brief=args.brief)

    # Exit code: non-zero if any missing docs
    total = sum(len(v) for v in results.values())
    sys.exit(1 if total > 0 else 0)


if __name__ == "__main__":
    main()
