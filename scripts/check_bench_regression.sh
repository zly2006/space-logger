#!/usr/bin/env bash
set -euo pipefail

RESULT_ROOT="${1:-target/criterion}"

if [[ ! -d "$RESULT_ROOT" ]]; then
  echo "error: benchmark result root not found: $RESULT_ROOT" >&2
  exit 1
fi

python3 - "$RESULT_ROOT" <<'PY'
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(sys.argv[1]).resolve()
QUERY_GROUPS = {
    "query_hot_limit20",
    "query_cold_open_and_first_query",
    "query_multi_segment_fanout",
}
WRITE_GROUPS = {"write_ingest"}
QUERY_SPEEDUP_MIN = 10.0
WRITE_SLOWDOWN_MAX = 1.25


@dataclass(frozen=True)
class BenchResult:
    category: str
    bench_path: str
    base_mean_ns: float
    new_mean_ns: float


def load_estimates(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise SystemExit(f"error: missing Criterion estimates file: {path}")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"error: invalid JSON in {path}: {exc}")


def mean_point_estimate(data: dict, path: Path) -> float:
    try:
        value = data["mean"]["point_estimate"]
    except KeyError as exc:
        raise SystemExit(f"error: missing mean.point_estimate in {path}: {exc}")
    if not isinstance(value, (int, float)):
        raise SystemExit(f"error: mean.point_estimate in {path} is not numeric: {value!r}")
    return float(value)


def fmt_duration(ns: float) -> str:
    if ns >= 1_000_000_000:
        return f"{ns / 1_000_000_000:.3f}s"
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f}ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f}us"
    return f"{ns:.3f}ns"


def classify(bench_path: str) -> str | None:
    group = bench_path.split("/", 1)[0]
    if group in QUERY_GROUPS or group.startswith("query_"):
        return "query"
    if group in WRITE_GROUPS:
        return "write"
    return None


results: list[BenchResult] = []
missing: list[str] = []

for base_file in sorted(ROOT.rglob("base/estimates.json")):
    bench_dir = base_file.parent.parent
    try:
        bench_path = bench_dir.relative_to(ROOT).as_posix()
    except ValueError:
        continue

    category = classify(bench_path)
    if category is None:
        continue

    new_file = bench_dir / "new" / "estimates.json"
    if not new_file.is_file():
        missing.append(bench_path)
        continue

    base_data = load_estimates(base_file)
    new_data = load_estimates(new_file)
    results.append(
        BenchResult(
            category=category,
            bench_path=bench_path,
            base_mean_ns=mean_point_estimate(base_data, base_file),
            new_mean_ns=mean_point_estimate(new_data, new_file),
        )
    )

if missing:
    for bench_path in missing:
        print(f"FAIL {bench_path}: missing new/estimates.json", file=sys.stderr)
    raise SystemExit(1)

if not results:
    raise SystemExit(
        f"error: no query/write Criterion benchmarks found under {ROOT}. "
        "Expected base/new pairs below range_query/ and write_ingest/."
    )

failures = 0
query_count = 0
write_count = 0

print(f"Criterion root: {ROOT}")

for result in results:
    base_mean = result.base_mean_ns
    new_mean = result.new_mean_ns
    if base_mean <= 0 or new_mean <= 0:
        print(
            f"FAIL {result.category} {result.bench_path}: invalid mean values "
            f"base={base_mean} new={new_mean}",
            file=sys.stderr,
        )
        failures += 1
        continue

    slowdown = new_mean / base_mean
    speedup = base_mean / new_mean

    if result.category == "query":
        query_count += 1
        passed = speedup >= QUERY_SPEEDUP_MIN
        status = "PASS" if passed else "FAIL"
        print(
            f"{status} query {result.bench_path}: "
            f"base={fmt_duration(base_mean)} new={fmt_duration(new_mean)} "
            f"speedup={speedup:.2f}x (min {QUERY_SPEEDUP_MIN:.2f}x)"
        )
        if not passed:
            failures += 1
    else:
        write_count += 1
        passed = slowdown <= WRITE_SLOWDOWN_MAX
        status = "PASS" if passed else "FAIL"
        throughput_ratio = speedup
        print(
            f"{status} write {result.bench_path}: "
            f"base={fmt_duration(base_mean)} new={fmt_duration(new_mean)} "
            f"throughput={throughput_ratio * 100:.1f}% of baseline "
            f"(max slowdown {WRITE_SLOWDOWN_MAX:.2f}x)"
        )
        if not passed:
            failures += 1

print(
    f"Summary: {query_count} query benchmark(s), {write_count} write benchmark(s), "
    f"{failures} failure(s)."
)

raise SystemExit(1 if failures else 0)
PY
