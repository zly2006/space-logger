# Performance Contract

This repository uses Criterion benchmark baselines as a performance gate.
The contract is intentionally small:

- query benchmarks must be at least `10x` faster than the saved baseline
- write benchmarks may not regress by more than `20%` in throughput

## Benchmark Layout

Criterion writes results under:

```text
target/criterion/<group>/<benchmark>/base/estimates.json
target/criterion/<group>/<benchmark>/new/estimates.json
```

For the current OLAP benchmark suite, the gate script checks:

- `query_hot_limit20/*`, `query_cold_open_and_first_query/*`,
  `query_multi_segment_fanout/*` as query benchmarks
- `write_ingest/*` as write benchmarks

The script compares `mean.point_estimate` from `estimates.json`.
For Criterion, that value is the estimated time per operation in nanoseconds.

## Run Benchmarks

Run the benchmark suite with Criterion:

```bash
cargo bench --bench olap_bench --bench query_perf_bench -- --noplot
```

To create a named baseline:

```bash
cargo bench --bench olap_bench --bench query_perf_bench -- --noplot --save-baseline base
```

That command stores the baseline data under `target/criterion/.../base/`.

To compare a later run against that baseline:

```bash
cargo bench --bench olap_bench --bench query_perf_bench -- --noplot --baseline base
```

Criterion will write the comparison data under `new/` alongside the baseline.

## Run The Gate

Use the regression gate script after you have both `base/` and `new/` results:

```bash
scripts/check_bench_regression.sh
```

You can also point it at a different Criterion result root:

```bash
scripts/check_bench_regression.sh /path/to/criterion-results
```

The default root is `target/criterion`.

The script prints one line per benchmark and returns a non-zero exit code when
any target benchmark fails the contract.

## Thresholds

### Query Benchmarks

Pass when:

```text
base_mean / new_mean >= 10.0
```

That is equivalent to:

```text
new_mean <= 0.1 * base_mean
```

### Write Benchmarks

Pass when write throughput does not regress by more than `20%`.

Because throughput is the inverse of time per operation, the same condition can
be written in either form:

```text
new_throughput >= 0.8 * base_throughput
```

or:

```text
new_mean <= 1.25 * base_mean
```

## Recommended Workflow

1. Run the benchmark suite and save a baseline with `--save-baseline base`.
2. Make the code change.
3. Run the benchmark suite again with `--baseline base`.
4. Execute `scripts/check_bench_regression.sh`.
5. If the gate fails, inspect the per-benchmark output before changing the code
   or updating the contract.

When the contract changes, update this document and the gate script together so
the documented thresholds and enforcement stay aligned.
