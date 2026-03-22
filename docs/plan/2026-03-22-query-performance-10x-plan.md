# Segment Query 10x Optimization Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate full-segment in-memory loading during open/query, and achieve at least **10x query speedup** on benchmarked query scenarios.

## Current Status (2026-03-22)

- [x] Task 1 benchmark redesign + perf gate script
- [x] Lazy segment open (metadata/catalog-first), query `limit` pushdown, newest-first early stop
- [x] Persistent sidecar metadata/index with legacy fallback rebuild
- [x] Tiered auto-compaction (oldest batch) + manual full compaction retained
- [x] Segment V2 single-file segment format (manifest/header + lazy section read)
- [x] `DbOptions` 扩展：同地点击杀记录上限（默认 50）与后台维护开关
- [ ] Background compaction throttling worker (still pending)

Measured against a baseline worktree (same benches, old engine):

- `query_hot_limit20/xyz_time_subject_verb`: **1064.56x** speedup
- `query_cold_open_and_first_query/reopen_then_first_query_limit20`: **902.96x** speedup
- `query_multi_segment_fanout/segments/300`: **1334.83x** speedup
- write regression guard: `write_ingest/10000` slowdown `0.676x`, `write_ingest/50000` slowdown `0.643x` (both within `<=1.25x`)

**Architecture:** Introduce a new on-disk segment format with metadata + column blocks + persistent indexes, then execute queries with lazy materialization and limit pushdown. Keep old format readable for migration, and compact into new format in background.

**Tech Stack:** Rust (`space-logger`), Criterion benches, custom segment V2 binary layout + lazy readers.

---

## 0) Baseline Snapshot (2026-03-22)

Run:

```bash
cargo bench --bench olap_bench -- --noplot
```

Observed baseline (current `benches/olap_bench.rs`):

- `range_query/xyz_time_subject_verb`: mean `~9.31 ms` per query
- `write_ingest/10000`: mean `~136 ms`
- `write_ingest/50000`: mean `~650 ms`

Current bottleneck in code:

- Segment load path fully deserializes all columns into memory:
  - `Segment::load` reads full file + `bincode::deserialize` in [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs:346)
- Query always fan-outs all segments and materializes matching rows before global sort:
  - `SpaceLoggerDb::query` in [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs:749)

This makes startup/open and large-data query memory behavior poor, and caps query throughput.

---

## 1) Performance Contract & Benchmark Redesign

### Task 1.1: Define authoritative query performance benchmarks

**Files:**
- Modify: [benches/olap_bench.rs](/Users/zhaoliyan/IdeaProjects/space-logger/benches/olap_bench.rs)
- Create: `benches/query_perf_bench.rs`

- [ ] Add benchmark `query_hot_limit20` (same dataset, `limit=20`, mixed filters).
- [ ] Add benchmark `query_cold_open_and_first_query` (close DB, reopen, run one query).
- [ ] Add benchmark `query_multi_segment_fanout` (many small segments, same total rows).
- [ ] Keep existing write benchmarks unchanged as guardrails.

**Acceptance:**
- [ ] Benchmarks run in CI/local reproducibly.
- [ ] 10x target is explicitly defined against `base` in Criterion reports:
  - `query_hot_limit20` >= 10x faster
  - `query_cold_open_and_first_query` >= 10x faster

### Task 1.2: Add perf gating script

**Files:**
- Create: `scripts/check_bench_regression.sh`
- Create: `docs/benchmarks/perf-contract.md`

- [ ] Parse Criterion `new` vs `base` JSON.
- [ ] Fail if query speedup < 10x on target benches.
- [ ] Fail if write throughput regresses > 20%.

---

## 2) Segment Format V2 (No Full In-Memory Load)

### Task 2.1: Define file layout + metadata

**Files:**
- Create: `src/segment_v2/mod.rs`
- Create: `src/segment_v2/layout.rs`
- Create: `src/segment_v2/manifest.rs`
- Create: `docs/segment-format-v2.md`

- [ ] Define manifest with:
  - row count, seq range, min/max (`x/y/z/time`)
  - offsets/sizes for each column block
  - index presence flags
  - format version and checksum
- [ ] Split data blocks:
  - fixed-width columns (`x/y/z/time/seq`) contiguous
  - dictionary-encoded string columns (`subject/object/verb/subject_extra`)
  - blob column (`data`) with offset table

**Acceptance:**
- [ ] Segment can be opened by reading manifest only.
- [ ] No full-row deserialization needed at open.

### Task 2.2: Build persistent indexes in flush/compact

**Files:**
- Modify: [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs)
- Create: `src/segment_v2/writer.rs`
- Create: `src/segment_v2/index.rs`

- [ ] Persist:
  - `subject/object/verb` -> Roaring bitmap row IDs
  - `morton_sorted` row-id structure for xyz
  - `time_sorted` row-id structure for time range
- [ ] Persist small bloom/zone maps for segment-level pruning.

**Acceptance:**
- [ ] Flush and compact produce V2 segments with indexes.
- [ ] Old format still readable (migration bridge).

---

## 3) Query Engine V2 (Lazy + Top-K + Limit Pushdown)

### Task 3.1: Segment reader with lazy column access

**Files:**
- Create: `src/segment_v2/reader.rs`
- Create: `src/segment_v2/column_reader.rs`

- [ ] Use `memmap2` for blocks, decode only required columns.
- [ ] Materialize full `Row` only for final selected row IDs.

**Acceptance:**
- [ ] Querying by `subject+verb+time` does not touch `data` block.
- [ ] Memory profile no longer scales linearly with total segment bytes on open.

### Task 3.2: Query planner + bitmap intersection

**Files:**
- Create: `src/query/planner.rs`
- Create: `src/query/executor.rs`
- Modify: [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs)

- [ ] Plan filters by estimated selectivity (smallest-first).
- [ ] Intersect roaring bitmaps for string + range candidates.
- [ ] Add segment-level pruning via min/max and bloom metadata.

**Acceptance:**
- [ ] Candidate row IDs are bounded before row materialization.
- [ ] Multi-segment fan-out latency significantly reduced.

### Task 3.3: Limit pushdown and early-stop by `seq desc`

**Files:**
- Modify: `src/query/executor.rs`
- Modify: [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs:749)

- [ ] Execute per-segment top-k candidate retrieval.
- [ ] Merge k-way by `seq` and stop once global `limit` is satisfied.
- [ ] Avoid full result sort when `limit` is present.

**Acceptance:**
- [ ] `limit=20` query avoids scanning all matching rows.
- [ ] Hot query benchmark reaches target speedup trajectory.

---

## 4) Compaction Strategy (Reduce Query Fan-Out)

### Task 4.1: Tiered/leveled compaction

**Files:**
- Create: `src/compaction/mod.rs`
- Create: `src/compaction/policy.rs`
- Modify: [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs)

- [ ] Add size-tiered policy (initial).
- [ ] Add optional leveled policy (L0/L1/L2...).
- [ ] Keep compaction producing V2 segments + merged indexes.

**Acceptance:**
- [ ] Segment count remains bounded under sustained write.
- [ ] Query p95 stable as data grows.

### Task 4.2: Background compaction + throttling

**Files:**
- Create: `src/compaction/worker.rs`
- Modify: [src/lib.rs](/Users/zhaoliyan/IdeaProjects/space-logger/src/lib.rs)

- [ ] Background worker with rate limit / pause controls.
- [ ] Avoid blocking foreground query/write for long merges.

**Acceptance:**
- [ ] No long write stalls from synchronous full merge.

---

## 5) Compatibility & Migration

### Task 5.1: Dual reader, single writer

**Files:**
- Modify: `src/lib.rs`
- Create: `src/segment_legacy/mod.rs`

- [ ] Read both legacy and V2 segments.
- [ ] New flush/compact writes only V2.

### Task 5.2: Online migration via compaction

**Files:**
- Modify: compaction pipeline files
- Create: `docs/migration.md`

- [ ] Legacy segments converted during compaction.
- [ ] Add integrity checks (row count/seq continuity/checksum).

---

## 6) Test Plan (Correctness + Performance)

### Task 6.1: Correctness tests

**Files:**
- Create: `tests/query_engine_v2.rs`
- Create: `tests/segment_v2_roundtrip.rs`
- Create: `tests/compaction_v2.rs`

- [ ] Add tests for equality/range predicates parity with legacy.
- [ ] Add tests for `limit` ordering (`newest -> oldest`) parity.
- [ ] Add tests for mixed old/new segment query correctness.

### Task 6.2: Benchmark validation

**Files:**
- `benches/*.rs`
- `scripts/check_bench_regression.sh`

- [ ] Commit baseline report.
- [ ] Run after each phase and track speedup table in `docs/benchmarks/perf-contract.md`.
- [ ] Final gate: query benches >= 10x, write throughput regression <= 20%.

---

## 7) Delivery Milestones

### Milestone A (Fastest path to visible gains)
- [ ] Task 1 + Task 3.3 + segment-level pruning
- **Expected:** 2x~5x on hot query with `limit`.

### Milestone B (Primary 10x unlock)
- [ ] Task 2 + Task 3.1 + Task 3.2
- **Expected:** major gain on cold-open + multi-segment query; hit >=10x target.

### Milestone C (Scale stability)
- [ ] Task 4 + Task 5 + Task 6 hardening
- **Expected:** stable p95 under long-run ingest, bounded fan-out.

---

## 8) Non-Goals (This iteration)

- Do not redesign WAL format.
- Do not add full transactional isolation.
- Do not add distributed/remote storage.

---

## 9) Immediate Next Action

- [ ] Implement Task 1 first (new query benchmarks + perf gate), then freeze baseline artifacts before any engine rewrite.
