# Segment Format V2

`segment_*.bin` now uses a V2 binary layout for lazy open/query:

1. `magic` (8 bytes): `SLSEGv2\0`
2. `version` (u32 LE): `2`
3. `meta_len` (u64 LE)
4. `index_len` (u64 LE)
5. `columns_len` (u64 LE)
6. `meta` bytes (`bincode(SegmentMeta)`)
7. `index` bytes (`bincode(SegmentIndex)`, current writer stores `index_len=0`)
8. `columns` bytes (`bincode(ColumnStore)`)

## Read Path

- DB open reads only header + `meta` to build segment catalog in memory.
- Full columns and query index are loaded lazily on first use.
- Segment-level pruning (`x/y/z/time min/max`) happens before row materialization.

## Index Persistence Strategy

- Core segment durability uses the single V2 file.
- For fast reopen query planning, index is also persisted in sidecar:
  - `segment_index/segment_<id>.idx`
- If sidecar is missing, index is rebuilt from columns on demand.

## Compatibility

- Legacy V1 segment (`bincode(ColumnStore)` only) remains readable.
- On loading a V1 segment without sidecars, metadata/index sidecars are rebuilt.
- Compaction/flush always writes V2 segments.
