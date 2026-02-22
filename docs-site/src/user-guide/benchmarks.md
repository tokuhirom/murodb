# Benchmarks

This page tracks MuroDB performance over time for a fixed embedded-DB style workload mix.
Each entry is tied to a concrete version (`git commit`) so changes can be compared historically.

## Benchmark Scope

Current benchmark runner: `src/bin/murodb_bench.rs`

Workloads:

- `point_select_pk`: point lookup by primary key (`SELECT ... WHERE id = ?`)
- `point_update_pk`: point update by primary key (`UPDATE ... WHERE id = ?`)
- `insert_autocommit`: single-row insert per transaction (auto-commit)
- `range_scan_limit_100`: range read (`WHERE id >= ? ORDER BY id LIMIT 100`)
- `mixed_80r_15u_5i`: mixed OLTP-like workload (80% read / 15% update / 5% insert)
- `fts_select_natural`: fulltext natural-language search (`MATCH(body) AGAINST(... IN NATURAL LANGUAGE MODE)`)
- `fts_update_point`: point update on FTS-indexed `TEXT` column
- `fts_mixed_70q_30u`: FTS-focused mixed workload (70% search / 30% update)

Default dataset/config:

- initial rows: `20,000`
- fts initial rows: `256`
- select ops: `20,000`
- update ops: `5,000`
- insert ops: `5,000`
- scan ops: `2,000`
- mixed ops: `10,000`
- fts select ops: `5,000`
- fts update ops: `2,000`
- fts mixed ops: `5,000`
- warmup ops: `200`
- batch size (initial load): `500`

Run command:

```bash
cargo run --release --bin murodb_bench
```

> Migration note: as of 2026-02-22, benchmark scope includes additional FTS workloads.
> Older entries without FTS metrics are not directly comparable on total runtime.

## Versioned Results

| Date (UTC) | Commit | Environment | Notes |
|---|---|---|---|
| 2026-02-22 | `a78694537f59` | local dev machine | first baseline |
| 2026-02-22 | `829ad18145c2` | local dev machine | after secondary-index root persistence fix |

### 2026-02-22 / `a78694537f59`

Raw output summary:

| Workload | Ops | Total sec | Ops/sec | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| point_select_pk | 20,000 | 0.144532 | 138,377.80 | 0.0082 | 0.0096 | 0.0108 |
| point_update_pk | 5,000 | 27.098314 | 184.51 | 5.2210 | 6.9286 | 8.9318 |
| insert_autocommit | 5,000 | 8.785356 | 569.13 | 1.5480 | 2.4816 | 5.7406 |
| range_scan_limit_100 | 2,000 | 20.240664 | 98.81 | 9.6326 | 13.5310 | 13.9811 |
| mixed_80r_15u_5i | 10,000 | 10.417702 | 959.90 | 0.0112 | 6.2421 | 6.7669 |

Row counts:

- start: `20,000`
- after insert phase: `25,000`
- final: `25,519`

### 2026-02-22 / `829ad18145c2`

Raw output summary:

| Workload | Ops | Total sec | Ops/sec | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| point_select_pk | 20,000 | 0.123495 | 161,949.51 | 0.0072 | 0.0076 | 0.0087 |
| point_update_pk | 5,000 | 8.146347 | 613.77 | 1.4856 | 2.0578 | 5.4681 |
| insert_autocommit | 5,000 | 8.675668 | 576.32 | 1.5077 | 2.7074 | 5.8033 |
| range_scan_limit_100 | 2,000 | 18.577489 | 107.66 | 9.2207 | 12.1729 | 12.5408 |
| mixed_80r_15u_5i | 10,000 | 3.626337 | 2,757.60 | 0.0098 | 1.7256 | 2.4382 |
| fts_select_natural | 5,000 | 2.797700 | 1,787.18 | 0.5595 | 0.6087 | 0.6366 |
| fts_update_point | 2,000 | 11.462999 | 174.47 | 5.4321 | 7.0625 | 9.7803 |
| fts_mixed_70q_30u | 5,000 | 10.865190 | 460.19 | 0.6296 | 6.3375 | 7.4033 |

Row counts:

- start: `20,000`
- after insert phase: `25,000`
- final: `25,519`

## Adding New Entries

When updating this page for a new version:

1. Run `cargo run --release --bin murodb_bench`.
2. Record `git rev-parse --short=12 HEAD`.
3. Append one row to the "Versioned Results" table.
4. Add a new subsection with the raw metrics table for that commit.

Keep benchmark parameters constant unless intentionally changing the benchmark definition.
If benchmark definitions change, include a short migration note in the new entry.
