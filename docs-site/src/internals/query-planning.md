# Query Planning & Execution

This chapter explains how SQL predicates are converted into access paths and then executed.

## Pipeline

`sql/parser` produces AST (`Statement` / `Select`), then:

1. `plan_select(...)` in `src/sql/planner.rs` chooses a `Plan`.
2. Executor modules (`src/sql/executor/select_query.rs`, `src/sql/executor/mutation.rs`) dispatch by `Plan`.
3. B+tree/index scans are performed via `BTree::search`, `scan`, `scan_from`.

## Plan Types

`Plan` currently has these variants:

- `PkSeek`: full primary-key equality (single or composite).
- `IndexSeek`: equality lookup on a B-tree secondary index.
- `IndexRangeSeek`: bounded/ranged lookup on index prefix + next column range.
- `FtsScan`: full-text path using `MATCH ... AGAINST`.
- `FullScan`: fallback table scan.

## Candidate Extraction from WHERE

Planner heuristics extract:

- equalities (`col = expr`)
- numeric ranges (`<`, `<=`, `>`, `>=`, `BETWEEN`)
- full-text predicates (`MATCH(...) AGAINST(...)`)

Selection order:

1. If FTS predicate is present, choose `FtsScan`.
2. If all PK columns are equality-constrained, choose `PkSeek`.
3. Otherwise evaluate index candidates and pick minimum cost.
4. If none matches, use `FullScan`.

## Cost Model (Deterministic Heuristic)

`plan_cost_hint_with_stats` uses a stable heuristic (smaller is better):

- `PkSeek`: `100 + est_rows`
- `IndexSeek`: `1500 - 300*key_parts + 3*est_rows`
- `IndexRangeSeek`: `1400 - 250*prefix_parts - 250*bound_terms + 3*est_rows`
- `FtsScan`: `2000 + 2*est_rows`
- `FullScan`: `3000 + 5*est_rows`

Tie-break uses a stable string key, so identical inputs keep deterministic plans.

## Row Estimation Inputs

Estimator uses:

- table row count (`TableDef.stats_row_count`)
- index distinct count and optional numeric histogram (`IndexDef` stats)
- fallback defaults when stats are missing

`ANALYZE TABLE` persists these stats and improves plan quality.

## Plan-to-Executor Mapping

Main dispatch happens in `src/sql/executor/select_query.rs`:

- `PkSeek`: encode PK bytes and do one data B-tree lookup.
- `IndexSeek`: encode index key, fetch matching PKs from index B-tree, then fetch rows from data B-tree.
- `IndexRangeSeek`: range-scan index keys, then fetch rows by PK.
- `FtsScan`: evaluate FTS postings and scoring, then materialize matching rows.
- `FullScan`: iterate data B-tree and filter with WHERE.

For `UPDATE` / `DELETE`, planner is reused, then matching PKs are collected before mutation to avoid in-place scan mutation hazards.

## JOIN Strategy

Join execution is currently nested loop (`src/sql/executor/select_join.rs`).
For `INNER` / `CROSS`, loop order is chosen from estimated cardinality:

- smaller side tends to be outer loop (`choose_nested_loop_order`).

`EXPLAIN` includes join-loop notes in `Extra`.

## EXPLAIN Mapping

`src/sql/executor/select_meta.rs` maps plan to EXPLAIN fields:

- access `type`: `const`, `ref`, `range`, `fulltext`, `ALL`
- `key`: `PRIMARY` or chosen index name
- `rows`: estimated rows
- `cost`: heuristic planner cost
- `Extra`: e.g. `Using where`, `Using index`, `Using fulltext`

This is a planner/debug aid, not a precise runtime profiler.

## DDL Execution: ALTER TABLE

`ALTER TABLE` is executed in `src/sql/executor/alter.rs` and is mostly planner-independent.

### Operation Dispatch

`exec_alter_table(...)` dispatches by AST operation:

- `ADD COLUMN`
- `DROP COLUMN`
- `MODIFY COLUMN`
- `CHANGE COLUMN` (rename + optional type/constraint change)

### Fast Path vs Rewrite Path

Implementation uses two paths:

- metadata-only (catalog update only): no row rewrite
- full rewrite (scan + rebuild data B-tree): required when row bytes must change

Rules in current code:

- `ADD COLUMN` is metadata-only.
- `DROP COLUMN` always rewrites all rows.
- `MODIFY` / `CHANGE` rewrites only when column type changes.
- `MODIFY` / `CHANGE` without type change is metadata-only.

### Safety Checks and Validation

Before applying metadata/rewrite:

- adding `PRIMARY KEY` via `ADD COLUMN` is rejected
- dropping a PK column is rejected
- dropping a column referenced by any index is rejected
- adding `NOT NULL` checks existing rows for `NULL` and fails if found
- `ADD COLUMN ... NOT NULL` without `DEFAULT` fails on non-empty tables

### Rewrite Algorithm (when triggered)

Rewrite path is:

1. Scan old data B-tree and decode each row.
2. Transform row shape/value (drop column or type coercion).
3. Collect all old data-tree page IDs and free them.
4. Create a new data B-tree root and reinsert transformed rows.
5. Update `TableDef.data_btree_root` and persist catalog metadata.

Rewritten rows are stored in row format v1.

### Unique Index Reconciliation

After `MODIFY` / `CHANGE`, `reconcile_unique_index(...)` adjusts single-column unique index state:

- add `UNIQUE`: validate duplicates first, then create auto unique index
- remove `UNIQUE`: drop corresponding unique index and free its pages

For `CHANGE COLUMN`, index metadata that references the old column name is renamed to the new name.
