# Roadmap

## Implemented

- [x] Basic CRUD (INSERT, SELECT, UPDATE, DELETE)
- [x] CREATE TABLE (PRIMARY KEY, UNIQUE, NOT NULL)
- [x] CREATE INDEX / CREATE UNIQUE INDEX (single column)
- [x] CREATE FULLTEXT INDEX (bigram, BM25, NATURAL/BOOLEAN mode, snippet)
- [x] MySQL-compatible integer types (TINYINT, SMALLINT, INT, BIGINT)
- [x] VARCHAR(n), VARBINARY(n), TEXT with size validation
- [x] WHERE with comparison operators (=, !=, <, >, <=, >=)
- [x] AND, OR logical operators
- [x] ORDER BY (ASC/DESC, multi-column), LIMIT
- [x] JOIN (INNER, LEFT, CROSS) with table aliases
- [x] BEGIN / COMMIT / ROLLBACK
- [x] SHOW TABLES
- [x] Multi-row INSERT
- [x] Hidden _rowid auto-generation for tables without explicit PK
- [x] AES-256-GCM-SIV encryption, Argon2 KDF
- [x] WAL-based crash recovery
- [x] CLI with REPL
- [x] DROP TABLE / DROP TABLE IF EXISTS
- [x] DROP INDEX
- [x] IF NOT EXISTS for CREATE TABLE / CREATE INDEX
- [x] SHOW CREATE TABLE
- [x] DESCRIBE / DESC table
- [x] LIKE / NOT LIKE (% and _ wildcards)
- [x] IN (value list)
- [x] BETWEEN ... AND ...
- [x] IS NULL / IS NOT NULL
- [x] NOT operator (general)
- [x] OFFSET (SELECT ... LIMIT n OFFSET m)
- [x] DEFAULT column values
- [x] AUTO_INCREMENT
- [x] Arithmetic operators in expressions (+, -, *, /, %)
- [x] BOOLEAN type (alias for TINYINT)
- [x] CHECK constraint

## Phase 2 — Built-in Functions ✓

MySQL-compatible scalar functions.

- [x] String: LENGTH, CHAR_LENGTH, CONCAT, SUBSTRING/SUBSTR, UPPER, LOWER
- [x] String: TRIM, LTRIM, RTRIM, REPLACE, REVERSE, REPEAT
- [x] String: LEFT, RIGHT, LPAD, RPAD, INSTR/LOCATE
- [x] String: REGEXP / REGEXP_LIKE
- [x] Numeric: ABS, CEIL/CEILING, FLOOR, ROUND, MOD, POWER/POW
- [x] NULL handling: COALESCE, IFNULL, NULLIF, IF
- [x] Type conversion: CAST(expr AS type)
- [x] CASE WHEN ... THEN ... ELSE ... END

## Phase 3 — Aggregation & Grouping ✓

- [x] COUNT, SUM, AVG, MIN, MAX
- [x] COUNT(DISTINCT ...)
- [x] GROUP BY (single and multiple columns)
- [x] HAVING
- [x] SELECT DISTINCT

## Phase 4 — Schema Evolution ✓

- [x] ALTER TABLE ADD COLUMN
- [x] ALTER TABLE DROP COLUMN
- [x] ALTER TABLE MODIFY COLUMN / CHANGE COLUMN
- [x] RENAME TABLE
- [x] Composite PRIMARY KEY
- [x] Composite UNIQUE / composite INDEX

## Phase 5 — Advanced Query ✓

- [x] Subqueries (WHERE col IN (SELECT ...), scalar subquery)
- [x] UNION / UNION ALL
- [x] EXISTS / NOT EXISTS
- [x] INSERT ... ON DUPLICATE KEY UPDATE
- [x] REPLACE INTO
- [x] EXPLAIN (query plan display)
- [x] RIGHT JOIN
- [x] Shared-lock read path (`Database::query`) with CLI auto routing

## Phase 6 — Types & Storage

- [x] FLOAT / DOUBLE
- [x] DATE, DATETIME, TIMESTAMP
  - Scope: fully align parser/executor/CAST/default/literal behavior and edge-case validation.
  - Done when:
    - Temporal literals and string casts behave consistently across INSERT/UPDATE/WHERE.
    - Arithmetic and comparison semantics are defined/documented for mixed temporal expressions.
    - Timezone handling policy is explicit (especially TIMESTAMP input/output normalization).
    - Invalid dates/times reject with deterministic errors.
- [x] Date/time functions: NOW, CURRENT_TIMESTAMP, DATE_FORMAT, etc.
- [ ] BLOB (skipped for now)
  - Decision (2026-02-22): defer and move focus to Phase 7 performance work.
  - Why skipped now:
    - Current product priorities are query/index performance and planner improvements, not large-object type expansion.
    - `BLOB` adds non-trivial storage/operational surface area (limits, indexing semantics, comparison behavior) with low near-term user impact.
    - Existing `VARBINARY(n)`/`TEXT` coverage is sufficient for current workloads.
  - Revisit when:
    - There is a concrete workload requiring large binary payloads that cannot be handled acceptably by current types.
    - The performance roadmap items in Phase 7 are complete or no longer the bottleneck.
- [x] Overflow pages (posting list > 4096B)
  - Scope: support values/postings that exceed single-page capacity.
  - Progress:
    - Implemented FTS segment overflow chains (`__segovf__`) with typed page format (`OFG1`).
    - Read/write/delete + vacuum path now reclaims overflow pages without orphaning.
    - Covered by unit/integration tests (`cargo test` green as of 2026-02-22).
    - Added WAL recovery integration tests for overflow chains (torn WAL tail and post-sync partial-write replay paths).
    - Benchmarked on 2026-02-22 (`murodb_bench`, commit `829ad18145c2`) with no severe small-record regression signal.
  - Done when:
    - Overflow chain format is versioned and crash-safe.
    - WAL/recovery covers partial-write and torn-tail scenarios for overflow chains.
    - Vacuum/reclaim path correctly frees overflow pages without orphaning.
    - Benchmarks show no severe regressions for small records.

## Phase 7 — Performance & Internals

- [x] Auto-checkpoint (threshold-based WAL)
- [x] Composite index range scan
  - Progress:
    - Added planner/executor support for composite-index range seek on the last key part (e.g. `(a,b)` with `a = ?` and `b` range).
    - EXPLAIN now reports `type=range` for this access path.
    - EXPLAIN now reports estimated cardinality via `rows`.
  - Done when:
    - Multi-column prefix ranges (`(a,b)` with predicates on `a`, optional range on `b`) use index scan.
    - EXPLAIN shows index-range choice and estimated cardinality.
    - Fallback path remains correct for unsupported predicate shapes.
- [x] Query optimizer improvements (cost-based)
  - Progress:
    - Added deterministic heuristic cost hints for `PkSeek` / `IndexSeek` / `IndexRangeSeek` / `FullScan`.
    - Planner now compares index candidates by cost instead of choosing the first matching index.
    - EXPLAIN now reports a `cost` column for the chosen plan.
    - Added persisted stats via `ANALYZE TABLE` (`table_rows`, `index_distinct_keys`) in catalog metadata.
    - EXPLAIN row estimation now prefers persisted `table_rows` when available.
    - Planner cost model now incorporates persisted `table_rows`/`index_distinct_keys` when available, with conservative fallback selectivity when stats are missing.
    - EXPLAIN `rows`/`cost` now uses the same planner estimation logic (with table-row fallback), so estimates reflect planner tradeoffs.
    - JOIN loop-order choice for `INNER`/`CROSS` now uses planner-side estimated row counts (stats-aware with runtime fallback) and keeps row shape (`left + right`) stable.
    - `ANALYZE TABLE` now persists numeric min/max bounds and equal-width histogram bins for single-column numeric B-tree indexes; range row estimation uses these stats when available.
    - EXPLAIN for JOIN now reports nested-loop outer-side choice with estimated left/right row counts in `Extra`.
  - Done when:
    - Planner compares at least full-scan vs single-index vs join-order alternatives.
    - Basic column stats/histograms are persisted and refreshable.
    - Plan choice is deterministic under identical stats.
- [x] FTS stop-ngram filtering
  - Progress:
    - Added FULLTEXT options `stop_filter` and `stop_df_ratio_ppm` (ppm threshold).
    - NATURAL LANGUAGE MODE now supports skipping high-DF ngrams when enabled.
    - Default remains OFF for exact-behavior compatibility.
    - Recall/precision tradeoff example documented in Full-Text Search guide.
  - Done when:
    - Frequent low-information ngrams are skipped using configurable thresholds.
    - Recall/precision tradeoff is documented with benchmark examples.
    - Toggle exists for exact behavior compatibility.
- [x] fts_snippet acceleration (pos-to-offset map)
  - Progress:
    - Replaced snippet byte/char conversion loops with a UTF-8 position-to-offset map plus binary search.
    - Snippet assembly now slices by byte ranges instead of repeatedly collecting char vectors.
    - Added dedicated benchmark runner (`murodb_snippet_bench`) with legacy-vs-new comparison and offset-map memory estimate.
    - On 2026-02-22 (local, release build), long-text tail-hit case showed small p50 improvement (legacy `1245.52us` -> new `1228.43us`).
  - Done when:
    - Snippet generation avoids repeated UTF-8 rescans for long docs.
    - Latency improvement is measured and documented on representative datasets.
    - Memory overhead remains bounded and observable.

## Phase 8 — Security (Future)

- [ ] Key rotation (epoch-based re-encryption)
  - Progress:
    - Extended WAL `MetaUpdate` to persist `epoch` alongside `catalog_root` / `page_count` / `freelist_page_id`.
    - WAL recovery now restores the latest committed `epoch` value into DB metadata.
    - Added backward-compatible decode defaults (`epoch=0`) for legacy WAL MetaUpdate records.
  - Decision (2026-02-22):
    - Deferred for now due to low immediate demand relative to implementation/operational complexity.
    - Keep current work as groundwork and resume when concrete user or compliance requirements arise.
  - Done when:
    - Online/offline rotation flow is available with resumable progress.
    - WAL + data file epoch mismatch handling is crash-safe.
    - Rotation metrics/events are visible via inspection commands.
- [ ] Collation support (Japanese sort order, etc.)
  - Decision (2026-02-22):
    - Deferred for now; reverted the in-progress SQL `COLLATE` implementation due high complexity/maintenance cost relative to current user value.
    - For now, use function-based handling in queries (for example `LOWER(col) = LOWER(?)`, `ORDER BY LOWER(col)`).
  - Revisit when:
    - There is clear demand for locale-aware ordering/comparison beyond function-based workarounds.
    - We are ready to align planner/index key encoding with collation semantics end-to-end.
  - Done when:
    - Collation can be selected per column/index.
    - ORDER BY / comparison / LIKE behavior is deterministic per collation.
    - Index key encoding respects collation sort rules.

## Phase 9 — Practical Embedded DB (Next)

Real-world deployment features to make MuroDB easier to embed and operate.

- [x] Encryption OFF mode
  - Motivation: some embedded deployments prefer CPU savings and rely on disk/host-level protection.
  - Done when:
    - DB format can be created/opened in explicit plaintext mode.
    - File header clearly records mode to avoid accidental mis-open.
    - CLI/API require explicit opt-in (no silent downgrade from encrypted DB).
- [x] Pluggable encryption suite
  - Motivation: allow policy-driven algorithm choice without forking storage engine.
  - Done when:
    - Algorithm + KDF are selected by explicit config at DB creation.
    - Supported suites are versioned, discoverable, and recorded in metadata.
    - Wrong-suite open errors are deterministic and actionable.
- [ ] Rekey / algorithm migration
  - Done when:
    - Existing DB can migrate key and/or cipher suite safely.
    - Migration is resumable and crash-recoverable.
    - Rollback/retry procedure is documented and tested.
- [ ] Backup API + consistent snapshot
  - Done when:
    - Online consistent backup without long writer stalls.
    - Restore path validated by integration tests.
    - Snapshot metadata includes format/security parameters.
- [ ] Operational limits and safeguards
  - Done when:
    - Configurable caps for DB file size, WAL size, statement timeout, and memory budget.
    - Error surfaces are clear and machine-parseable for host applications.
    - Default limits are documented with recommended profiles (edge device / server / CI).
