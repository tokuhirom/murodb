# AGENTS

## Source Code Map
- `src/lib.rs`: public database API surface and module exports.
- `src/sql/mod.rs`: SQL subsystem wiring (lexer/parser/planner/executor/session).
- `src/sql/parser/mod.rs`: SQL parser entry and core DDL/DML parsing helpers.
- `src/sql/parser/expr_and_select.rs`: parser tail for set queries, expression parsing, and SELECT-body parsing.
- `src/sql/parser/tests.rs`: parser unit tests extracted from `parser/mod.rs`.
- `src/sql/session/mod.rs`: runtime SQL session state machine (transaction lifecycle, checkpoint policy, stats SQL handlers).
- `src/sql/session/tests.rs`: session unit tests extracted from `session/mod.rs`.
- `src/sql/executor.rs` and `src/sql/executor/*.rs`: statement execution and query operators.
- `src/storage/pager/mod.rs`: page cache/persistence core.
- `src/storage/pager/tests.rs`: pager unit tests extracted from `pager/mod.rs`.
- `src/wal/*.rs`: write-ahead log format, reader/writer, and recovery.
- `src/btree/ops/mod.rs`: B-tree high-level operations.
- `src/btree/ops/tests.rs`: B-tree ops tests extracted from `ops/mod.rs`.
- `src/fts/index/mod.rs`: full-text index maintenance and segment encoding.
- `src/fts/index/tests.rs`: FTS index tests extracted from `index/mod.rs`.
- `src/btree/*.rs`: B-tree nodes, cursor, and supporting operations.
- `src/schema/*.rs`: table/index catalog and schema metadata.

## Refactor Notes
- `src/sql/session.rs` was split into `src/sql/session/mod.rs` and `src/sql/session/tests.rs`.
- Duplicated post-commit/post-rollback checkpoint flow was consolidated into `Session::post_checkpoint`.
- `src/storage/pager.rs` was split into `src/storage/pager/mod.rs` and `src/storage/pager/tests.rs`.
- `src/fts/index.rs` was split into `src/fts/index/mod.rs` and `src/fts/index/tests.rs`.
- `src/btree/ops.rs` was split into `src/btree/ops/mod.rs` and `src/btree/ops/tests.rs`.
- `src/sql/parser.rs` was split into `src/sql/parser/mod.rs`, `src/sql/parser/expr_and_select.rs`, and `src/sql/parser/tests.rs`.
