# AGENTS

## Source Code Map
- `src/lib.rs`: public database API surface and module exports.
- `src/sql/mod.rs`: SQL subsystem wiring (lexer/parser/planner/executor/session).
- `src/sql/parser/mod.rs`: SQL parser entry and statement dispatch.
- `src/sql/parser/ddl_admin.rs`: parser DDL/admin statement parsing helpers.
- `src/sql/parser/select_stmt.rs`: parser SELECT statement construction.
- `src/sql/parser/query_common.rs`: parser shared query/update/delete parsing helpers.
- `src/sql/parser/insert_stmt.rs`: parser INSERT/REPLACE statement parsing.
- `src/sql/parser/expr_and_select.rs`: parser expression precedence and primary-expression parsing.
- `src/sql/parser/tests.rs`: parser unit tests extracted from `parser/mod.rs`.
- `src/sql/session/mod.rs`: runtime SQL session state machine core (transaction lifecycle, execution routing).
- `src/sql/session/checkpoint.rs`: session checkpoint policy/env parsing and stats/checkpoint handlers.
- `src/sql/session/tests.rs`: session unit tests extracted from `session/mod.rs`.
- `src/sql/session/tests/tail.rs`: session tests split from `tests.rs` (poison/read-only/rekey stats cases).
- `src/sql/executor.rs` and `src/sql/executor/*.rs`: statement execution and query operators.
- `src/sql/executor/tests.rs`: executor integration-style unit tests extracted from `executor.rs`.
- `src/storage/pager/mod.rs`: pager core (header/cache/freelist IO, metadata accessors).
- `src/storage/pager/backup_rekey.rs`: pager backup and rekey operations.
- `src/storage/pager/rekey_marker.rs`: `.rekey` marker encoding/decoding helpers.
- `src/storage/pager/tests.rs`: pager unit tests.
- `src/wal/recovery/mod.rs`: WAL recovery core (validation/collection/apply orchestration).
- `src/wal/recovery/tests.rs`: WAL recovery unit tests.
- `src/wal/*.rs`: write-ahead log format, reader/writer, and recovery entry modules.
- `src/btree/ops/mod.rs`: B-tree high-level operations.
- `src/btree/ops/tests.rs`: B-tree ops tests extracted from `ops/mod.rs`.
- `src/fts/index/mod.rs`: full-text index maintenance and segment encoding.
- `src/fts/index/tests.rs`: FTS index tests extracted from `index/mod.rs`.
- `src/btree/*.rs`: B-tree nodes, cursor, and supporting operations.
- `src/schema/*.rs`: table/index catalog and schema metadata.

## Documentation Map
- `docs-site/src/`: mdBook source (edit here).
- `docs-site/src/internals/*.md`: internals documentation pages.
- `docs-site/src/user-guide/*.md`: user-facing guides and SQL reference.
- `docs-site/src/SUMMARY.md`: sidebar/order for mdBook.
- `docs-site/book/`: generated output (do not hand-edit).

## Refactor Notes
- `src/sql/session.rs` was split into `src/sql/session/mod.rs` and `src/sql/session/tests.rs`.
- Duplicated post-commit/post-rollback checkpoint flow was consolidated into `Session::post_checkpoint`.
- `src/storage/pager.rs` was split into `src/storage/pager/mod.rs` and `src/storage/pager/tests.rs`.
- Pager rekey/backup and marker handling were split into `src/storage/pager/backup_rekey.rs` and `src/storage/pager/rekey_marker.rs`.
- `src/fts/index.rs` was split into `src/fts/index/mod.rs` and `src/fts/index/tests.rs`.
- `src/btree/ops.rs` was split into `src/btree/ops/mod.rs` and `src/btree/ops/tests.rs`.
- `src/sql/parser.rs` was split into `src/sql/parser/mod.rs`, `src/sql/parser/expr_and_select.rs`, and `src/sql/parser/tests.rs`.
- Parser responsibilities were further split into `src/sql/parser/ddl_admin.rs`, `src/sql/parser/select_stmt.rs`, `src/sql/parser/query_common.rs`, and `src/sql/parser/insert_stmt.rs`.
- Session checkpoint/stats logic was split into `src/sql/session/checkpoint.rs`.
- `src/sql/session/tests.rs` was further split with `src/sql/session/tests/tail.rs`.
- `src/wal/recovery.rs` was split into `src/wal/recovery/mod.rs` and `src/wal/recovery/tests.rs`.
- `src/sql/executor.rs` tests were split into `src/sql/executor/tests.rs`.

## Shell / Git Command Hygiene
- Prefer non-interactive commands.
- When posting issue/PR comments with `gh`, do not inline long text in `--body "..."` if it may contain backticks.
- Use a temp file + `--body-file` to avoid shell command-substitution bugs:
  1. `cat > /tmp/msg.md <<'EOF'` ... `EOF`
  2. `gh issue comment <id> --body-file /tmp/msg.md`
- Same rule for `gh issue create` / `gh pr create` bodies: prefer `--body-file`.
