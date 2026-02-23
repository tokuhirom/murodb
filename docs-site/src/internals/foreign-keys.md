# Foreign Keys

This page describes how foreign-key metadata and enforcement are implemented in murodb.

## Source Layout

- Metadata types and serialization: `src/schema/catalog.rs`
- DDL validation (`CREATE TABLE`): `src/sql/executor/ddl.rs`
- ALTER integration (`ADD/DROP FOREIGN KEY`, column guards): `src/sql/executor/alter.rs`
- Runtime enforcement core: `src/sql/executor/foreign_key.rs`
- DML integration:
  - `INSERT` / `REPLACE` / `ON DUPLICATE KEY UPDATE`: `src/sql/executor/insert.rs`
  - `UPDATE` / `DELETE`: `src/sql/executor/mutation.rs`
- Introspection output: `src/sql/executor/show.rs`

## Metadata Model

Each table stores outgoing FKs in `TableDef.foreign_keys`:

- child columns: `columns`
- parent table: `ref_table`
- parent columns: `ref_columns`
- actions: `on_delete`, `on_update` (`RESTRICT`, `CASCADE`, `SET NULL`)

Metadata is persisted in `TableDef::serialize` / `TableDef::deserialize` as an optional tail.
The current format uses `FK_LAYOUT_V2_TAG` and stores both actions explicitly.

## DDL and ALTER Rules

`CREATE TABLE` and `ALTER TABLE ... ADD FOREIGN KEY` validate:

- referenced table exists
- child and parent column counts match
- referenced parent columns exist
- child/parent types are compatible
- existing rows in child table satisfy the new constraint

`ALTER TABLE` also protects FK dependencies:

- cannot drop/modify/change a column used by local FK definitions
- cannot drop parent-side columns referenced by incoming FKs (including self-reference)
- `DROP FOREIGN KEY (cols)` is rejected when ambiguous

## Runtime Enforcement

### Child-side check (outgoing FK)

`enforce_child_foreign_keys(...)` validates that each non-NULL child key has a matching parent row.
Any NULL in FK columns skips the check (SQL nullable FK behavior).

### Parent-side check (incoming FK)

- `enforce_parent_restrict_on_delete(...)`
- `enforce_parent_restrict_on_update(...)`

These locate incoming references by scanning catalog tables and selecting FKs where `ref_table` matches the parent table.

For `RESTRICT`, they fail the statement.
For `CASCADE` / `SET NULL`, they mutate child rows through helper paths in `foreign_key.rs`.

## Ordering and Atomicity Safeguards

The implementation uses ordering rules to avoid partial side effects on failed statements:

- delete path validates the full delete set first, then applies pending cascade/set-null actions
- update path validates incoming `RESTRICT` before applying pending child updates
- mutation paths run local uniqueness/outgoing-FK checks before triggering parent-side cascades
- `REPLACE` pre-validates conflict-delete effects needed for self-referential FK safety

## Recursion and Cycles

Cascade logic tracks visited `(table, pk)` entries to break recursive loops in cyclic FK graphs.
This is used for both delete and update cascades.

## Introspection

- `SHOW CREATE TABLE` renders FK clauses including `ON DELETE` and `ON UPDATE`.
- `DESCRIBE` emits an FK row with action details in `Extra`:
  - `ON DELETE <action> ON UPDATE <action>`

## Current Cost Characteristics

FK checks currently rely on scans in several places:

- parent existence checks scan parent rows
- incoming-reference discovery scans table metadata (`list_tables`)
- child-reference matching scans child table rows

This is correct but not index-accelerated yet; heavy FK workloads may pay O(table size) costs.
