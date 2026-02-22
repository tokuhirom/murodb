# Catalog Format

This page documents how schema metadata is persisted in the system catalog.

## Where Catalog Metadata Lives

- The system catalog itself is a B-tree (`src/schema/catalog.rs`).
- The root page id of that B-tree is stored in the main DB header field `catalog_root`.
- Catalog keys are UTF-8 bytes; values are binary-serialized structs.

Key namespace:

- `table:<table_name>` -> serialized `TableDef`
- `index:<index_name>` -> serialized `IndexDef`

## TableDef Value Format

`TableDef::serialize` / `deserialize` in `src/schema/catalog.rs`.

Layout (length-prefixed, little-endian integers):

1. `name_len: u16`
2. `name: [u8; name_len]` (UTF-8)
3. `column_count: u16`
4. Repeated `column_count` times:
   - `col_blob_len: u16`
   - `col_blob: [u8; col_blob_len]` (`ColumnDef` payload)
5. `pk_tag: u8`
6. Primary-key names by `pk_tag`:
   - `0`: no PK names
   - `1`: single-column PK:
     - `pk_len: u16`
     - `pk_name: [u8; pk_len]`
   - `2`: composite PK:
     - `pk_count: u16`
     - repeated `pk_count` times: `pk_len: u16` + `pk_name`
7. `data_btree_root: u64`
8. `next_rowid: i64` (optional tail; defaults to `0` if absent)
9. `row_format_version: u8` (optional tail; defaults to `0` if absent)
10. `stats_row_count: u64` (optional tail; defaults to `0` if absent)

Unknown `pk_tag` causes decode failure.

## ColumnDef Embedded Blob Format

`ColumnDef::serialize` / `deserialize` in `src/schema/column.rs`.

Layout:

1. `name_len: u16`
2. `name: [u8; name_len]` (UTF-8)
3. `type_byte: u8`
4. `flags: u8`
5. Optional `size: u32` only when type is `VARCHAR`/`VARBINARY`
6. `default_tag: u8` + optional default payload
7. `check_len: u16` + optional check expression bytes

`type_byte` mapping:

- `1` BIGINT
- `2` VARCHAR
- `3` VARBINARY
- `4` TINYINT
- `5` SMALLINT
- `6` INT
- `7` TEXT
- `8` FLOAT
- `9` DOUBLE
- `10` DATE
- `11` DATETIME
- `12` TIMESTAMP

Flag bits:

- `0x01` primary key
- `0x02` unique
- `0x04` nullable
- `0x08` hidden
- `0x10` auto_increment

`default_tag` mapping:

- `0` no default
- `1` `NULL`
- `2` integer (`i64`)
- `3` string (`u16` length + bytes)
- `4` float (`f64`)

Unknown `type_byte` or `default_tag` causes decode failure.

## IndexDef Value Format

`IndexDef::serialize` / `deserialize` in `src/schema/index.rs`.

Layout:

1. `name_len: u16` + `name`
2. `table_len: u16` + `table_name`
3. `first_col_len: u16` + `first_column_name` (legacy position)
4. `index_type: u8` (`1` BTree, `2` Fulltext)
5. `is_unique: u8` (`0`/`1`)
6. `btree_root: u64`
7. `extra_col_count: u16`
8. Repeated extra columns: `col_len: u16` + `col_name`
9. `stats_distinct_keys: u64` (optional tail; default `0`)
10. Numeric-bounds extension (optional):
   - `stats_num_bounds_known: u8`
   - `stats_num_min: i64`
   - `stats_num_max: i64`
11. FULLTEXT stop-filter extension (optional):
   - `fts_stop_filter: u8`
   - `fts_stop_df_ratio_ppm: u32`
12. Histogram extension (optional):
   - `hist_bin_count: u16`
   - repeated `hist_bin_count` times: `u32`

Unknown `index_type` causes decode failure.

## Compatibility Policy in Code

Current decode strategy is append-only/tolerant:

- New fields are generally appended at the tail.
- Older records are accepted by defaulting missing tail fields.
- Some old layouts are explicitly recognized (for example `IndexDef` stats tails).
- Truncated/corrupt payloads fail decode (`None`) or ignore incomplete optional tails (histogram extension in `IndexDef`).

## Executable Spec (Tests)

Primary roundtrip tests:

- `src/schema/catalog.rs` (`test_table_def_roundtrip`)
- `src/schema/column.rs` (`test_column_roundtrip_all_types`)
- `src/schema/index.rs` (`test_composite_index_roundtrip`)

Backward-compat/malformed behavior:

- `src/schema/index.rs` (`test_deserialize_old_layout_keeps_fts_settings`)
- `src/schema/index.rs` (`test_deserialize_truncated_index_returns_none`)
