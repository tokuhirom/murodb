# Limits Reference

This page documents the known limits of MuroDB. These limits arise from the fixed page size,
serialization formats, and design decisions of the storage engine.

## Page & Row Limits

| Limit | Value | Notes |
|---|---|---|
| Page size | 4,096 bytes | Fixed; all data pages, B-tree nodes, and catalog entries use this size |
| Page header | 14 bytes | page_id (8) + cell_count (2) + free_start (2) + free_end (2) |
| Max row size | ~4,048 bytes | Depends on key size and column overhead; row overflow is not supported |
| Max cell payload | ~4,078 bytes | 4,096 − 14 (header) − 2 (cell pointer) − 2 (cell length prefix) |

Rows that exceed the page capacity will produce a **PageOverflow** error.
MuroDB does not currently support row overflow pages; a single row must fit within one page.

## Column Limits

| Limit | Value | Notes |
|---|---|---|
| Max column count | 65,535 (u16) | Serialization limit; practical limit is lower due to page capacity |
| Column name max length | 65,535 bytes (u16) | Limited in practice by catalog page capacity |
| Table name max length | 65,535 bytes (u16) | Limited in practice by catalog page capacity |

## Data Type Ranges

| Type | Min | Max | Storage |
|---|---|---|---|
| TINYINT | −128 | 127 | 1 byte |
| SMALLINT | −32,768 | 32,767 | 2 bytes |
| INT | −2,147,483,648 | 2,147,483,647 | 4 bytes |
| BIGINT | −2^63 | 2^63 − 1 | 8 bytes |
| FLOAT | ±1.2×10^−38 | ±3.4×10^38 | 4 bytes (finite values only; NaN/Infinity rejected) |
| DOUBLE | ±2.2×10^−308 | ±1.7×10^308 | 8 bytes (finite values only; NaN/Infinity rejected) |

## String & Binary Limits

| Limit | Value | Notes |
|---|---|---|
| VARCHAR(n) max n | 4,294,967,295 (u32) | Practical max is ~4,048 bytes due to page capacity |
| VARBINARY(n) max n | 4,294,967,295 (u32) | Practical max is ~4,048 bytes due to page capacity |
| TEXT max size | ~4,048 bytes | Same as VARCHAR; limited by page capacity |
| VARCHAR(n) length check | Byte-based | `VARCHAR(100)` allows up to 100 *bytes*, not characters |

> **Note:** MuroDB checks `VARCHAR(n)` against *byte length*, not character count.
> This differs from MySQL, where `VARCHAR(n)` limits the number of *characters*.
> Multi-byte UTF-8 characters (e.g., Japanese characters at 3 bytes each, emoji at 4 bytes)
> consume multiple bytes of the VARCHAR(n) budget.

## Internal Limits

| Limit | Value | Notes |
|---|---|---|
| B-tree max depth | 64 | Exceeded depth indicates corruption |
| WAL max frame length | 5,120 bytes | |
| FTS inline segment limit | 3,000 bytes | Larger posting lists use overflow pages |
| FTS max payload | 65,536 bytes | |
| LRU cache default size | 256 pages | Configurable |

## NULL Behavior

- Primary key columns cannot be NULL
- `NULL = NULL` evaluates to UNKNOWN (not TRUE); use `IS NULL` instead
- Aggregate functions (SUM, AVG, MIN, MAX) skip NULL values
- `COUNT(*)` counts all rows; `COUNT(column)` counts non-NULL values only
- Multiple NULL values are allowed in UNIQUE indexes (SQL standard)
- NULL values in ORDER BY are grouped together
