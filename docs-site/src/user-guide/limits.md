# Limits Reference

This page documents the known limits of MuroDB. These limits arise from the fixed page size,
serialization formats, and design decisions of the storage engine.

## Page & Row Limits

| Limit | Value | Notes |
|---|---|---|
| Page size | 4,096 bytes | Fixed; all data pages, B-tree nodes, and catalog entries use this size |
| Page header | 14 bytes | page_id (8) + cell_count (2) + free_start (2) + free_end (2) |
| Max inline row size | ~4,073 bytes | Rows within this limit are stored inline in a single page |
| Max row size (with overflow) | ~4 GB | Limited by u32 total_value_len; values exceeding inline limit use overflow pages |
| Max cell payload | ~4,073 bytes | 4,096 − 14 (header) − 5 (node header cell) − 4 (cell pointer + length prefix) |
| Overflow chunk size | 4,077 bytes | Per overflow page: 4,096 − 19 bytes header |

Rows with values that exceed the inline page capacity automatically use **overflow pages**.
The value is stored in a chain of overflow pages, with the leaf cell containing only the key
and a pointer to the first overflow page. Keys must still fit inline (max ~4,071 bytes).

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
| VARCHAR(n) max n | 4,294,967,295 (u32) | Values exceeding ~4,073 bytes use overflow pages |
| VARBINARY(n) max n | 4,294,967,295 (u32) | Values exceeding ~4,073 bytes use overflow pages |
| TEXT max size | ~4 GB | Limited by u32 value length; large values use overflow pages |
| VARCHAR(n) length check | Character-based | `VARCHAR(100)` allows up to 100 *characters* (MySQL-compatible) |

> **Note:** MuroDB checks `VARCHAR(n)` against *character count*, consistent with MySQL.
> Multi-byte UTF-8 characters (e.g., Japanese characters at 3 bytes each, emoji at 4 bytes)
> each count as one character. `VARBINARY(n)` still uses byte-based length checking.

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
