# SQL Reference

## Data Types

| Type | Storage | Range |
|------|---------|-------|
| TINYINT | 1 byte | -128 to 127 |
| SMALLINT | 2 bytes | -32,768 to 32,767 |
| INT | 4 bytes | -2,147,483,648 to 2,147,483,647 |
| BIGINT | 8 bytes | -2^63 to 2^63-1 |
| BOOLEAN | 1 byte | Alias for TINYINT |
| DATE | 4 bytes | `YYYY-MM-DD` |
| DATETIME | 8 bytes | `YYYY-MM-DD HH:MM:SS` |
| TIMESTAMP | 8 bytes | `YYYY-MM-DD HH:MM:SS` (timezone-aware input, normalized to UTC) |
| VARCHAR(n) | variable | max n bytes (optional) |
| TEXT | variable | unbounded text |
| JSONB | variable | Canonical JSON text (validated on write) |
| VARBINARY(n) | variable | max n bytes (optional) |
| FLOAT | 4 bytes | Single-precision IEEE 754 |
| DOUBLE | 8 bytes | Double-precision IEEE 754 |
| DECIMAL(p,s) | 16 bytes | Fixed-point exact numeric (precision 1-28, scale 0-p). Alias: NUMERIC(p,s). Default: DECIMAL(10,0) |
| UUID | 16 bytes | 128-bit UUID (RFC 9562), stored as fixed-length binary |
| NULL | 0 bytes | null value |

Temporal semantics:
- `DATE` stores calendar date only.
- `DATETIME` stores date-time as provided (no timezone conversion).
- `TIMESTAMP` accepts timezone offsets in string input (for example `+09:00`, `Z`) and stores UTC-normalized value.
- Invalid calendar/time values are rejected.

## DDL (Data Definition Language)

### CREATE TABLE

```sql
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  body VARCHAR,
  blob VARBINARY,
  uniq VARCHAR UNIQUE
);

-- With additional features
CREATE TABLE users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR NOT NULL,
  email VARCHAR UNIQUE,
  age INT DEFAULT 0,
  active BOOLEAN DEFAULT 1,
  CONSTRAINT chk_age CHECK (age >= 0)
);

-- IF NOT EXISTS
CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY);

-- Composite PRIMARY KEY
CREATE TABLE orders (
  user_id INT,
  order_id INT,
  amount INT,
  PRIMARY KEY (user_id, order_id)
);

-- Composite UNIQUE constraint
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  a INT,
  b INT,
  UNIQUE (a, b)
);

-- FOREIGN KEY (default: RESTRICT)
CREATE TABLE children (
  id BIGINT PRIMARY KEY,
  parent_id BIGINT,
  FOREIGN KEY (parent_id) REFERENCES parents(id)
);

-- FOREIGN KEY with actions
CREATE TABLE child_actions (
  id BIGINT PRIMARY KEY,
  parent_id BIGINT,
  FOREIGN KEY (parent_id) REFERENCES parents(id)
    ON DELETE CASCADE
    ON UPDATE SET NULL
);
```

### CREATE INDEX

```sql
CREATE UNIQUE INDEX idx_email ON users(email);

-- IF NOT EXISTS
CREATE INDEX IF NOT EXISTS idx_name ON users(name);

-- Composite index (multiple columns)
CREATE INDEX idx_ab ON t(a, b);
CREATE UNIQUE INDEX idx_ab ON t(a, b);
```

### CREATE FULLTEXT INDEX

```sql
CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc', stop_filter=off, stop_df_ratio_ppm=200000);
```

`FULLTEXT` is usable with any primary-key type. Internally, MuroDB maintains a separate FTS `doc_id`.
`stop_filter` supports `on`/`off` (quoted or unquoted), `1`/`0`, and `true`/`false`.
`stop_df_ratio_ppm` range is `0..=1000000`.

### DROP TABLE / DROP INDEX

```sql
DROP TABLE t;
DROP TABLE IF EXISTS t;
DROP INDEX idx_email;
DROP INDEX IF EXISTS idx_email;
```

### ALTER TABLE

```sql
-- Add a new column (O(1), no row rewrite)
ALTER TABLE t ADD COLUMN email VARCHAR;
ALTER TABLE t ADD age INT DEFAULT 0;

-- Drop a column (full table rewrite)
ALTER TABLE t DROP COLUMN age;

-- Modify column type or constraints (full rewrite if type changes)
ALTER TABLE t MODIFY COLUMN name VARCHAR(255) NOT NULL;
ALTER TABLE t MODIFY name TEXT;

-- Rename and optionally change a column (CHANGE COLUMN)
ALTER TABLE t CHANGE COLUMN name username VARCHAR;

-- Add / drop FOREIGN KEY
ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent(id);
ALTER TABLE child DROP FOREIGN KEY (parent_id);
```

**Performance notes:**
- `ADD COLUMN` is O(1) — only updates the catalog. Existing rows return the default value (or NULL) for the new column without rewriting data.
- `DROP COLUMN`, `MODIFY COLUMN` (with type change), and `CHANGE COLUMN` (with type change) perform a full table rewrite.
- `MODIFY COLUMN` / `CHANGE COLUMN` without a type change is catalog-only (O(1)).

**Behavior details:**
- `ADD COLUMN ... NOT NULL` without `DEFAULT` fails if the table already has rows.
- `ADD COLUMN ... UNIQUE` creates an automatic unique index (`auto_unique_<table>_<column>`).
- `ADD COLUMN ... UNIQUE` with a non-`NULL` default fails for multi-row existing tables, because all rows would backfill to the same value.
- `MODIFY COLUMN` / `CHANGE COLUMN` that adds `NOT NULL` validates existing rows and fails if `NULL` values are present.
- `MODIFY COLUMN` / `CHANGE COLUMN` with a type change rewrites all rows and coerces values; conversion failures abort the statement.
- `CHANGE COLUMN` updates index metadata to the new column name when indexes reference the old name.
- `MODIFY COLUMN` / `CHANGE COLUMN` reconcile single-column `UNIQUE`: adding `UNIQUE` may create an index; removing `UNIQUE` drops the corresponding auto unique index.
- `ADD FOREIGN KEY` validates existing rows; if orphan rows exist, it fails.
- FK actions support `RESTRICT`, `CASCADE`, and `SET NULL` for both `ON DELETE` and `ON UPDATE`.

**Limitations:**
- Cannot add a PRIMARY KEY column via ALTER TABLE.
- Cannot drop a PRIMARY KEY column.
- Cannot drop a column that has an index on it (drop the index first).
- Cannot drop a table that is referenced by a foreign key.
- `DROP FOREIGN KEY` is specified by child column list: `DROP FOREIGN KEY (col1, col2)`.

### RENAME TABLE

```sql
RENAME TABLE old_name TO new_name;
```

Renames a table. All indexes are automatically updated. No row data is rewritten.

### Schema Inspection

```sql
SHOW TABLES;
SHOW CREATE TABLE t;
DESCRIBE t;
DESC t;
```

### Operational Inspection

```sql
SHOW CHECKPOINT STATS;
SHOW DATABASE STATS;
```

Both commands return two columns: `stat` and `value`.

`SHOW DATABASE STATS` includes cache observability fields:
- `pager_cache_hits`
- `pager_cache_misses`
- `pager_cache_hit_rate_pct`

It also exposes checkpoint policy/runtime fields:
- `deferred_checkpoints`
- `checkpoint_pending_ops`
- `checkpoint_policy_tx_threshold`
- `checkpoint_policy_wal_bytes_threshold`
- `checkpoint_policy_interval_ms`

WAL observability:
- `wal_file_size_bytes`

### Runtime Configuration

Checkpoint policy can be changed at runtime (session scope):

```sql
SET checkpoint_tx_threshold = 8;
SET checkpoint_wal_bytes_threshold = 1048576;
SET checkpoint_interval_ms = 1000;
```

Supported runtime option names:
- `checkpoint_tx_threshold`
- `checkpoint_wal_bytes_threshold`
- `checkpoint_interval_ms`

Notes:
- Values must be non-negative integers.
- Scope is session-only (not persisted in the database file).
- `SET` runtime options are rejected inside explicit transactions (`BEGIN ... COMMIT/ROLLBACK`).
- Active values are observable via `SHOW DATABASE STATS` (`checkpoint_policy_*` fields).

## DML (Data Manipulation Language)

### ANALYZE TABLE

Refreshes persisted planner statistics.

```sql
ANALYZE TABLE t;
```

Current persisted stats include:

- table row count
- index distinct-key count

### INSERT

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice');

-- Multi-row insert
INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob');
```

### INSERT ... ON DUPLICATE KEY UPDATE

If a row with the same PRIMARY KEY already exists, updates the existing row instead of inserting a new one.

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice')
  ON DUPLICATE KEY UPDATE name = 'Alice Updated';

-- Expressions can reference existing column values
INSERT INTO counters (id, cnt) VALUES (1, 1)
  ON DUPLICATE KEY UPDATE cnt = cnt + 1;
```

**Affected rows (MySQL-compatible):**
- New row inserted: 1
- Existing row updated: 2

**Limitations:**
- `VALUES()` function is not supported. Use column references to access the existing row's values.

### REPLACE INTO

Inserts a new row. If a row with the same PRIMARY KEY or UNIQUE index value already exists, deletes the old row first, then inserts the new one.

```sql
REPLACE INTO t (id, name) VALUES (1, 'Alice');

-- Multi-row replace
REPLACE INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob');
```

Unlike `INSERT ... ON DUPLICATE KEY UPDATE`, `REPLACE` deletes and re-inserts the entire row. This means:
- All columns are replaced with the new values (columns not specified get defaults/NULL).
- Conflicts on any UNIQUE index (not just PRIMARY KEY) also trigger deletion of the conflicting row.

### SELECT

```sql
SELECT * FROM t WHERE id = 42 ORDER BY id DESC LIMIT 10;

-- With OFFSET
SELECT * FROM t LIMIT 10 OFFSET 20;
```

### UPDATE

```sql
UPDATE t SET name = 'Alicia' WHERE id = 1;
```

### DELETE

```sql
DELETE FROM t WHERE id = 1;
```

### Index Hints (FORCE INDEX / USE INDEX / IGNORE INDEX)

MySQL-compatible index hints allow controlling which indexes the query planner considers.

```sql
-- Force the planner to use only the specified index (skips PK seek)
SELECT * FROM t FORCE INDEX (idx_age) WHERE age = 20;

-- Suggest the planner to use the specified index (PK seek still allowed)
SELECT * FROM t USE INDEX (idx_age) WHERE age = 20;

-- Exclude the specified index from consideration
SELECT * FROM t IGNORE INDEX (idx_age) WHERE age = 20;

-- Multiple index names
SELECT * FROM t FORCE INDEX (idx_age, idx_name) WHERE age = 20;

-- Also works with UPDATE and DELETE
UPDATE t FORCE INDEX (idx_age) SET name = 'updated' WHERE age = 20;
DELETE FROM t IGNORE INDEX (idx_age) WHERE age = 20;
```

Behavior:
- **FORCE INDEX**: Only the specified indexes are candidates. Primary key seek is skipped. If the specified index cannot be used for the query, falls back to full table scan (matching MySQL behavior).
- **USE INDEX**: The specified indexes are preferred, but full table scan is also a candidate. Primary key seek is still allowed.
- **IGNORE INDEX**: The specified indexes are excluded from consideration. All other indexes and primary key seek remain available.

## WHERE Clause

### Comparison operators

```sql
WHERE id = 1
WHERE id != 1
WHERE id < 10
WHERE id > 5
WHERE id <= 10
WHERE id >= 5
```

### Logical operators

```sql
WHERE id > 1 AND name = 'Alice'
WHERE id = 1 OR id = 2
WHERE NOT (id = 1)
```

### LIKE / NOT LIKE

```sql
WHERE name LIKE 'Ali%'
WHERE name LIKE '_ob'
WHERE name NOT LIKE '%test%'
```

### IN

```sql
WHERE id IN (1, 2, 3)
```

### BETWEEN

```sql
WHERE id BETWEEN 1 AND 10
```

### IS NULL / IS NOT NULL

```sql
WHERE name IS NULL
WHERE name IS NOT NULL
```

## ORDER BY / LIMIT

```sql
SELECT * FROM t ORDER BY id ASC;
SELECT * FROM t ORDER BY name DESC, id ASC;
SELECT * FROM t LIMIT 10;
SELECT * FROM t LIMIT 10 OFFSET 5;
```

## Literals

### Hex Literal (Binary)

Binary data can be specified using the `X'...'` syntax (SQL standard / MySQL compatible):

```sql
-- Insert binary data
INSERT INTO t (id, data) VALUES (1, X'DEADBEEF');

-- Empty binary literal
INSERT INTO t (id, data) VALUES (2, X'');

-- Case-insensitive (both X and x are accepted)
INSERT INTO t (id, data) VALUES (3, x'cafebabe');

-- Use in WHERE clause
SELECT * FROM t WHERE data = X'DEADBEEF';
```

The hex string must contain an even number of hex digits (`0-9`, `A-F`, `a-f`).
Odd-length hex strings and invalid characters produce a parse error.

## Expressions

### Arithmetic operators

```sql
SELECT id, price * quantity AS total FROM orders;
SELECT id, (a + b) / 2 AS average FROM t;
-- Supported: +, -, *, /, %
```

## Built-in Functions

### String Functions

#### LENGTH(s)

Returns the byte length of a string.

```sql
SELECT LENGTH('hello');       -- 5
SELECT LENGTH('héllo');       -- 6 (é is 2 bytes in UTF-8)
```

#### CHAR_LENGTH(s) / CHARACTER_LENGTH(s)

Returns the character count of a string.

```sql
SELECT CHAR_LENGTH('hello');  -- 5
SELECT CHAR_LENGTH('héllo');  -- 5
```

#### CONCAT(s1, s2, ...)

Concatenates two or more strings. Returns NULL if any argument is NULL.

```sql
SELECT CONCAT('hello', ' ', 'world');  -- 'hello world'
SELECT CONCAT('a', NULL);              -- NULL
```

#### SUBSTRING(s, pos [, len]) / SUBSTR(s, pos [, len])

Returns a substring starting at position `pos` (1-based). Optional `len` limits the length.

```sql
SELECT SUBSTRING('hello world', 7);     -- 'world'
SELECT SUBSTRING('hello world', 1, 5);  -- 'hello'
SELECT SUBSTRING('hello', -3);          -- 'llo'
```

#### UPPER(s) / LOWER(s)

Converts a string to upper or lower case.

```sql
SELECT UPPER('hello');  -- 'HELLO'
SELECT LOWER('HELLO');  -- 'hello'
```

#### TRIM(s) / LTRIM(s) / RTRIM(s)

Removes whitespace from both ends, the left end, or the right end.

```sql
SELECT TRIM('  hello  ');   -- 'hello'
SELECT LTRIM('  hello  ');  -- 'hello  '
SELECT RTRIM('  hello  ');  -- '  hello'
```

#### REPLACE(s, from, to)

Replaces all occurrences of `from` with `to` in `s`.

```sql
SELECT REPLACE('hello world', 'world', 'rust');  -- 'hello rust'
```

#### REVERSE(s)

Reverses a string.

```sql
SELECT REVERSE('hello');  -- 'olleh'
```

#### REPEAT(s, n)

Repeats a string `n` times.

```sql
SELECT REPEAT('ab', 3);  -- 'ababab'
```

#### LEFT(s, n) / RIGHT(s, n)

Returns the leftmost or rightmost `n` characters.

```sql
SELECT LEFT('hello', 3);   -- 'hel'
SELECT RIGHT('hello', 3);  -- 'llo'
```

#### LPAD(s, len, pad) / RPAD(s, len, pad)

Pads a string to length `len` using `pad` characters on the left or right.

```sql
SELECT LPAD('hi', 5, '*');  -- '***hi'
SELECT RPAD('hi', 5, '*');  -- 'hi***'
```

#### INSTR(s, sub)

Returns the position (1-based) of the first occurrence of `sub` in `s`. Returns 0 if not found.

```sql
SELECT INSTR('hello world', 'world');  -- 7
SELECT INSTR('hello', 'xyz');          -- 0
```

#### LOCATE(sub, s [, pos])

Returns the position (1-based) of `sub` in `s`, starting the search at `pos`.

```sql
SELECT LOCATE('hello', 'hello hello');     -- 1
SELECT LOCATE('hello', 'hello hello', 2);  -- 7
```

### REGEXP

#### REGEXP / REGEXP_LIKE(s, pattern)

Tests whether a string matches a regular expression. Can be used as an operator or function.

```sql
-- Operator syntax
SELECT * FROM t WHERE name REGEXP '[0-9]+';

-- Function syntax
SELECT REGEXP_LIKE(name, '^hello') FROM t;
```

### Numeric Functions

All numeric functions support INTEGER, FLOAT, DOUBLE, and DECIMAL types.

#### ABS(n)

Returns the absolute value.

```sql
SELECT ABS(-42);    -- 42
SELECT ABS(-3.14);  -- 3.14 (DECIMAL)
```

#### CEIL(n) / CEILING(n) / FLOOR(n)

Returns the ceiling or floor. (Identity for integer types.)

```sql
SELECT CEIL(3.14);   -- 4
SELECT FLOOR(3.14);  -- 3
```

#### ROUND(n [, decimals])

Rounds a number to `decimals` decimal places (default 0). Works with DECIMAL for exact rounding.

```sql
SELECT ROUND(3.1459, 2);  -- 3.15 (DECIMAL)
SELECT ROUND(42);          -- 42
```

#### MOD(a, b)

Returns the modulo (same as `%` operator).

```sql
SELECT MOD(10, 3);  -- 1
```

#### POWER(base, exp) / POW(base, exp)

Returns `base` raised to the power of `exp`.

```sql
SELECT POWER(2, 10);  -- 1024
```

### UUID Functions

#### UUID_V4()

Generates a random UUID (version 4, RFC 9562).

```sql
SELECT UUID_V4();  -- e.g. '550e8400-e29b-41d4-a716-446655440000'
```

#### UUID_V7()

Generates a time-ordered UUID (version 7, RFC 9562). UUIDs generated later sort after earlier ones, making them suitable for primary keys with time-ordered insertion.

```sql
CREATE TABLE events (id UUID PRIMARY KEY, data VARCHAR);
INSERT INTO events VALUES (UUID_V7(), 'event data');
```

UUID values are displayed as lowercase hyphenated hex strings (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`). String literals in UUID format (with or without hyphens) are automatically parsed when inserted into UUID columns.

```sql
-- Both forms are accepted:
INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000', 'with hyphens');
INSERT INTO t VALUES ('550e8400e29b41d4a716446655440000', 'without hyphens');

-- Cast between UUID and VARCHAR/VARBINARY:
SELECT CAST(id AS VARCHAR) FROM t;
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
SELECT CAST(id AS VARBINARY) FROM t;  -- 16-byte binary
```

### Date/Time Functions

#### NOW() / CURRENT_TIMESTAMP[()]

Returns the current UTC datetime as a `DATETIME` value.

```sql
SELECT NOW();
SELECT CURRENT_TIMESTAMP();
SELECT CURRENT_TIMESTAMP; -- parentheses are optional
```

#### DATE_FORMAT(dt, format)

Formats a date/datetime/timestamp string/value using MySQL-style format specifiers.

```sql
SELECT DATE_FORMAT('2026-02-22 13:04:05', '%Y/%m/%d %H:%i:%s');
-- '2026/02/22 13:04:05'
```

Common specifiers:
- `%Y` year (4 digits), `%y` year (2 digits)
- `%m` month (01-12), `%c` month (1-12), `%M` month name, `%b` month abbreviation
- `%d` day (01-31), `%e` day (1-31)
- `%H` hour (00-23), `%h`/`%I` hour (01-12), `%i` minute, `%s` second
- `%W` weekday name, `%a` weekday abbreviation
- `%T` `HH:MM:SS`, `%r` 12-hour time with AM/PM, `%%` literal percent

### NULL Handling & Conditional

#### COALESCE(a, b, ...)

Returns the first non-NULL argument.

```sql
SELECT COALESCE(NULL, NULL, 'fallback');  -- 'fallback'
```

#### IFNULL(a, b)

Returns `a` if not NULL, otherwise `b`.

```sql
SELECT IFNULL(NULL, 'default');  -- 'default'
SELECT IFNULL('value', 'default');  -- 'value'
```

#### NULLIF(a, b)

Returns NULL if `a = b`, otherwise returns `a`.

```sql
SELECT NULLIF(0, 0);  -- NULL
SELECT NULLIF(5, 0);  -- 5
```

#### IF(cond, then, else)

Returns `then` if `cond` is truthy, otherwise `else`.

```sql
SELECT IF(1, 'yes', 'no');  -- 'yes'
SELECT IF(0, 'yes', 'no');  -- 'no'
```

### CASE WHEN

```sql
-- Searched CASE
SELECT CASE
  WHEN val < 10 THEN 'low'
  WHEN val < 20 THEN 'mid'
  ELSE 'high'
END FROM t;

-- Simple CASE
SELECT CASE status
  WHEN 1 THEN 'active'
  WHEN 2 THEN 'inactive'
  ELSE 'unknown'
END FROM t;
```

### CAST

Converts a value to a different data type.

```sql
SELECT CAST('42' AS INT);      -- 42
SELECT CAST(42 AS VARCHAR);    -- '42'
SELECT CAST(val AS BIGINT) FROM t;
```

Supported target types: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL(p,s), DATE, DATETIME, TIMESTAMP, VARCHAR, TEXT, JSONB, VARBINARY.

### JSON Functions

`JSONB` accepts valid JSON only. Values are canonicalized when stored.

```sql
CREATE TABLE docs (id BIGINT PRIMARY KEY, doc JSONB);
INSERT INTO docs VALUES (1, '{"a":{"b":[1,2,3]}}');
```

#### JSON_EXTRACT(json, path)

Evaluates `path` using `jsonpath_lib` JSONPath semantics and returns matched JSON (canonical text). If multiple values match, returns a JSON array.

```sql
SELECT JSON_EXTRACT('{"a":{"b":1}}', '$.a.b'); -- "1"
```

#### JSON_SET(json, path, value)

Sets `value` at `path` and returns updated JSON. Supported update-path syntax is root-based dot/index form (`$.key`, `$.arr[0]`, chained combinations).

```sql
SELECT JSON_SET('{"a":1}', '$.b', 2); -- {"a":1,"b":2}
```

#### JSON_REMOVE(json, path)

Removes value at `path` and returns updated JSON. Missing path is a no-op.

```sql
SELECT JSON_REMOVE('{"a":1,"b":2}', '$.a'); -- {"b":2}
```

#### JSON_TYPE(json)

Returns one of: `NULL`, `BOOLEAN`, `INTEGER`, `DOUBLE`, `STRING`, `ARRAY`, `OBJECT`.

```sql
SELECT JSON_TYPE('[1,2,3]'); -- ARRAY
```

#### JSON_CONTAINS(json, value_or_path)

- If second argument starts with `$`, it is evaluated as JSONPath (via `jsonpath_lib`); returns `1` when any match exists.
- Otherwise it is treated as a JSON candidate value and checked for containment.

```sql
SELECT JSON_CONTAINS('{"a":{"b":1}}', '$.a.b'); -- 1
SELECT JSON_CONTAINS('{"a":1,"b":2}', '{"b":2}'); -- 1
```

JSON function behavior:
- If any argument is `NULL`, returns `NULL`.
- Invalid JSON input returns an error.
- Invalid/unsupported update-path syntax in `JSON_SET`/`JSON_REMOVE` returns an error.

## Aggregation & GROUP BY

### Aggregate Functions

```sql
SELECT COUNT(*) FROM t;              -- count all rows
SELECT COUNT(col) FROM t;            -- count non-NULL values
SELECT COUNT(DISTINCT col) FROM t;   -- count distinct non-NULL values
SELECT SUM(amount) FROM orders;      -- sum (skips NULLs)
SELECT AVG(amount) FROM orders;      -- average (integer for integer inputs, float otherwise)
SELECT MIN(amount) FROM orders;      -- minimum (skips NULLs)
SELECT MAX(amount) FROM orders;      -- maximum (skips NULLs)
```

**NULL semantics (SQL standard):**
- `COUNT(*)` counts all rows including NULLs
- `COUNT(col)` counts non-NULL values only
- `SUM`, `AVG`, `MIN`, `MAX` skip NULLs; return NULL if all values are NULL
- On empty tables: `COUNT` returns 0, others return NULL

### GROUP BY

```sql
SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category;
SELECT category, status, SUM(amount) FROM orders GROUP BY category, status;
```

NULLs in GROUP BY columns form their own group.

### HAVING

Filters groups after aggregation (use WHERE to filter rows before aggregation).

```sql
SELECT category, COUNT(*) AS cnt
FROM orders
GROUP BY category
HAVING COUNT(*) > 2;
```

### SELECT DISTINCT

```sql
SELECT DISTINCT category FROM orders;
SELECT DISTINCT category, status FROM orders;
```

## Subqueries

Uncorrelated subqueries are supported in WHERE clauses and SELECT lists.

### IN / NOT IN (SELECT ...)

```sql
-- Find users who have placed orders
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);

-- Find users who have NOT placed orders
SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders);
```

### EXISTS / NOT EXISTS

```sql
-- Check if any orders exist (uncorrelated)
SELECT * FROM users WHERE EXISTS (SELECT id FROM orders);

-- Check if no orders exist
SELECT * FROM users WHERE NOT EXISTS (SELECT id FROM orders WHERE amount > 1000);
```

### Scalar Subqueries

A scalar subquery returns exactly one column and at most one row. If it returns zero rows, the result is NULL. If it returns more than one row, an error is raised.

```sql
-- Scalar subquery in SELECT list
SELECT id, (SELECT MAX(amount) FROM orders) AS max_order FROM users;

-- Scalar subquery in WHERE
SELECT * FROM users WHERE id = (SELECT MIN(user_id) FROM orders);
```

### Nested Subqueries

Subqueries can be nested:

```sql
SELECT * FROM t1 WHERE id IN (
  SELECT id FROM t2 WHERE EXISTS (SELECT id FROM t3)
);
```

**Limitations:**
- Only uncorrelated subqueries (no outer row references).
- Subqueries are pre-materialized once per query (not per row).

## JOIN

```sql
-- INNER JOIN
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id;

-- LEFT JOIN
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id;

-- RIGHT JOIN
SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.t1_id;

-- CROSS JOIN
SELECT * FROM t1 CROSS JOIN t2;

-- Table aliases
SELECT a.id, b.name FROM t1 AS a JOIN t2 AS b ON a.id = b.t1_id;
```

## UNION / UNION ALL

Combines results from multiple SELECT statements.

```sql
-- UNION (removes duplicates)
SELECT id, name FROM t1 UNION SELECT id, name FROM t2;

-- UNION ALL (keeps duplicates)
SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2;

-- With ORDER BY and LIMIT (applies to the whole result)
SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id LIMIT 10;
```

All SELECT statements in a UNION must return the same number of columns.

## EXPLAIN

Shows the optimizer's chosen access path and cardinality/cost estimates for a statement.

```sql
EXPLAIN SELECT * FROM t WHERE id = 1;
EXPLAIN UPDATE t SET name = 'Alicia' WHERE id = 1;
EXPLAIN DELETE FROM t WHERE id = 1;
```

### Output Columns

| Column | Description |
|--------|-------------|
| id | Always `1` (single plan row output) |
| select_type | `SIMPLE`, `UPDATE`, or `DELETE` |
| table | Base table name |
| type | Access type: `const` (PK lookup), `ref` (index lookup), `range` (index range seek), `ALL` (full scan), `fulltext` (FTS) |
| key | Index used (NULL for full scan) |
| rows | Estimated candidate rows for the chosen access path |
| cost | Heuristic cost of the chosen plan |
| Extra | Additional diagnostics (`Using where`, `Using index`, `Using fulltext`, JOIN loop notes, etc.) |

### Access Type Meanings

- `const`: primary-key equality lookup (`WHERE pk = ...`).
- `ref`: secondary index equality lookup.
- `range`: index range scan (single/composite range shape).
- `ALL`: full table scan.
- `fulltext`: FULLTEXT index path.

### How `rows` Is Estimated

- If table/index stats are present (`ANALYZE TABLE`), EXPLAIN uses:
  - table row stats,
  - index distinct-key stats,
  - numeric min/max bounds,
  - numeric histograms (single-column numeric B-tree indexes).
- If stats are missing, EXPLAIN falls back to conservative heuristics (or table row scan fallback where applicable).

### How `cost` Is Estimated

- `cost` is a deterministic heuristic score used for plan comparison.
- Lower is better.
- It includes access-path cost and, for JOIN planning diagnostics, nested-loop alternative comparison cost.
- Compare `cost` values primarily within the same query shape.

### JOIN Diagnostics in `Extra`

For `EXPLAIN SELECT ... JOIN ...`, `Extra` can include join-loop notes such as:

```text
Join loops: j1=right_outer (L=20,R=3,cL=620,cR=603)
```

- `j1` = first JOIN step.
- `left_outer` / `right_outer` = chosen outer loop side.
- `L` / `R` = estimated left/right input rows at that step.
- `cL` / `cR` = compared heuristic costs for each outer-loop alternative.

### Practical Workflow

```sql
-- 1) inspect plan
EXPLAIN SELECT * FROM t WHERE a >= 100 AND a <= 110;

-- 2) refresh stats after major data changes
ANALYZE TABLE t;

-- 3) inspect again (rows/cost should better reflect current data)
EXPLAIN SELECT * FROM t WHERE a >= 100 AND a <= 110;
```

### Current Scope and Limits

**Limitations:**
- Supported targets are `SELECT`, `UPDATE`, and `DELETE`.
- Output is currently a single-row summary (not a full operator tree).
- JOIN/subquery internals are summarized in `Extra` rather than emitted as multiple plan rows.

## Rekey (Password Rotation)

Password rotation is not available as SQL syntax.
Use API or dedicated CLI command instead:

- Rust API: `Database::rekey_with_password("new_password")`
- CLI: `murodb-rekey <db-file>`

## Transactions

```sql
BEGIN;
INSERT INTO t (id, name) VALUES (1, 'Alice');
INSERT INTO t (id, name) VALUES (2, 'Bob');
COMMIT;

-- Or rollback
BEGIN;
INSERT INTO t (id, name) VALUES (3, 'Charlie');
ROLLBACK;
```

Rust API note:
- `Database::query()` accepts read-only SQL only.
- `Database::query()` takes `&mut self` because read execution may refresh pager/catalog state from disk before running.
- For concurrent reads in one process, use multiple read-only handles (for example `Database::open_reader()`).
- Inside an explicit transaction (`BEGIN` ... `COMMIT`/`ROLLBACK`), run statements through `Database::execute()`, including `SELECT`.

## Hidden _rowid

Tables without an explicit PRIMARY KEY automatically get a hidden `_rowid` column with auto-generated values.
