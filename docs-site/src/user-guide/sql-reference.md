# SQL Reference

## Data Types

| Type | Storage | Range |
|------|---------|-------|
| TINYINT | 1 byte | -128 to 127 |
| SMALLINT | 2 bytes | -32,768 to 32,767 |
| INT | 4 bytes | -2,147,483,648 to 2,147,483,647 |
| BIGINT | 8 bytes | -2^63 to 2^63-1 |
| BOOLEAN | 1 byte | Alias for TINYINT |
| VARCHAR(n) | variable | max n bytes (optional) |
| TEXT | variable | unbounded text |
| VARBINARY(n) | variable | max n bytes (optional) |
| NULL | 0 bytes | null value |

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
  OPTIONS (n=2, normalize='nfkc');
```

**Current status:**
- `CREATE FULLTEXT INDEX` syntax is parsed, but SQL-engine integration is not complete in the current release.
- See [Full-Text Search](full-text-search.md) for the currently supported Rust API workflow.

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
```

**Performance notes:**
- `ADD COLUMN` is O(1) — only updates the catalog. Existing rows return the default value (or NULL) for the new column without rewriting data.
- `DROP COLUMN`, `MODIFY COLUMN` (with type change), and `CHANGE COLUMN` (with type change) perform a full table rewrite.
- `MODIFY COLUMN` / `CHANGE COLUMN` without a type change is catalog-only (O(1)).

**Limitations:**
- Cannot add a PRIMARY KEY column via ALTER TABLE.
- Cannot drop a PRIMARY KEY column.
- Cannot drop a column that has an index on it (drop the index first).

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

## DML (Data Manipulation Language)

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

#### ABS(n)

Returns the absolute value.

```sql
SELECT ABS(-42);  -- 42
```

#### CEIL(n) / CEILING(n) / FLOOR(n)

Returns the ceiling or floor. (Identity for integer types.)

```sql
SELECT CEIL(42);   -- 42
SELECT FLOOR(42);  -- 42
```

#### ROUND(n [, decimals])

Rounds a number. (Identity for integer types.)

```sql
SELECT ROUND(42);  -- 42
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

Supported target types: TINYINT, SMALLINT, INT, BIGINT, VARCHAR, TEXT, VARBINARY.

## Aggregation & GROUP BY

### Aggregate Functions

```sql
SELECT COUNT(*) FROM t;              -- count all rows
SELECT COUNT(col) FROM t;            -- count non-NULL values
SELECT COUNT(DISTINCT col) FROM t;   -- count distinct non-NULL values
SELECT SUM(amount) FROM orders;      -- sum (skips NULLs)
SELECT AVG(amount) FROM orders;      -- average (integer division, skips NULLs)
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

Shows the query execution plan for a SELECT statement.

```sql
EXPLAIN SELECT * FROM t WHERE id = 1;
```

Output columns:

| Column | Description |
|--------|-------------|
| id | Always 1 (single-table queries) |
| select_type | Always "SIMPLE" |
| table | Table name |
| type | Access type: `const` (PK lookup), `ref` (index lookup), `ALL` (full scan), `fulltext` (FTS) |
| key | Index used (NULL for full scan) |
| Extra | Additional info: "Using where", "Using index", "Using fulltext" |

**Limitations:**
- Only SELECT statements are supported.
- JOIN and subquery queries show only one row (the primary table's plan).

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

## Hidden _rowid

Tables without an explicit PRIMARY KEY automatically get a hidden `_rowid` column with auto-generated values.
