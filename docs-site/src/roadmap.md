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

## Phase 6 — Types & Storage

- [ ] FLOAT / DOUBLE
- [ ] DATE, DATETIME, TIMESTAMP
- [ ] Date/time functions: NOW, CURRENT_TIMESTAMP, DATE_FORMAT, etc.
- [ ] BLOB
- [ ] Overflow pages (posting list > 4096B)

## Phase 7 — Performance & Internals

- [ ] Auto-checkpoint (threshold-based WAL)
- [ ] Composite index range scan
- [ ] Query optimizer improvements (cost-based)
- [ ] FTS stop-ngram filtering
- [ ] fts_snippet acceleration (pos-to-offset map)

## Phase 8 — Security (Future)

- [ ] Key rotation (epoch-based re-encryption)
- [ ] Collation support (Japanese sort order, etc.)
