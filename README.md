# MuroDB

Encrypted embedded SQL database with B-Tree + Full-Text Search (Bigram), written in Rust.

## Features

- **Transparent encryption** - AES-256-GCM-SIV (nonce-misuse resistant) for all pages and WAL
- **B-tree storage** - PRIMARY KEY (TINYINT/SMALLINT/INT/BIGINT), UNIQUE indexes (single column)
- **Full-text search** - Bigram (n=2) with NFKC normalization
  - MySQL-style `MATCH(col) AGAINST(...)` syntax
  - NATURAL LANGUAGE MODE with BM25 scoring
  - BOOLEAN MODE with `+term`, `-term`, `"phrase"` operators
  - `fts_snippet()` for highlighted excerpts
- **ACID transactions** - WAL-based crash recovery
- **Concurrency** - Multiple readers / single writer (thread RwLock + process file lock)
- **Single file** - Database file + WAL file

## Install

```bash
cargo install --path .
```

## Usage

```bash
# Create a new database
murodb mydb.db --create -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"

# Insert data
murodb mydb.db -e "INSERT INTO t (id, name) VALUES (1, 'hello')"

# Query
murodb mydb.db -e "SELECT * FROM t"

# Show tables
murodb mydb.db -e "SHOW TABLES"

# Interactive REPL
murodb mydb.db
```

Options:
- `-e <SQL>` — Execute SQL and exit
- `--create` — Create a new database
- `--password <PW>` — Password (prompts if omitted)

## Components

| Component | Description |
|---|---|
| `crypto/` | AES-256-GCM-SIV page encryption, Argon2 KDF, HMAC-SHA256 term blinding |
| `storage/` | 4096B slotted pages, encrypted pager with LRU cache, freelist |
| `btree/` | Insert/split, delete, search, scan, order-preserving key encoding |
| `wal/` | Encrypted WAL records, writer/reader, crash recovery |
| `tx/` | Transaction with dirty page buffer, commit/rollback |
| `schema/` | System catalog, table/index definitions |
| `sql/` | Hand-written lexer/parser, AST, rule-based planner, executor |
| `fts/` | Bigram tokenizer, delta+varint postings, BM25, NATURAL/BOOLEAN queries, snippets |
| `concurrency/` | parking_lot RwLock + fs4 file lock |

## SQL Surface

### Types

| Type | Storage | Range |
|------|---------|-------|
| TINYINT | 1 byte | -128 to 127 |
| SMALLINT | 2 bytes | -32,768 to 32,767 |
| INT | 4 bytes | -2,147,483,648 to 2,147,483,647 |
| BIGINT | 8 bytes | -2^63 to 2^63-1 |
| VARCHAR(n) | variable | max n bytes (optional) |
| TEXT | variable | unbounded text |
| VARBINARY(n) | variable | max n bytes (optional) |
| NULL | 0 bytes | null value |

### DDL

```sql
CREATE TABLE t (
  id BIGINT PRIMARY KEY,
  body VARCHAR,
  blob VARBINARY,
  uniq VARCHAR UNIQUE
);

CREATE UNIQUE INDEX idx_email ON users(email);

CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc');
```

### DML

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM t WHERE id = 42 ORDER BY id DESC LIMIT 10;

UPDATE t SET name = 'Alicia' WHERE id = 1;

DELETE FROM t WHERE id = 1;
```

### Full-Text Search

```sql
-- NATURAL LANGUAGE MODE (BM25 ranking)
SELECT id, MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) AS score
FROM t
WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0
ORDER BY score DESC
LIMIT 20;

-- BOOLEAN MODE (phrase / +/-)
SELECT id
FROM t
WHERE MATCH(body) AGAINST('"東京タワー" +夜景 -混雑' IN BOOLEAN MODE) > 0;

-- Snippet with highlight
SELECT id,
  fts_snippet(body, '"東京タワー"', '<mark>', '</mark>', 30) AS snippet
FROM t
WHERE MATCH(body) AGAINST('"東京タワー"' IN BOOLEAN MODE) > 0
LIMIT 10;
```

## Architecture

### Storage

- **Page size**: 4096 bytes (slotted page layout)
- **Encryption**: Each page encrypted with AES-256-GCM-SIV, AAD = (page_id, epoch)
- **Cache**: LRU page cache (default 256 pages)

### B-tree

- Key encoding: Integer types (big-endian + sign flip for order preservation), VARCHAR/VARBINARY (raw bytes)
- Clustered by PRIMARY KEY
- Secondary indexes share the same B-tree implementation

### FTS

- Tokenization: NFKC normalization + bigram (n=2)
- Term IDs: HMAC-SHA256 blinded (no plaintext tokens on disk)
- Postings: delta + varint compressed, stored in B-tree
- Scoring: BM25
- Phrase matching: consecutive bigram position verification
- Snippet: local scan approach (Option B)

### WAL & Recovery

- Records: BEGIN, PAGE_PUT, META_UPDATE, COMMIT, ABORT
- Recovery: replay committed transactions, discard uncommitted
- All WAL records encrypted

### Formal Verification

- TLA+ model for crash/recovery invariants: `specs/tla/CrashResilience.tla`
- TLC config: `specs/tla/CrashResilience.cfg`
- Runner script: `specs/tla/run_tlc.sh`
- Make targets: `make tlc-tools`, `make tlc`, `make tlc-large`
- Notes and scope: `specs/tla/README.md`

### Concurrency

- Thread-level: `parking_lot::RwLock`
- Process-level: `fs4` file lock
- Model: multiple readers, single writer

## Dependencies

| Crate | Purpose |
|---|---|
| `aes-gcm-siv` | AEAD encryption |
| `argon2` | Passphrase KDF |
| `hmac` + `sha2` | FTS term ID blinding |
| `nom` | SQL lexer |
| `unicode-normalization` | NFKC normalization |
| `parking_lot` | RwLock |
| `fs4` | File lock |
| `lru` | Page cache |
| `rand` | Nonce generation |
| `thiserror` | Error types |

## Non-goals

- Network server protocol
- Full access-pattern obfuscation (ORAM, etc.)
- Stored procedures / triggers

## Roadmap

### Implemented

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

### Phase 1 — SQL Essentials

Basic operators and DDL to make daily use practical.

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

### Phase 2 — Built-in Functions

MySQL-compatible scalar functions.

- [ ] String: LENGTH, CHAR_LENGTH, CONCAT, SUBSTRING/SUBSTR, UPPER, LOWER
- [ ] String: TRIM, LTRIM, RTRIM, REPLACE, REVERSE, REPEAT
- [ ] String: LEFT, RIGHT, LPAD, RPAD, INSTR/LOCATE
- [ ] String: REGEXP / REGEXP_LIKE
- [ ] Numeric: ABS, CEIL/CEILING, FLOOR, ROUND, MOD, POWER/POW
- [ ] NULL handling: COALESCE, IFNULL, NULLIF, IF
- [ ] Type conversion: CAST(expr AS type)
- [ ] CASE WHEN ... THEN ... ELSE ... END

### Phase 3 — Aggregation & Grouping

- [ ] COUNT, SUM, AVG, MIN, MAX
- [ ] COUNT(DISTINCT ...)
- [ ] GROUP BY
- [ ] HAVING
- [ ] SELECT DISTINCT

### Phase 4 — Schema Evolution

- [ ] ALTER TABLE ADD COLUMN
- [ ] ALTER TABLE DROP COLUMN
- [ ] ALTER TABLE MODIFY COLUMN / CHANGE COLUMN
- [ ] RENAME TABLE
- [ ] Composite PRIMARY KEY
- [ ] Composite UNIQUE / composite INDEX

### Phase 5 — Advanced Query

- [ ] Subqueries (WHERE col IN (SELECT ...), scalar subquery)
- [ ] UNION / UNION ALL
- [ ] EXISTS / NOT EXISTS
- [ ] INSERT ... ON DUPLICATE KEY UPDATE
- [ ] REPLACE INTO
- [ ] EXPLAIN (query plan display)
- [ ] RIGHT JOIN

### Phase 6 — Types & Storage

- [ ] FLOAT / DOUBLE
- [ ] DATE, DATETIME, TIMESTAMP
- [ ] Date/time functions: NOW, CURRENT_TIMESTAMP, DATE_FORMAT, etc.
- [ ] BLOB
- [ ] Overflow pages (posting list > 4096B)

### Phase 7 — Performance & Internals

- [ ] Auto-checkpoint (threshold-based WAL)
- [ ] Composite index range scan
- [ ] Query optimizer improvements (cost-based)
- [ ] FTS stop-ngram filtering
- [ ] fts_snippet acceleration (pos-to-offset map)

### Phase 8 — Security (Future)

- [ ] Key rotation (epoch-based re-encryption)
- [ ] Collation support (Japanese sort order, etc.)

## License

MIT License. See [LICENSE](LICENSE) for details.
