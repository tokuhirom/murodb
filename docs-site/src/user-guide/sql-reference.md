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
```

### CREATE INDEX

```sql
CREATE UNIQUE INDEX idx_email ON users(email);

-- IF NOT EXISTS
CREATE INDEX IF NOT EXISTS idx_name ON users(name);
```

### CREATE FULLTEXT INDEX

```sql
CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc');
```

### DROP TABLE / DROP INDEX

```sql
DROP TABLE t;
DROP TABLE IF EXISTS t;
DROP INDEX idx_email;
```

### Schema Inspection

```sql
SHOW TABLES;
SHOW CREATE TABLE t;
DESCRIBE t;
DESC t;
```

## DML (Data Manipulation Language)

### INSERT

```sql
INSERT INTO t (id, name) VALUES (1, 'Alice');

-- Multi-row insert
INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob');
```

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

## JOIN

```sql
-- INNER JOIN
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id;

-- LEFT JOIN
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id;

-- CROSS JOIN
SELECT * FROM t1 CROSS JOIN t2;

-- Table aliases
SELECT a.id, b.name FROM t1 AS a JOIN t2 AS b ON a.id = b.t1_id;
```

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
