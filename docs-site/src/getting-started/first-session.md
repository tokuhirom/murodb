# First Session (10 Minutes)

This page is a fast path for SQL users who want to feel MuroDB before reading full details.

## 1. Install

```bash
cargo install --path .
```

## 2. Create a database and schema

```bash
murodb demo.db --create -e "CREATE TABLE notes (
  id BIGINT PRIMARY KEY,
  title VARCHAR NOT NULL,
  body TEXT
)"
```

Encrypted mode prompts for a password by default.

If you need plaintext mode, opt in explicitly:

```bash
murodb demo-plain.db --create --encryption off -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

## 3. Insert and query rows

```bash
murodb demo.db -e "INSERT INTO notes (id, title, body) VALUES
  (1, 'welcome', 'hello from murodb'),
  (2, 'next', 'test full text search')"

murodb demo.db -e "SELECT id, title FROM notes ORDER BY id"
```

## 4. Add full-text search

```bash
murodb demo.db -e "CREATE FULLTEXT INDEX notes_body_fts ON notes(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc', stop_filter=off, stop_df_ratio_ppm=200000)"

murodb demo.db -e "SELECT id,
  MATCH(body) AGAINST('full text' IN NATURAL LANGUAGE MODE) AS score
FROM notes
WHERE MATCH(body) AGAINST('full text' IN NATURAL LANGUAGE MODE) > 0
ORDER BY score DESC"
```

## 5. Open interactive REPL

```bash
murodb demo.db
```

Start without `-e` to enter REPL mode.

## Next

- [Quick Start](quick-start.md) for concise command examples.
- [SQL Reference](../user-guide/sql-reference.md) for statement details.
- [Recovery](../user-guide/recovery.md) for durability and incident handling.
