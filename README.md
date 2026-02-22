# MuroDB

Embedded SQL database written in Rust.

## Why Try MuroDB

- SQL-first workflow: create tables, insert rows, query immediately from CLI.
- Safe-by-default storage: encrypted pages + WAL crash recovery.
- Single-process simplicity: embed locally, no server setup required.
- Built-in full-text search: bigram index + relevance scoring.

## Status

MuroDB is under active development.

Current core capabilities:

- Storage engine with WAL-based durability
- Pluggable at-rest mode: `aes256-gcm-siv` (default) or explicit `off` (plaintext)
- Practical SQL subset for local application use

## Install

```bash
cargo install --path .
```

## Two-Minute Run

```bash
# 1) Create a database and table (password prompt appears in encrypted mode)
murodb mydb.db --create -e "CREATE TABLE notes (id BIGINT PRIMARY KEY, title VARCHAR, body TEXT)"

# 2) Insert rows
murodb mydb.db -e "INSERT INTO notes (id, title, body) VALUES
  (1, 'first', 'hello murodb'),
  (2, 'todo', 'ship docs')"

# 3) Query rows
murodb mydb.db -e "SELECT id, title FROM notes ORDER BY id"

# 4) Open REPL
murodb mydb.db
```

If you need plaintext mode, opt in explicitly:

```bash
murodb mydb_plain.db --create --encryption off -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"
```

## Documentation

The detailed manual and internals docs are in `docs-site/`.

- Book source: `docs-site/src/`
- Build locally:

```bash
mdbook build docs-site
```

Recommended reading order:

- `docs-site/src/getting-started/first-session.md`
- `docs-site/src/getting-started/quick-start.md`
- `docs-site/src/user-guide/sql-reference.md`
- `docs-site/src/user-guide/full-text-search.md`
- `docs-site/src/user-guide/recovery.md`

Internals deep dive:

- `docs-site/src/internals/reading-guide.md`
- `docs-site/src/internals/files-and-locking.md`
- `docs-site/src/internals/btree.md`
- `docs-site/src/internals/query-planning.md`
- `docs-site/src/internals/cryptography.md`
- `docs-site/src/internals/wal.md`

## API Notes

- `Database::execute(sql)` is the general SQL entrypoint (read/write, exclusive lock).
- `Database::query(sql)` is read-only (shared lock, rejects write SQL).
- CLI auto-routes read-only SQL to the read path; inside explicit transactions it always uses execute semantics.

## Repository Layout

- `src/` - database implementation
- `tests/` - integration and regression tests
- `docs-site/` - user and internals documentation (mdBook)
- `specs/tla/` - TLA+ specs for crash/recovery protocol

## License

MIT. See `LICENSE`.
