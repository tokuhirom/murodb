# MuroDB

Embedded SQL database written in Rust.

## Status

MuroDB is under active development.

- Core storage: encrypted pages + WAL crash recovery
- Pluggable encryption suite: `aes256-gcm-siv` / `off` (plaintext)
- SQL engine: practical subset for local/embedded use
- Documentation: moved to `docs-site/`

## Install

```bash
cargo install --path .
```

## Quick Start

```bash
# Create a database
murodb mydb.db --create -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"

# Create a plaintext database (explicit opt-in)
murodb mydb_plain.db --create --encryption off -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"

# Insert data
murodb mydb.db -e "INSERT INTO t (id, name) VALUES (1, 'hello')"

# Query
murodb mydb.db -e "SELECT * FROM t"

# Interactive REPL
murodb mydb.db
```

## Documentation

The detailed manual and internals docs are in `docs-site/`.

- Book source: `docs-site/src/`
- Build locally:

```bash
mdbook build docs-site
```

Main entry points:

- `docs-site/src/getting-started/quick-start.md`
- `docs-site/src/user-guide/sql-reference.md`
- `docs-site/src/user-guide/recovery.md`
- `docs-site/src/internals/architecture.md`

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
