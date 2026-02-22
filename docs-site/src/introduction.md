# MuroDB

MuroDB is an embedded SQL database written in Rust.

This documentation is designed for people who already know SQL and want to start building quickly.

## What You Can Do Quickly

- Create a local database file and start querying right away.
- Keep data durable with WAL-based crash recovery.
- Use encrypted-at-rest mode by default, or explicitly opt into plaintext mode.
- Add full-text search with bigram tokenization and relevance scoring.

If you want hands-on steps first, start here:

1. [First Session (10 minutes)](getting-started/first-session.md)
2. [Quick Start](getting-started/quick-start.md)
3. [SQL Reference](user-guide/sql-reference.md)

## Core Capabilities

- **At-rest mode**: `aes256-gcm-siv` (default) or explicit `off` (plaintext)
- **Storage engine**: B+tree, WAL, page cache, crash recovery
- **Transactions**: ACID semantics
- **Concurrency model**: multiple readers / single writer
- **Full-text search**: `MATCH(...) AGAINST(...)`, NATURAL/BOOLEAN modes, `fts_snippet()`

## Architecture Map

| Component | Description |
|---|---|
| `crypto/` | Page encryption, key derivation, and term-ID blinding |
| `storage/` | Pager, slotted pages, cache, and freelist management |
| `btree/` | Index/table structures, search, and mutation operations |
| `wal/` | Write-ahead log, reader/writer, recovery flow |
| `tx/` | Transaction lifecycle, dirty-page buffering, commit/rollback |
| `schema/` | Catalog metadata for tables and indexes |
| `sql/` | Lexer, parser, planner, executor, and session control |
| `fts/` | Tokenizer, postings, BM25 scoring, snippets |
| `concurrency/` | In-process and cross-process locking |

## Non-goals

- Network server protocol
- Full access-pattern obfuscation
- Stored procedures / triggers
