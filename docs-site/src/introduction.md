# MuroDB

Embedded SQL database with B+Tree (no leaf links) + Full-Text Search (Bigram), written in Rust.

## Features

- **Pluggable at-rest mode** - `aes256-gcm-siv` (default) or explicit `off` plaintext mode
- **B+tree storage (no leaf links)** - PRIMARY KEY (TINYINT/SMALLINT/INT/BIGINT), UNIQUE indexes (single column)
- **Full-text search** - Bigram (n=2) with NFKC normalization
  - MySQL-style `MATCH(col) AGAINST(...)` syntax
  - NATURAL LANGUAGE MODE with BM25 scoring
  - BOOLEAN MODE with `+term`, `-term`, `"phrase"` operators
  - `fts_snippet()` for highlighted excerpts
- **ACID transactions** - WAL-based crash recovery
- **Concurrency** - Multiple readers / single writer (thread RwLock + process file lock)
- **Single file** - Database file + WAL file

## Components

| Component | Description |
|---|---|
| `crypto/` | AES-256-GCM-SIV page encryption, Argon2 KDF, HMAC-SHA256 term blinding |
| `storage/` | 4096B slotted pages, pager with pluggable at-rest mode + LRU cache, freelist |
| `btree/` | Insert/split, delete, search, scan, order-preserving key encoding |
| `wal/` | WAL records (suite-aligned with DB mode), writer/reader, crash recovery |
| `tx/` | Transaction with dirty page buffer, commit/rollback |
| `schema/` | System catalog, table/index definitions |
| `sql/` | Hand-written lexer/parser, AST, rule-based planner, executor |
| `fts/` | Bigram tokenizer, delta+varint postings, BM25, NATURAL/BOOLEAN queries, snippets |
| `concurrency/` | parking_lot RwLock + fs4 file lock |

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

## License

MIT License. See [LICENSE](https://github.com/tokuhirom/murodb/blob/main/LICENSE) for details.
