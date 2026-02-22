# Architecture

MuroDB is an encrypted embedded SQL database. The system is organized in layers:

```
sql/ (lexer → parser → planner → executor)
  ↓
schema/ (catalog: table/index definitions)
  ↓
tx/ (transaction: dirty page buffer, commit/rollback)
  ↓
btree/ (B-tree: insert/split, delete, search, scan)
  ↓
wal/ (WAL: encrypted records, crash recovery)
  ↓
storage/ (pager: encrypted page I/O, LRU cache, freelist)
  ↓
crypto/ (AES-256-GCM-SIV, Argon2 KDF, HMAC-SHA256)
```

Additional modules:

- `fts/` - Full-text search (bigram tokenizer, postings B-tree, BM25, BOOLEAN/NATURAL mode)
- `concurrency/` - parking_lot::RwLock (thread) + fs4 file lock (process)

## Module Map

| Module | Files | Role |
|---|---|---|
| `storage/` | page.rs, pager.rs, freelist.rs | 4096B encrypted page I/O |
| `crypto/` | aead.rs, kdf.rs, hmac_util.rs | Encryption primitives |
| `btree/` | node.rs, ops.rs, cursor.rs, key_encoding.rs | B-tree operations |
| `wal/` | record.rs, writer.rs, reader.rs, recovery.rs | WAL + crash recovery |
| `tx/` | transaction.rs, lock_manager.rs | Transactions |
| `schema/` | catalog.rs, column.rs, index.rs | System catalog |
| `sql/` | lexer.rs, parser.rs, ast.rs, planner.rs, executor.rs, eval.rs | SQL processing |
| `fts/` | tokenizer.rs, postings.rs, index.rs, query.rs, scoring.rs, snippet.rs | Full-text search |
| `concurrency/` | mod.rs | Concurrency control |

## Concurrency Model

- **Thread-level**: `parking_lot::RwLock` - multiple readers, single writer
- **Process-level**: `fs4` file lock - prevents concurrent access from multiple processes
- **API routing**:
  - `Database::query` acquires a shared lock for read-only statements.
  - `Database::execute` acquires an exclusive lock for general SQL execution.
  - CLI routes read-only statements to `query` unless an explicit transaction is active.
