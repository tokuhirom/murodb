# Architecture

MuroDB is an embedded SQL database with optional at-rest encryption.
The runtime stack is organized in layers:

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

## How To Read This Section

If your goal is "reconstruct internals after a long break", read in this order:

1. [Reading Guide](reading-guide.md)
2. [Files, WAL, and Locking](files-and-locking.md)
3. [B-tree](btree.md)
4. [Query Planning & Execution](query-planning.md)
5. [Cryptography](cryptography.md)
6. [WAL & Crash Resilience](wal.md)

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

For on-disk file contracts (main DB file / `.wal` / `.lock`), see [Files, WAL, and Locking](files-and-locking.md).
