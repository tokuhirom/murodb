# Files, WAL, and Locking

This chapter defines the on-disk files and lock behavior.

## At a Glance

For database path `<db_path>`, MuroDB uses three files:

`<db_path>` (main state) + `<db_path>.wal` (durability log) + `<db_path>.lock` (OS advisory lock target)

High-level flow:

1. Append commit-intent records to `.wal` and `wal.sync()`.
2. Commit is durable at this point.
3. Main DB file is checkpointed/flushed later.
4. `.lock` coordinates readers/writers across processes during API calls.

The sections below expand each file in this order.

## File Set

If you open database path `<db_path>`, MuroDB uses:

- `<db_path>`: main database file (header + pages)
- `<db_path>.wal`: write-ahead log
- `<db_path>.lock`: lock file for cross-process coordination

Example:

- if `<db_path> = mydata`, files are `mydata`, `mydata.wal`, `mydata.lock`
- if `<db_path> = mydb.db`, files are `mydb.db`, `mydb.db.wal`, `mydb.db.lock`

## Main DB File Layout

The main file starts with a 76-byte plaintext header (`src/storage/pager/mod.rs`):

| Offset | Size | Field |
|---|---:|---|
| 0 | 8 | Magic `"MURODB01"` |
| 8 | 4 | Format version (`u32`, current `4`) |
| 12 | 16 | KDF salt |
| 28 | 8 | Catalog root page id |
| 36 | 8 | Page count |
| 44 | 8 | Epoch |
| 52 | 8 | Freelist root page id |
| 60 | 8 | Next transaction id |
| 68 | 4 | Encryption suite id |
| 72 | 4 | CRC32 over bytes `0..72` |

Immediately after the header, pages are stored sequentially.
For page-level internals, see [Storage](storage.md).

## `.wal` File Role

`.wal` stores commit-intent records before data-file flush.
Binary framing details are in [WAL & Crash Resilience](wal.md).

Durability boundary:

- commit is considered durable after `wal.sync()` succeeds.
- data-file flush may happen after that; failures become `CommitInDoubt`.

## `.lock` File Semantics

`<db_path>.lock` is created by `LockManager::new` (`src/concurrency/mod.rs`).

- It is not a structured metadata file.
- Its payload is not interpreted by MuroDB.
- It exists as a stable file descriptor target for advisory file locks (`fs4`).

## Lock Granularity

Locking has two layers:

1. In-process: `parking_lot::RwLock<()>`
2. Cross-process: `fs4` shared/exclusive lock on `.lock`

API behavior:

- `Database::query(...)` acquires shared read lock.
- `Database::execute(...)` acquires exclusive write lock.
- `Database::query(...)` is a `&mut self` API because read execution may refresh pager/catalog metadata from disk before running.
- For multiple concurrent readers within one process, use separate read-only handles (for example `Database::open_reader()`).

Important granularity note:

- Locks are acquired per API call, not globally for session lifetime.
- During explicit transactions (`BEGIN ... COMMIT`), each statement still enters through `execute(...)` and takes the write lock for that call.

## Visibility Refresh

When no explicit transaction is active, session execution calls `pager.refresh_from_disk_if_changed()` and reloads catalog metadata when header fields changed.
This is how a process observes committed changes from other processes.

## Why this split (main file + `.wal` + `.lock`)?

- main DB file: stable state and efficient reads.
- `.wal`: sequential append for crash-safe commit protocol.
- `.lock`: avoids embedding lock bytes into data format and delegates arbitration to OS advisory locks.

Why `.lock` is separated from the main DB file:

- Keep data format clean: lock state is operational state, not database state.
- Avoid extra data-file churn: lock acquire/release does not force DB header/page writes.
- Better crash behavior: lock lifetime is tied to OS file-lock semantics; stale lock bytes do not need cleanup from the DB payload.
- Portability and tooling: advisory locking APIs (`flock`/`fcntl`-style via `fs4`) naturally target a lock file descriptor.
- Lower format coupling: lock strategy can evolve without changing on-disk table/page format.
