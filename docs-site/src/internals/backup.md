# Backup

## Overview

MuroDB provides a `Database::backup()` API for creating consistent, point-in-time snapshots of a running database. The backup file is a fully valid MuroDB database file that can be opened directly with the same key or password.

## Design

### Consistency Model

The backup API uses a **write-lock + WAL checkpoint + byte-copy** approach:

1. **Write lock**: Acquires an exclusive lock to prevent concurrent writes during the backup window.
2. **WAL checkpoint**: Calls `checkpoint_truncate()` to flush all committed WAL records into the main data file and truncate the WAL. This ensures the data file is self-consistent.
3. **Byte-copy**: Copies the plaintext header (76 bytes) and all encrypted pages as raw bytes from the source file to the destination. No decryption or re-encryption occurs.
4. **fsync**: The destination file is fsynced before the lock is released, ensuring durability.

### What Gets Copied

The backup copies exactly `HEADER_SIZE + page_count * page_size_on_disk` bytes:

- **Header (76 bytes)**: Magic, format version, salt, catalog root, page count, epoch, freelist page ID, next TxId, encryption suite ID, CRC32.
- **Pages**: All pages in their on-disk (encrypted) form, including data pages, B-tree nodes, freelist pages, and FTS posting pages.

The WAL file is **not** included in the backup because the checkpoint step ensures all committed data is already in the main file.

### Concurrency

- During backup, **writers are blocked** (exclusive lock held).
- Backup is typically fast (sequential I/O), so the write-stall window is proportional to database size.
- After the backup completes, normal read/write operations resume.

## Usage

### Rust API

```rust
use murodb::Database;

let mut db = Database::open(path, &master_key)?;

// ... normal operations ...

// Create a backup
db.backup("backup.db")?;
```

### Restore

Restoring from a backup is a file-level operation:

1. Stop the application (or close the `Database` handle).
2. Replace the database file with the backup file.
3. Delete the WAL file (`.wal` suffix) if present — the backup has no pending WAL.
4. Reopen the database.

```rust
// The backup file is a valid MuroDB database
let mut restored = Database::open("backup.db", &master_key)?;
```

## Constraints

| Constraint | Detail |
|---|---|
| Disk space | Requires free space equal to the full database size. |
| Write stall | Writers are blocked for the duration of the copy. |
| WAL | WAL is checkpointed and truncated before copy; not included in backup. |
| Encryption | Backup preserves the same encryption suite, key, and salt. |
| Atomicity | If the backup process crashes mid-copy, the destination file may be partial/corrupt. The source database is unaffected. |
