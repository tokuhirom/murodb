# Backup & Restore

## Overview

MuroDB provides a `Database::backup()` API for creating consistent, point-in-time backups of a running database. The backup file is a complete MuroDB database that can be opened directly with the same key or password.

## Creating a Backup

### Rust API

```rust
use murodb::Database;
use murodb::crypto::aead::MasterKey;

let mut db = Database::open(path, &master_key)?;

// Normal operations ...
db.execute("INSERT INTO users VALUES (1, 'alice')")?;

// Create a consistent backup
db.backup("/path/to/backup.db")?;
```

Password-based databases work the same way:

```rust
let mut db = Database::open_with_password(path, "my-password")?;
db.backup("/path/to/backup.db")?;
```

Plaintext databases too:

```rust
let mut db = Database::open_plaintext(path)?;
db.backup("/path/to/backup.db")?;
```

### What Happens During Backup

1. An exclusive lock is acquired (writers are blocked, but backup is typically fast).
2. The WAL is checkpointed so all committed data is flushed to the main file.
3. The database file is copied byte-by-byte to the destination.
4. The destination file is fsynced for durability.
5. The lock is released and normal operations resume.

## Restoring from a Backup

The backup file is a standard MuroDB database file. To restore:

1. Stop the application (or close the `Database` handle).
2. Replace the original database file with the backup file.
3. Delete the WAL file (`<dbname>.wal`) if present — the backup has no pending WAL.
4. Reopen the database normally.

```rust
// Open the backup directly
let mut db = Database::open("/path/to/backup.db", &master_key)?;
```

## Safety

- **Same-file protection**: Attempting to backup to the source file itself (including via symlinks or hardlinks) returns an error without modifying the source.
- **Encryption preserved**: Encrypted databases are copied as-is. No decryption or re-encryption occurs. The backup uses the same key, salt, and encryption suite.
- **Crash safety**: If the process crashes during backup, the source database is unaffected. The destination file may be incomplete and should be discarded.

## Limitations

| Item | Detail |
|---|---|
| Disk space | Requires free space equal to the full database size. |
| Writer blocking | Writers are blocked for the duration of the copy (proportional to DB size). |
| WAL not included | The WAL is checkpointed before copy; the backup file has no WAL dependency. |
| Incremental backup | Not supported. Each backup is a full copy. |
