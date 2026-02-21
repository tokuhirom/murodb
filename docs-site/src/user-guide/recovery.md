# Recovery

MuroDB uses WAL-based crash recovery with two recovery modes.

## Recovery modes

### strict (default)

The default mode. Fails on any WAL protocol violation.

```bash
murodb mydb.db
```

Detects and rejects:
- Records before BEGIN
- Commit LSN mismatches
- Duplicate terminal records
- PagePut integrity mismatches

### permissive

Skips invalid transactions and recovers only valid committed transactions. Useful for salvaging data from corrupted databases.

```bash
murodb mydb.db --recovery-mode permissive
```

When transactions are skipped, the original WAL is quarantined to `*.wal.quarantine.*`.

## WAL Inspection

Analyze WAL consistency without modifying the database.

```bash
# Text output
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive

# JSON output (for automation)
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive --format json
```

Quarantine files can also be inspected directly:

```bash
murodb-wal-inspect mydb.db --wal mydb.wal.quarantine.20240101_120000
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | No malformed transactions detected |
| `10` | Malformed transactions detected (inspection succeeded) |
| `20` | Fatal error (decrypt/IO/strict failure, etc.) |

## API

```rust
use murodb::{Database, RecoveryMode};

// strict (default)
let db = Database::open("mydb.db", &master_key)?;

// permissive
let db = Database::open_with_recovery_mode(
    "mydb.db", &master_key, RecoveryMode::Permissive
)?;

// permissive with report
let (db, report) = Database::open_with_recovery_mode_and_report(
    "mydb.db", &master_key, RecoveryMode::Permissive
)?;
for skip in &report.skipped {
    eprintln!("Skipped tx {}: {:?}", skip.txid, skip.reason);
}
```

## JSON Schema Versioning Policy

- `schema_version` increments only on breaking changes (key removal, type changes)
- New keys are added without version bump (consumers should ignore unknown keys)
- `RecoverySkipCode` string values are frozen (regression-tested)
- `InspectFatalKind` string values are frozen (regression-tested)
