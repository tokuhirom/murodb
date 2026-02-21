# Alerting & Monitoring

MuroDB exposes internal health metrics via the `SHOW DATABASE STATS` SQL command. This page describes how to use these metrics for operational alerting.

## Querying Stats

```sql
SHOW DATABASE STATS;
```

Returns a table with columns: `stat_name` and `stat_value`.

## Key Metrics

### commit_in_doubt_count

**Alert threshold**: `> 0`

Indicates the number of times a commit succeeded in the WAL (durable) but failed to write pages or metadata to the data file. The session is poisoned after this event.

**Action**: Close the session and reopen the database. WAL recovery will replay the committed transaction. Investigate the root cause (disk full, I/O errors, hardware failure).

```
IF commit_in_doubt_count > 0 THEN ALERT
  severity: CRITICAL
  message: "CommitInDoubt detected. Session is poisoned. Reopen database to recover."
```

### failed_checkpoints

**Alert threshold**: `> 0`

Indicates checkpoint truncation failures. The WAL file is not being truncated after successful commits, which causes WAL growth.

**Action**: Monitor WAL file size. If it grows unboundedly, investigate disk I/O. The database remains correct (WAL replay is idempotent), but performance may degrade due to longer recovery times.

```
IF failed_checkpoints > 0 THEN ALERT
  severity: WARNING
  message: "Checkpoint failures detected. WAL may be growing. Monitor wal_file_size_bytes."
```

### freelist_sanitize_count

**Alert threshold**: `> 0` (informational)

Number of times the freelist was sanitized during page allocation to remove invalid entries. This is a self-healing mechanism, not necessarily an error.

**Action**: If the count is consistently non-zero across sessions, investigate potential freelist corruption. A one-time occurrence after crash recovery is normal.

### wal_file_size_bytes

**Alert threshold**: Application-specific (e.g., `> 10MB`)

Current WAL file size. Under normal operation, the WAL is truncated after each commit. Persistent growth indicates checkpoint failures.

**Action**: If WAL grows beyond expected bounds, check `failed_checkpoints`. Consider restarting the process to trigger recovery and WAL truncation.

## Monitoring Patterns

### Polling Loop

For long-running applications, periodically query stats:

```rust
use murodb::Database;

fn check_health(db: &mut Database) {
    if let Ok(result) = db.execute("SHOW DATABASE STATS") {
        // Parse result and check thresholds
        // Alert on commit_in_doubt_count > 0
        // Alert on failed_checkpoints > 0
        // Track wal_file_size_bytes trend
    }
}
```

### Post-Recovery Check

After opening a database that required WAL recovery, verify the recovery result:

```rust
use murodb::{Database, RecoveryMode};

let (db, report) = Database::open_with_recovery_mode_and_report(
    "mydb.db", &master_key, RecoveryMode::Strict
)?;

if !report.committed_txids.is_empty() {
    log::info!("Recovered {} committed transactions", report.committed_txids.len());
}
if !report.skipped.is_empty() {
    log::warn!("Skipped {} malformed transactions", report.skipped.len());
}
```

## Summary

| Metric | Threshold | Severity | Meaning |
|---|---|---|---|
| `commit_in_doubt_count` | `> 0` | Critical | Session poisoned; reopen required |
| `failed_checkpoints` | `> 0` | Warning | WAL growing; checkpoint failing |
| `freelist_sanitize_count` | `> 0` | Info | Freelist self-healed |
| `wal_file_size_bytes` | App-specific | Warning | WAL not being truncated |
