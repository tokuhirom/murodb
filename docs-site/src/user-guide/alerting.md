# Alerting & Monitoring

MuroDB exposes internal health metrics via the `SHOW DATABASE STATS` SQL command. This page describes how to use these metrics for operational alerting.

## Querying Stats

```sql
SHOW DATABASE STATS;
```

Returns a key-value table with columns: `stat` and `value`.

Related command:

```sql
SHOW CHECKPOINT STATS;
```

This returns the checkpoint-only subset for backward compatibility.

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
  message: "Checkpoint failures detected. WAL may be growing. Monitor WAL file size on disk."
```

### deferred_checkpoints / checkpoint_pending_ops

`deferred_checkpoints` is the number of transactions where checkpoint was intentionally skipped by policy.
`checkpoint_pending_ops` is the current backlog since the last successful checkpoint.

High `deferred_checkpoints` is expected with batch policies. Alert only when `checkpoint_pending_ops` keeps growing together with WAL size.

### checkpoint_policy_*

The active checkpoint policy is surfaced as:
- `checkpoint_policy_tx_threshold`
- `checkpoint_policy_wal_bytes_threshold`
- `checkpoint_policy_interval_ms`

Policy is configured via environment variables:
- `MURODB_CHECKPOINT_TX_THRESHOLD` (default `1`, `0` disables tx-count trigger)
- `MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD` (default `0`, disabled)
- `MURODB_CHECKPOINT_INTERVAL_MS` (default `0`, disabled)

### freelist_sanitize_count

**Alert threshold**: `> 0` (informational)

Number of times the freelist was sanitized during page allocation to remove invalid entries. This is a self-healing mechanism, not necessarily an error.

**Action**: If the count is consistently non-zero across sessions, investigate potential freelist corruption. A one-time occurrence after crash recovery is normal.

### freelist_out_of_range_total / freelist_duplicates_total

**Alert threshold**: `> 0` (informational)

Breakdown counters for freelist sanitization events:

- `freelist_out_of_range_total`: removed page IDs outside valid page range
- `freelist_duplicates_total`: removed duplicate page IDs

**Action**: non-zero values are expected only when sanitization occurred. If values continue increasing across clean restarts, investigate possible on-disk corruption.

## WAL Size Monitoring

`wal_file_size_bytes` is not currently exposed via `SHOW DATABASE STATS`.

Track WAL size at the file level and correlate with `failed_checkpoints`:

```bash
ls -lh mydb.wal
```

Persistent growth together with `failed_checkpoints > 0` indicates checkpoint truncate failures.

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
        // Track WAL file size via filesystem metrics
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
| `freelist_out_of_range_total` | `> 0` | Info | Invalid freelist entries removed (range) |
| `freelist_duplicates_total` | `> 0` | Info | Invalid freelist entries removed (duplicates) |
