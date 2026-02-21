# Incident Response Runbook

This runbook covers common failure scenarios and the recommended response procedures for MuroDB operators.

## First Response: Gather Diagnostics

Before taking any corrective action, collect the current state:

```sql
SHOW DATABASE STATS;
```

Note the values of `commit_in_doubt_count`, `failed_checkpoints`, `freelist_sanitize_count`, and `wal_file_size_bytes`. Check the WAL file size on disk as well:

```bash
ls -lh mydb.wal
```

If the database cannot be opened, inspect the WAL without modifying it:

```bash
murodb mydb.db --inspect-wal mydb.wal --recovery-mode permissive --format json
```

## Scenario: CommitInDoubt Detected

**Symptom**: `commit_in_doubt_count > 0` in `SHOW DATABASE STATS`, or the application receives a `CommitInDoubt` error.

**What happened**: The transaction was durably written to the WAL, but the subsequent write of dirty pages or metadata to the data file failed (e.g., disk full, I/O error). The session is poisoned and will reject further operations.

**Response**:

1. Close the current session / database handle immediately.
2. Investigate the root cause — check disk space (`df -h`), kernel logs (`dmesg`), and storage health.
3. Resolve the underlying issue (free disk space, replace failing disk).
4. Reopen the database. WAL recovery will automatically replay the committed transaction.
5. Run `SHOW DATABASE STATS` to confirm `commit_in_doubt_count` is `0` after recovery.

**Do NOT** delete or rename the WAL file — it contains the committed data that needs to be replayed.

## Scenario: Checkpoint Failures / WAL Growth

**Symptom**: `failed_checkpoints > 0`, `wal_file_size_bytes` growing over time.

**What happened**: After committing, MuroDB truncates the WAL via a checkpoint. If truncation fails, the WAL keeps growing. The database remains correct (WAL replay is idempotent), but recovery time increases.

**Response**:

1. Check disk I/O health and available space.
2. If the WAL is very large but the database is otherwise healthy, restart the process. Recovery on startup will replay and then truncate the WAL.
3. Monitor `wal_file_size_bytes` after restart to confirm the WAL was truncated.

## Scenario: Freelist Corruption Suspected

**Symptom**: `freelist_sanitize_count` is consistently non-zero across multiple sessions (not just once after a crash recovery).

**What happened**: The freelist (which tracks reusable pages) contained invalid entries. MuroDB self-heals by removing invalid entries during allocation, but persistent occurrences may indicate deeper corruption.

**Response**:

1. A single occurrence after crash recovery is **normal** — no action needed.
2. If it recurs across sessions:
   - Back up the database file and WAL immediately.
   - Open with `--recovery-mode permissive` and check the recovery report for skipped transactions.
   - If data integrity is confirmed, the self-healing is working correctly. Continue monitoring.
   - If data loss is suspected, restore from backup and replay from the last known good state.

## Scenario: Database Fails to Open (WAL Corruption)

**Symptom**: Opening the database fails with a recovery error in strict mode.

**Response**:

1. **Inspect first** — do not delete any files:
   ```bash
   murodb mydb.db --inspect-wal mydb.wal --recovery-mode permissive --format json
   ```
2. Review the report. If only incomplete (uncommitted) transactions are malformed, they can be safely skipped.
3. Open with permissive mode to recover valid data:
   ```bash
   murodb mydb.db --recovery-mode permissive
   ```
4. The original WAL is automatically quarantined to `*.wal.quarantine.*` for forensic analysis.
5. Verify recovered data integrity by querying critical tables.

## Scenario: Process Crash / Kill During Operation

**Symptom**: The MuroDB process was killed (SIGKILL, OOM, power loss) mid-operation.

**Response**:

1. Simply reopen the database. WAL recovery handles this automatically.
2. Check `SHOW DATABASE STATS` after recovery:
   - `commit_in_doubt_count` should be `0`.
   - `freelist_sanitize_count` may be `> 0` once — this is normal.
3. If strict recovery fails, follow the "Database Fails to Open" procedure above.

## When to Restart vs. Quarantine WAL

| Situation | Action |
|---|---|
| Session poisoned (CommitInDoubt) | Restart — recovery replays committed data |
| WAL growing (checkpoint failures) | Restart — recovery truncates WAL |
| Strict recovery fails | Inspect WAL, then open with `--recovery-mode permissive` |
| Repeated freelist sanitization | Back up, then investigate with permissive mode |
| Corrupted WAL with data loss | Restore from backup |

## Escalation Criteria

Escalate to the development team if:

- `commit_in_doubt_count > 0` persists **after** reopening the database (recovery failed to replay).
- WAL inspection shows committed transactions that were not recovered.
- Permissive mode skips transactions that should have been valid.
- Freelist sanitization count grows across sessions without any preceding crash.
- The database file size is inconsistent with expected data volume (possible page leak).

When escalating, include:

1. Full `SHOW DATABASE STATS` output.
2. WAL inspection JSON output (`--format json`).
3. Kernel logs around the time of failure (`dmesg`, `journalctl`).
4. The quarantined WAL file(s), if any.
