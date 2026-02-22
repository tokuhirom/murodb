# WAL & Crash Resilience

MuroDB uses a write-ahead log (`.wal`) for crash recovery.
All commits are WAL-first: durable intent is recorded before data-file flush.

## `.wal` Binary Layout

WAL constants are in `src/wal/mod.rs`:

- magic: `"MUROWAL1"` (8 bytes)
- version: `u32` (current `1`)
- header size: 12 bytes

File layout:

1. Header: `[magic:8][version:4]`
2. Repeating frames:
   - `[frame_len: u32]`
   - `[encrypted_payload: frame_len bytes]`

`frame_len` is bounded by `MAX_WAL_FRAME_LEN` (`PAGE_SIZE + 1024`).

Encrypted payload format before encryption (`src/wal/writer.rs`):

- `record_bytes = WalRecord::serialize(...)`
- `payload = record_bytes || crc32(record_bytes)`

Encryption uses `PageCipher`; frame nonce context is `(lsn, 0)`.

## WAL Record Types

`WalRecord` (`src/wal/record.rs`) variants:

| Record | Payload |
|---|---|
| `Begin` | `txid` |
| `PagePut` | `txid`, `page_id`, full page image bytes |
| `MetaUpdate` | `txid`, `catalog_root`, `page_count`, `freelist_page_id`, `epoch` |
| `Commit` | `txid`, `lsn` |
| `Abort` | `txid` |

Record tags on wire:

- `1=Begin`, `2=PagePut`, `3=Commit`, `4=Abort`, `5=MetaUpdate`

## Write Path

### Read-Only Query Path (`Database::query`)

`Database::query(sql)`:

1. Acquire shared lock
2. Parse/validate read-only statement
3. Execute directly on pager/catalog (no implicit WAL transaction)

If an explicit transaction is active, read statements are executed in the transaction context (`execute_in_tx`) so uncommitted writes remain visible to that session.

### Auto-Commit Mode (no explicit BEGIN)

`Session::execute_auto_commit(stmt)`:

1. Create implicit transaction + dirty-page buffer.
2. Execute statement against transactional page store.
3. `tx.commit(...)` writes:
   - `Begin`
   - all dirty `PagePut`
   - freelist `PagePut` pages (if needed)
   - `MetaUpdate`
   - `Commit`
4. `wal.sync()` (fsync) establishes durability boundary.
5. Flush pages + metadata to main DB file.

### Explicit Transaction (BEGIN ... COMMIT)

Explicit transaction (`BEGIN ... COMMIT`) follows the same commit primitive.
`ROLLBACK` discards dirty state without WAL append (`rollback_no_wal` in session path).

## Commit Point

Durability commit point is WAL fsync:

- before `wal.sync()`: commit may be lost on crash
- after `wal.sync()`: commit must be recoverable even if DB flush fails

If post-sync DB flush fails, transaction returns `CommitInDoubt`, session is poisoned, and next open recovers from WAL.

## Recovery (Database::open)

```
Database::open(path, master_key)
  1. If WAL file exists, run recovery::recover()
     → Scan WAL and validate per-tx state machine
       (Begin -> PagePut/MetaUpdate* -> Commit/Abort)
     → Collect latest page images from committed transactions
     → Replay to data file
  2. Truncate WAL file (empty it)
     → fsync WAL file
     → best-effort fsync parent directory
  3. Build Session with Pager + Catalog + WalWriter
```

Validation is implemented in `src/wal/recovery.rs` with explicit skip/error codes.

## Recovery Modes

- **strict** (default): Fails on any WAL protocol violation
- **permissive**: Skips invalid transactions, recovers only valid committed ones

See [Recovery](../user-guide/recovery.md) for user-facing documentation.

In permissive mode, if invalid transactions were skipped, WAL can be quarantined (`*.quarantine.<ts>.<pid>`) before reopening a clean WAL stream.

### Inspect-WAL JSON Contract

`murodb-wal-inspect --format json` returns machine-readable diagnostics with a stable schema contract:

- `schema_version=1` for the current contract
- `status`: `ok` / `warning` / `fatal`
- `exit_code`: mirrors CLI exit code semantics (`0`, `10`, `20`)
- `skipped[].code`: stable machine-readable skip classification
- On fatal failures, `fatal_error` and `fatal_error_code` are included

## Secondary Index Consistency

All index updates happen within the same transaction as the data update:

### INSERT
1. Insert row into data B-tree
2. Insert entry into each secondary index (column_value → PK)
3. Check UNIQUE constraint before insertion

### DELETE
1. Scan for rows to delete (collect PK + all column values)
2. Delete entries from each secondary index
3. Delete row from data B-tree

### UPDATE
1. Scan for rows to update (collect PK + old column values)
2. Compute new values
3. Check UNIQUE constraints (for changed values)
4. Update secondary indexes (delete old entry + insert new entry)
5. Write new row data to data B-tree

## Remaining Constraints

### fsync granularity

`Pager::write_page_to_disk()` does not call `sync_all()` individually. Only `flush_meta()` calls `sync_all()`. WAL `sync()` guarantees data durability, so this is safe in normal operation.

### allocate_page counter

`Pager::allocate_page()` increments in-memory `page_count`, which is not persisted until `flush_meta()` after WAL commit.

### WAL file size

After successful commits and explicit `ROLLBACK`, the Session auto-checkpoints the WAL according to policy. Checkpoint is best-effort and does not affect commit success.

Default policy is per-transaction (`MURODB_CHECKPOINT_TX_THRESHOLD=1`), and can be tuned with:

- `MURODB_CHECKPOINT_TX_THRESHOLD`
- `MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD`
- `MURODB_CHECKPOINT_INTERVAL_MS`

When checkpoint truncate fails, MuroDB emits a warning with `wal_path` and `wal_size_bytes` so operators can detect and triage WAL growth.

## TLA+ Correspondence

See [Formal Verification](formal-verification.md) for the TLA+ model and its mapping to implementation.

| TLA+ Intent | Implementation | Regression Test |
|---|---|---|
| Only valid state transitions are recovered | State transition validation in `recovery.rs` | `test_recovery_rejects_pageput_before_begin` |
| Commit/Abort is terminal | Reject duplicate terminal / post-terminal records | `test_recovery_rejects_duplicate_terminal_record_for_tx` |
| Commit has consistent terminal info | Validate `Commit.lsn == actual LSN` | `test_recovery_rejects_commit_lsn_mismatch` |
| Commit requires metadata | Reject Commit without MetaUpdate | `test_recovery_rejects_commit_without_meta_update` |
| PagePut matches target page | Validate `PagePut.page_id` vs page header | `test_recovery_rejects_pageput_page_id_mismatch` |
| Tail corruption tolerated, mid-log rejected | Reader tolerates tail only | `test_tail_truncation_tolerated`, `test_mid_log_corruption_is_error` |
| Oversized frames handled safely | Frame length limit in Reader/Writer | `test_oversized_tail_frame_tolerated` |
| Freelist recovered from committed MetaUpdate | `freelist_page_id` in WAL MetaUpdate | `test_freelist_wal_recovery` |
