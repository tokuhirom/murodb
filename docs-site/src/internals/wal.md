# WAL & Crash Resilience

MuroDB uses a Write-Ahead Log (WAL) for crash recovery. All writes go through the WAL before being applied to the data file.

## WAL Record Types

| Record | Description |
|---|---|
| BEGIN | Start of a transaction |
| PAGE_PUT | Write a page (page_id + page data) |
| META_UPDATE | Metadata update (catalog_root, freelist_page_id, page_count, epoch) |
| COMMIT | Transaction commit marker |
| ABORT | Transaction abort marker |

All WAL records are encrypted.

## Write Path

### Read-Only Query Path (`Database::query`)

```
Database::query(sql)
  1. Acquire shared lock
  2. Parse and validate statement is read-only
  3. Execute directly on pager/catalog (no implicit tx, no WAL append)
```

If an explicit transaction is active, read statements are executed in the transaction context (`execute_in_tx`) so uncommitted writes remain visible to that session.

### Auto-Commit Mode (no explicit BEGIN)

```
Session::execute_auto_commit(stmt)
  1. Transaction::begin(txid, snapshot_lsn)
  2. Create TxPageStore (dirty page buffer)
  3. execute_statement(stmt, tx_page_store, catalog)
       → BTree::insert(tx_page_store, key, value)
         → TxPageStore::write_page()
           → Transaction::write_page()  ← stored in HashMap (memory only)
  4. tx.commit(&mut pager, &mut wal)    ← WAL-first commit
       → Write Begin + PagePut + MetaUpdate + Commit records to WAL
       → wal.sync()                     ← fsync WAL
       → Write dirty pages to data file
       → pager.flush_meta()             ← fsync data file
  (on error: tx.rollback_no_wal() + restore catalog)
```

### Explicit Transaction (BEGIN ... COMMIT)

```
BEGIN
  → Transaction::begin(txid, wal.current_lsn())

exec_insert() / exec_update() / exec_delete()
  → Write to dirty page buffer via TxPageStore

COMMIT
  → tx.commit(&mut pager, &mut wal)   ← WAL-first commit
    1. Write Begin record to WAL
    2. Write PagePut record for each dirty page
    3. Write MetaUpdate (catalog_root, freelist_page_id, page_count, epoch)
    4. Write Commit record
    5. wal.sync()                      ← fsync WAL
    6. Write dirty pages to data file
    7. pager.flush_meta()              ← fsync data file

ROLLBACK
  → tx.rollback_no_wal()              ← discard dirty buffer (no WAL write)
  → Session post_rollback_checkpoint() keeps WAL clean
  → Reload catalog from disk
```

## Recovery (Database::open)

```
Database::open(path, master_key)
  1. If WAL file exists, run recovery::recover()
     → Scan WAL and validate transaction state transitions
       (reject PagePut/MetaUpdate/Commit/Abort before Begin)
       (reject records after Commit/Abort)
       (reject Commit.lsn mismatch with actual LSN)
     → Collect latest page images from committed transactions
     → Replay to data file
  2. Truncate WAL file (empty it)
     → fsync WAL file
     → best-effort fsync parent directory
  3. Build Session with Pager + Catalog + WalWriter
```

## Recovery Modes

- **strict** (default): Fails on any WAL protocol violation
- **permissive**: Skips invalid transactions, recovers only valid committed ones

See [Recovery](../user-guide/recovery.md) for user-facing documentation.

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
