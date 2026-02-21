# Durability Matrix

This page describes the exact guarantees MuroDB provides at each stage of the commit pipeline and the post-crash outcome when a failure occurs at each step.

## Commit Pipeline

The commit flow proceeds through these ordered steps:

```
1. WAL: Begin record
2. WAL: PagePut records (one per dirty page)
3. WAL: PagePut for freelist page
4. WAL: MetaUpdate record (catalog_root, page_count, freelist_page_id)
5. WAL: Commit record
6. WAL: fsync            ← COMMIT POINT (durability boundary)
7. Data file: write dirty pages
8. Data file: flush_meta (fsync)
9. WAL: checkpoint_truncate (fsync + directory fsync)
```

The **commit point** is step 6 (WAL fsync). Once `wal.sync()` returns successfully, the transaction is durable. Steps 7-9 are performance optimizations that apply the committed data to the main database file; if they fail, WAL recovery replays the committed transaction on next open.

## Fsync Points

| Step | Fsync Target | What It Protects |
|---|---|---|
| WAL sync (step 6) | WAL file | All WAL records for the transaction reach stable storage. This is the commit point. |
| flush_meta (step 8) | Data file | Page data and metadata (catalog_root, page_count, freelist_page_id) are persisted to the main DB file. |
| checkpoint_truncate (step 9) | WAL file + directory | WAL is truncated to header-only. Directory fsync hardens the metadata change. |

## Crash-at-Each-Step Outcome Matrix

| Crash Point | WAL State | Committed? | Post-Recovery Outcome |
|---|---|---|---|
| After Begin (step 1) | Begin only | No | Transaction discarded. Prior committed data intact. |
| After PagePut (step 2) | Begin + PagePut(s) | No | Transaction discarded. No pages applied. |
| After freelist PagePut (step 3) | Begin + PagePut(s) + freelist | No | Transaction discarded. Freelist unchanged. |
| After MetaUpdate (step 4) | Begin + PagePut(s) + MetaUpdate | No | Transaction discarded. Metadata unchanged. |
| After Commit record (step 5) | Complete WAL sequence | No | WAL not fsynced; records may not have reached disk. OS may or may not have flushed buffers. If records survived: recovery replays. If not: transaction lost (no durability guarantee without fsync). |
| After WAL sync (step 6) | Complete + fsynced | **Yes** | Recovery replays committed pages and metadata to data file. |
| After page writes (step 7) | Complete + fsynced | **Yes** | Some or all pages written. Recovery replays any missing pages idempotently. |
| After flush_meta (step 8) | Complete + fsynced | **Yes** | Data file fully consistent. WAL replay is idempotent (re-applying same pages is safe). |
| After checkpoint_truncate (step 9) | Truncated | **Yes** | WAL is empty. Data file is self-consistent. Normal operation resumes. |

## Post-WAL-Sync Failures (CommitInDoubt)

When steps 7 or 8 fail after the WAL has been synced, the commit is durable in the WAL but the in-process session cannot confirm it succeeded on the data file. MuroDB handles this as follows:

1. `Transaction::commit()` returns `Err(CommitInDoubt)`.
2. The session is **poisoned** - all subsequent operations return `SessionPoisoned`.
3. On reopen, WAL recovery replays the committed transaction, converging to the correct state.

This design ensures that a durable commit is never lost, even if the process crashes or encounters I/O errors after the commit point.

## Checkpoint Truncate Failure

If `checkpoint_truncate()` fails (step 9), the WAL retains committed records. On next open, recovery replays them idempotently. The data file already has the correct state (from steps 7-8), so replay simply overwrites pages with identical content. WAL growth is the only concern; monitor WAL file size on disk and `failed_checkpoints` via `SHOW DATABASE STATS`.

## Idempotent Recovery

WAL recovery is designed to be idempotent:

- Running `recover()` multiple times on the same WAL produces identical database state.
- `page_count` only increases, never decreases, during recovery.
- Page data is overwritten with the WAL image regardless of current content.
- Metadata (catalog_root, freelist_page_id) is set to the last committed values.

This property is critical for crash-during-recovery scenarios: if the process crashes during recovery, the next recovery attempt produces the same result.

## Torn WAL Tail

A crash during WAL writes can leave a partially-written frame at the end of the WAL file. MuroDB's WAL reader handles this gracefully:

- **Truncated frame**: A frame header claiming more bytes than remain in the file is treated as end-of-log.
- **Garbage bytes**: Bytes that fail decryption or CRC validation at the tail are ignored.
- **Zero-filled tail**: Zero bytes (from filesystem pre-allocation) produce a zero frame length, treated as end-of-log.
- **Mid-log corruption**: Corruption followed by valid frames is a hard error (prevents silent data loss).

The WAL reader uses a two-layer tail detection heuristic:
1. **Structural check**: Is the next frame structurally plausible (non-zero length, fits in file)?
2. **Content probe**: Even if structurally plausible, can any following frame be successfully decrypted and CRC-validated?

If both checks indicate no valid data follows the corrupt frame, it is treated as tail garbage and ignored.
