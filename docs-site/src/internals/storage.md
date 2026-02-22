# Storage

## Page Layout

- **Page size**: 4096 bytes (`PAGE_SIZE`)
- **Page header**: 14 bytes (`src/storage/page.rs`)
- **Cell pointer**: 2 bytes per cell
- **Cell payload**: `[len:u16][payload bytes]`
- **Cache**: LRU page cache (default 256 pages)

Slotted-page structure:

`[header(14)] [cell pointer array] [free space] [cell bodies (from tail)]`

This slotted layout is generic; B+tree node format is layered on top of it.  
See [B-tree](btree.md) for node/header cell conventions.

## Encryption

Encrypted mode stores each page as:

`nonce(12) || ciphertext || tag(16)`

- **Algorithm**: AES-256-GCM-SIV (nonce-misuse resistant AEAD)
- **KDF**: Argon2 derives the master key from the user's passphrase + random salt
- **AAD binding**: `(page_id, epoch)` prevents page-swap/misbinding attacks

See [Cryptography](cryptography.md) for rationale and full details.

## Freelist

Freed pages are tracked in a freelist for reuse:

## Freelist In-Memory Model

Implementation (`src/storage/freelist.rs`) uses `Vec<PageId>`:

- `allocate()` pops from tail (LIFO reuse)
- `free(page_id)` pushes if not already present
- duplicate `free` is treated as double-free and rejected
- `undo_last_free()` exists for speculative commit-time calculations

## Freelist On-Disk Format

Primary format is a multi-page chain.

Per freelist page data area:

`[magic "FLMP":4][next_page_id:u64][count:u64][entries:u64...]`

Facts:

- `ENTRIES_PER_FREELIST_PAGE = 507` for 4096-byte pages with 14-byte page header
- `next_page_id = 0` marks chain end
- DB header field `freelist_page_id` points to chain head

Backward compatibility:

- legacy single-page format (count + entries, without `FLMP`) is still readable

## Commit-Time Freelist Handling

During `Transaction::commit` (`src/tx/transaction.rs`):

1. Build a speculative freelist snapshot (without permanent mutation).
2. Determine how many freelist pages are needed.
3. Reuse existing freelist head page when possible, allocate more page IDs if needed.
4. Serialize freelist pages and emit them as WAL `PagePut`.
5. Emit `MetaUpdate` with new `freelist_page_id`.
6. After `wal.sync()` succeeds, apply freed pages to in-memory freelist.

This ordering avoids freelist state leaks when commit fails before WAL durability.

## Open-Time Freelist Loading and Sanitize

At open/refresh (`Pager::reload_freelist_from_disk`):

1. Read freelist chain from `freelist_page_id`.
2. For multi-page chain, detect cycles and out-of-range next pointers.
3. Deserialize entries.
4. Run `sanitize(page_count)` to remove:
   - out-of-range entries (`pid >= page_count`)
   - duplicate entries

Sanitization results are exposed as diagnostics and warning counters.

## Data File Header

Main DB file header (`src/storage/pager/mod.rs`) is 76 bytes:

```
Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B)
+ PageCount (8B) + Epoch (8B) + FreelistPageId (8B)
+ NextTxId (8B) + EncryptionSuiteId (4B) + CRC32 (4B)
```

- CRC32 covers bytes `0..72`
- `freelist_page_id` persists the freelist root across restarts

See [Files, WAL, and Locking](files-and-locking.md) for main file / `.wal` / `.lock` overview.
