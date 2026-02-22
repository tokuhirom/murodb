# Storage

## Data File Structure (At a Glance)

The main `.db` file is:

`[plaintext file header (76B)] [page 0 on disk] [page 1 on disk] [page 2 on disk] ...`

- File header is always plaintext and fixed-size.
- Each page is a logical 4096-byte page (`PAGE_SIZE`) stored at:
  - `offset = 76 + page_id * page_size_on_disk`
- `page_size_on_disk` is:
  - plaintext mode: `4096`
  - encrypted mode: `12 (nonce) + 4096 (ciphertext) + 16 (tag) = 4124`

This chapter first explains the file header, then the page layout, then special page formats (freelist).

## Data File Header

Main DB file header (`src/storage/pager/mod.rs`) is 76 bytes:

```
0..8    Magic "MURODB01"
8..12   Format version (u32 LE)
12..28  Salt (16B, Argon2 input)
28..36  Catalog root page ID (u64 LE)
36..44  Page count (u64 LE)
44..52  Epoch (u64 LE)
52..60  Freelist page ID (u64 LE, 0 = none)
60..68  Next TxId (u64 LE)
68..72  Encryption suite ID (u32 LE)
72..76  CRC32 over bytes 0..72
```

- `freelist_page_id` persists the freelist root across restarts.
- CRC32 protects header integrity before any page decryption.
- This header exists once per file; everything after this is page data.
- `catalog_root` points to the system catalog B-tree root (format in [Catalog Format](catalog-format.md)).

See [Files, WAL, and Locking](files-and-locking.md) for `.db` / `.wal` / `.lock` lifecycle.

## Generic Page Layout

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

Freed pages are tracked in a freelist for reuse.

## Freelist In-Memory Model

Implementation (`src/storage/freelist.rs`) uses `Vec<PageId>`:

- `allocate()` pops from tail (LIFO reuse)
- `free(page_id)` pushes if not already present
- duplicate `free` is treated as double-free and rejected
- `undo_last_free()` exists for speculative commit-time calculations

## Freelist On-Disk Format

Freelist is stored in normal data pages, linked as a chain.

Per freelist page data area (after the generic 14-byte page header):

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
