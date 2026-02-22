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

- Stored as a multi-page chain on disk
- Each page has the format: `[magic "FLMP": 4B] [next_freelist_page_id: u64] [count: u64] [page_id entries: u64...]`
- `next_freelist_page_id = 0` indicates end of chain
- The header's `freelist_page_id` points to the first page in the chain
- Legacy single-page format (without `FLMP` magic) is supported for backward compatibility

## Data File Header

Main DB file header (`src/storage/pager/mod.rs`) is 76 bytes:

```
Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B)
+ PageCount (8B) + Epoch (8B) + FreelistPageId (8B)
+ NextTxId (8B) + EncryptionSuiteId (4B) + CRC32 (4B)
```

- CRC32 covers bytes `0..72`
- `freelist_page_id` persists the freelist root across restarts

See [Files, WAL, and Locking](files-and-locking.md) for `.db` / `.wal` / `.lock` overview.
