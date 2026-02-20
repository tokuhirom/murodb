# Storage

## Page Layout

- **Page size**: 4096 bytes (slotted page layout)
- **Encryption**: Each page encrypted with AES-256-GCM-SIV
  - AAD (Additional Authenticated Data) = (page_id, epoch)
- **Cache**: LRU page cache (default 256 pages)

## Encryption

All data at rest is encrypted:

- **Algorithm**: AES-256-GCM-SIV (nonce-misuse resistant AEAD)
- **KDF**: Argon2 derives the master key from the user's passphrase + random salt
- **Per-page encryption**: Each page is independently encrypted with its own nonce
- **AAD binding**: page_id and epoch are bound as additional authenticated data, preventing page swap attacks

## Freelist

Freed pages are tracked in a freelist for reuse:

- Stored as a multi-page chain on disk
- Each page has the format: `[magic "FLMP": 4B] [next_freelist_page_id: u64] [count: u64] [page_id entries: u64...]`
- `next_freelist_page_id = 0` indicates end of chain
- The header's `freelist_page_id` points to the first page in the chain
- Legacy single-page format (without `FLMP` magic) is supported for backward compatibility

## Data File Header

Format v2 header layout:

```
Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B)
+ PageCount (8B) + Epoch (8B) + FreelistPageId (8B) + CRC32 (4B)
```

- CRC32 covers bytes 0..60 for header corruption detection
- `freelist_page_id` persists the freelist root across restarts
