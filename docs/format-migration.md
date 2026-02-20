# Database Format Migration Policy

## Format Versions

### Version 1 (Legacy)
- Header layout: Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B) + PageCount (8B) + Epoch (8B)
- No freelist page ID field
- No header CRC32

### Version 2 (Current)
- Header layout: Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B) + PageCount (8B) + Epoch (8B) + FreelistPageId (8B) + CRC32 (4B)
- Added freelist page ID for persistent freelist storage
- Added CRC32 over bytes 0..60 for header corruption detection

## Auto-Migration: v1 to v2

When MuroDB opens a database file with format version 1:

1. The header is read using v1 layout rules (no CRC validation)
2. `freelist_page_id` defaults to 0 (no persisted freelist)
3. The header is immediately rewritten as v2 with CRC32
4. The file is fsynced to ensure the upgrade is durable

This is a safe, non-destructive upgrade: v1 databases have no freelist page, so defaulting to 0 preserves correctness.

## Forward Incompatibility Policy

MuroDB **rejects** database files with a format version higher than the current `FORMAT_VERSION`. This prevents data corruption from attempting to read formats that require features not yet implemented.

Opening a database with an unsupported future version returns:
```
WAL error: unsupported database format version N
```

Users must upgrade their MuroDB binary to open databases created by newer versions.

## Freelist Multi-Page Chain (v2)

In format version 2, the freelist may span multiple pages linked as a chain:

```
[magic "FLMP": 4B] [next_freelist_page_id: u64] [count_in_this_page: u64] [page_id entries: u64...]
```

- `next_freelist_page_id = 0` indicates end of chain
- Each page holds up to `ENTRIES_PER_FREELIST_PAGE` entries
- The header's `freelist_page_id` points to the first page in the chain
- For backward compatibility, pages without `FLMP` magic are treated as legacy single-page freelist format (`[count:u64][entries...]`)
