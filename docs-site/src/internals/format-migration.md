# Format Migration

## Current Policy (as of 2026-02-22)

MuroDB supports **database format v4 only**.

- Opening v4 works.
- Opening v1/v2/v3 is rejected.
- Opening future versions (>v4) is also rejected.

This project currently has no production users on pre-v4 formats, so compatibility-migration code is intentionally removed to keep core storage logic simple and safer.

## v4 Header Layout

```
Magic (8B) + Version (4B) + Salt (16B) + CatalogRoot (8B)
+ PageCount (8B) + Epoch (8B) + FreelistPageId (8B)
+ NextTxId (8B) + EncryptionSuiteId (4B) + CRC32 (4B)
```

- Header size: 76 bytes
- CRC32 covers bytes `0..72`

## Rejection Behavior

Opening an unsupported version returns:

```
WAL error: unsupported database format version N
```

## WAL Format Version History

| Version | Changes |
|---|---|
| v1 | Initial: Begin, PagePut, MetaUpdate(catalog_root, page_count), Commit, Abort |
| v2 | MetaUpdate adds `freelist_page_id` field. Legacy v1 MetaUpdate (25 bytes) decoded with `freelist_page_id=0` (backward compatible) |
| v3 | MetaUpdate adds `epoch` field. Legacy v1/v2 MetaUpdate records decode with `epoch=0` (backward compatible) |
