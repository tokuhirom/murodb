# B-tree

## Overview

MuroDB uses one B+tree implementation (`src/btree/*`) for:

- table primary data (clustered by primary key)
- secondary B-tree indexes
- some internal metadata trees

Core handle is `BTree { root_page_id }` in `src/btree/ops/mod.rs`.

## Is B+tree Stored Inside Pages?

Yes. A B+tree node is encoded directly inside one slotted page (`src/storage/page.rs` + `src/btree/node.rs`).

Page structure:

1. Slotted-page header/pointer area (generic storage layer)
2. Cell payloads (B+tree node header + entries)

So, "page" is storage primitive, and B+tree node format is layered on top of it.

## Node Layout on Page

`src/btree/node.rs` defines:

- Cell `0` is always a node header.
- Leaf header payload: `[node_type=1]`
- Internal header payload: `[node_type=2][right_child: u64]`

Leaf entry cell:

- `[key_len: u16][key bytes][value bytes]`

Internal entry cell:

- `[left_child: u64][key_len: u16][key bytes]`

Internal nodes store `N` separator keys and `N+1` child pointers:

- `left_child` in each entry (N pointers)
- `right_child` in header (last pointer)

## Key/Value Semantics by Tree Type

### Primary data tree

- key: encoded primary key bytes
- value: serialized row bytes

### Unique secondary index

- key: encoded indexed column(s)
- value: primary key bytes

### Non-unique secondary index

- key: `index_key || primary_key` (appended PK disambiguates duplicates)
- value: primary key bytes

That encoding is implemented in `src/sql/executor/indexing.rs`.

## Key Encoding Rules

Order-preserving encoding is in `src/btree/key_encoding.rs`.

- signed integers: sign-bit flip + big-endian
- float/double: order-preserving bit transform
- composite keys: null marker + per-column encoding
- variable-length components: byte-stuffed terminator scheme

Because encoded bytes preserve logical order, tree comparison is plain lexicographic byte compare.

## Search and Scan Behavior

### Point lookup

`BTree::search` walks internal nodes with separator comparison (`find_child`) until leaf, then linear-searches leaf cells.

### Full scan

There are no leaf sibling links.  
`BTree::scan` performs recursive in-order traversal from root:

- visit each internal left subtree in key order
- then rightmost subtree

### Range scan (`>= start_key`)

`BTree::scan_from` prunes early subtrees then falls back to in-order traversal for remaining branches.

## Insert Path

`BTree::insert` behavior:

1. Descend to target leaf.
2. Rebuild leaf page with new/updated cell in sorted position.
3. If overflow, split node and return median separator upward.
4. Parent inserts new separator; parent may split recursively.
5. If root splits, allocate new internal root.

## What Happens If It Does Not Fit in One Page?

Two different cases:

1. Tree growth case (many entries):
   - normal behavior is page split (leaf/internal), with separator propagation up to root.
   - this is fully supported.
2. Single-cell-too-large case (one key/value entry itself is huge):
   - value-only overflow pages store large values that exceed page capacity.
   - keys remain inline (max ~4,071 bytes); values spill to overflow page chains.

## Overflow Pages

When a leaf cell (key + value) exceeds ~4,073 bytes, the value is stored in an overflow page chain.

### Overflow cell format

Normal leaf cell: `[key_len: u16][key][value]`

Overflow leaf cell: `[key_len|0x8000: u16][key][total_value_len: u32][first_overflow_page: u64]`

The high bit of `key_len` (0x8000) signals an overflow cell. All value data is stored in the overflow chain; the leaf cell contains only the key and metadata pointer.

### Overflow page layout

Overflow pages use a simple linked-list format (not slotted pages):

```
[page_id: u64]       bytes 0..8   (standard page header)
[0xFF marker: u8]    byte 8       (distinguishes from B-tree nodes)
[next_page: u64]     bytes 9..17  (next page in chain, u64::MAX = end)
[chunk_len: u16]     bytes 17..19 (length of data in this page)
[chunk data]         bytes 19..   (up to 4,077 bytes per page)
```

### Operations

- **Insert**: if `needs_overflow(key, value)`, write value to overflow chain, store chain head pointer in leaf cell.
- **Search/Scan**: on overflow cell, call `read_overflow_chain` to reconstruct the full value.
- **Delete**: free all overflow pages before removing the leaf cell.
- **Update**: free old overflow chain (if any), then create new cell (inline or overflow).
- **Split/Merge**: work with raw cell bytes to preserve overflow pointers without touching overflow data.
- **collect_all_pages**: includes overflow page IDs for each overflow cell.

### Implementation

- `src/storage/overflow.rs`: `write_overflow_chain`, `read_overflow_chain`, `free_overflow_chain`, `collect_overflow_pages`
- `src/btree/node.rs`: `needs_overflow`, `encode_overflow_leaf_cell`, `is_overflow_cell`, `decode_overflow_metadata`
- `src/btree/ops/mod.rs`: overflow-aware insert/search/scan/delete/split/merge

## Delete/Rebalance Path

`BTree::delete` removes target entry and handles underflow:

- if leaf underfull, attempt merge/rebalance with sibling
- if root internal ends with zero entries, collapse root to its only child

Current rebalance path focuses on practical leaf merges; behavior is intentionally conservative.

## Practical Mental Model

If you are rebuilding this design:

1. Implement slotted page first.
2. Reserve cell `0` as node metadata.
3. Keep key bytes order-preserving so comparison is simple.
4. Start with split-only insert and basic delete, then add rebalance.
5. Keep scan correctness independent of leaf links (recursive in-order works immediately).
