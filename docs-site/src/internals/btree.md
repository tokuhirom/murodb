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
