# B-tree

## Overview

MuroDB uses B-tree as the primary index structure. Both primary (clustered) and secondary indexes share the same B-tree implementation.

## Key Encoding

Keys are encoded for order-preserving binary comparison:

- **Integer types** (TINYINT, SMALLINT, INT, BIGINT): Big-endian encoding with sign bit flip to preserve sort order
- **VARCHAR / VARBINARY**: Raw bytes

This encoding allows the B-tree to use simple byte comparison for key ordering.

## Operations

- **Insert**: Standard B-tree insert with node splitting when full
- **Delete**: Remove key-value pair from leaf nodes
- **Search**: Point lookup by key
- **Scan**: Range scan with cursor-based iteration

## Clustered Index

Tables are clustered by PRIMARY KEY. The row data is stored directly in the primary B-tree's leaf nodes.

## Secondary Indexes

Secondary indexes map column values to primary key values:

- `CREATE INDEX` creates a non-unique secondary index
- `CREATE UNIQUE INDEX` creates a unique secondary index with duplicate checking
- Secondary index entries: `(column_value) → (primary_key)`
