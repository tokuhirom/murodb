# Reading Guide

This page defines how to use the internals docs as a long-term memory aid.

## Current Focus Areas

If you return after weeks/months, start from these questions:

1. How is data physically laid out on disk?
2. How is a B+tree node stored inside a page?
3. How does a query become a concrete access path?
4. What is the exact `.wal` format and recovery state machine?
5. What does `.lock` lock, and at what granularity?
6. Which cryptographic primitives are used, and why these choices?

## Target Documentation Shape

The internals section now follows this order:

1. [Architecture](architecture.md): module map and end-to-end data flow.
2. [Files, WAL, and Locking](files-and-locking.md): main file / `.wal` / `.lock` contract.
3. [B-tree](btree.md): in-page node format and mutation/scan algorithms.
4. [Query Planning & Execution](query-planning.md): plan selection and execution mapping.
5. [Cryptography](cryptography.md): encryption/KDF details and rationale.
6. [WAL & Crash Resilience](wal.md): transaction protocol and recovery validation.
7. [Durability Matrix](durability-matrix.md): crash-at-each-step outcomes.

## Suggested Rebuild Path (for implementers)

If you are implementing an embedded RDBMS with an LLM agent, use this order:

1. Implement fixed-size pages and a pager (`storage/page.rs`, `storage/pager/mod.rs`).
2. Implement B+tree on top of slotted pages (`btree/node.rs`, `btree/ops/mod.rs`).
3. Add SQL parser/planner/executor (`sql/parser/*`, `sql/planner.rs`, `sql/executor/*`).
4. Add WAL append + replay (`wal/writer.rs`, `wal/reader.rs`, `wal/recovery.rs`).
5. Add lock manager for thread/process safety (`concurrency/mod.rs`).
6. Add encryption suite and key derivation (`crypto/*`).

Each chapter in this internals section maps directly to those steps.
