# Full-Text Search

MuroDB has an internal FTS engine (bigram + BM25 + boolean query parser), but the SQL DDL path is still being integrated.

## Current status

- SQL syntax for `CREATE FULLTEXT INDEX` is parsed.
- In the current release, executing `CREATE FULLTEXT INDEX ...` through SQL returns an execution error (integration in progress).
- FTS is currently usable through the Rust API in `murodb::fts`.

## Rust API workflow

```rust
use murodb::fts::index::{FtsIndex, FtsPendingOp};
use murodb::fts::query::{query_boolean, query_natural};

let mut idx = FtsIndex::create(&mut pager, term_key)?;

idx.apply_pending(&mut pager, &[
    FtsPendingOp::Add { doc_id: 1, text: "東京タワーの夜景".into() },
    FtsPendingOp::Add { doc_id: 2, text: "京都の金閣寺".into() },
])?;

let natural = query_natural(&idx, &mut pager, "東京タワー")?;
let boolean = query_boolean(&idx, &mut pager, "\"東京タワー\" +夜景 -混雑")?;
```

## Query semantics

### NATURAL mode

- BM25-based relevance ranking.

### BOOLEAN mode

Supports `+term` (required), `-term` (excluded), and `"phrase"` (exact phrase).

| Operator | Meaning | Example |
|---|---|---|
| `+term` | Term must be present | `+東京` |
| `-term` | Term must not be present | `-混雑` |
| `"phrase"` | Exact phrase match | `"東京タワー"` |
| `term` | Optional (score contribution) | `夜景` |

## Snippet helper

Use `murodb::fts::snippet::fts_snippet()` for highlighted excerpts.

```rust
use murodb::fts::snippet::fts_snippet;
let s = fts_snippet("東京タワーの夜景がきれい", "\"東京タワー\" +夜景", "<mark>", "</mark>", 30);
```

## Internal design

See [FTS Internals](../internals/fts-internals.md) for implementation details.
