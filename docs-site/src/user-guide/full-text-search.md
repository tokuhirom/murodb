# Full-Text Search

MuroDB provides MySQL-compatible full-text search with bigram tokenization.

## Creating a fulltext index

```sql
CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc', stop_filter=off, stop_df_ratio_ppm=200000);
```

`WITH PARSER` / `OPTIONS` syntax is available so parser variants can be expanded in future releases.

FTS uses an internal `doc_id` mapping, so it works with non-`BIGINT` primary keys too.
If a table has no explicit primary key, MuroDB's hidden `_rowid` is used.

Supported options:

- `n`: ngram size (`2` only for now)
- `normalize`: normalization mode (`'nfkc'` only for now)
- `stop_filter`: `on`/`off` (or `1`/`0`, `'true'`/`'false'`)
- `stop_df_ratio_ppm`: document-frequency threshold in ppm (`0..=1000000`)

`stop_filter` applies to `NATURAL LANGUAGE MODE` only. `BOOLEAN MODE` behavior is unchanged.

## Query semantics

### NATURAL LANGUAGE MODE

- BM25-based relevance ranking.

```sql
SELECT id, MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) AS score
FROM t
WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0
ORDER BY score DESC
LIMIT 20;
```

With stop-ngram filtering enabled:

```sql
CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc', stop_filter=on, stop_df_ratio_ppm=500000);
```

This skips very frequent low-information ngrams during scoring.

### BOOLEAN MODE

Supports `+term` (required), `-term` (excluded), and `"phrase"` (exact phrase).

```sql
SELECT id
FROM t
WHERE MATCH(body) AGAINST('"東京タワー" +夜景 -混雑' IN BOOLEAN MODE) > 0;
```

| Operator | Meaning | Example |
|---|---|---|
| `+term` | Term must be present | `+東京` |
| `-term` | Term must not be present | `-混雑` |
| `"phrase"` | Exact phrase match | `"東京タワー"` |
| `term` | Optional (score contribution) | `夜景` |

## Snippet helper

Use `fts_snippet()` for highlighted excerpts.

```sql
SELECT id,
  fts_snippet(body, '"東京タワー"', '<mark>', '</mark>', 30) AS snippet
FROM t
WHERE MATCH(body) AGAINST('"東京タワー"' IN BOOLEAN MODE) > 0
LIMIT 10;
```

## Recall/precision tradeoff example

Dataset:

- doc1: `東京タワー`
- doc2: `東京駅`
- doc3: `東京大学`
- doc4: `東京ドーム`

Query:

```sql
MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE)
```

Observed behavior:

- `stop_filter=off`: broad recall (multiple `東京*` docs can match)
- `stop_filter=on, stop_df_ratio_ppm=500000`: higher precision (mostly `東京タワー` doc remains)

## Internal design

See [FTS Internals](../internals/fts-internals.md) for implementation details.
