# Full-Text Search

MuroDB provides MySQL-compatible full-text search with bigram tokenization.

## Creating a fulltext index

```sql
CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc');
```

## NATURAL LANGUAGE MODE

BM25-based relevance ranking.

```sql
SELECT id, MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) AS score
FROM t
WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0
ORDER BY score DESC
LIMIT 20;
```

## BOOLEAN MODE

Supports `+term` (required), `-term` (excluded), and `"phrase"` (exact phrase) operators.

```sql
SELECT id
FROM t
WHERE MATCH(body) AGAINST('"東京タワー" +夜景 -混雑' IN BOOLEAN MODE) > 0;
```

### Boolean operators

| Operator | Meaning | Example |
|---|---|---|
| `+term` | Term must be present | `+東京` |
| `-term` | Term must not be present | `-混雑` |
| `"phrase"` | Exact phrase match | `"東京タワー"` |
| `term` | Term is optional (contributes to score) | `夜景` |

## Snippet with highlight

Use `fts_snippet()` to generate highlighted excerpts.

```sql
SELECT id,
  fts_snippet(body, '"東京タワー"', '<mark>', '</mark>', 30) AS snippet
FROM t
WHERE MATCH(body) AGAINST('"東京タワー"' IN BOOLEAN MODE) > 0
LIMIT 10;
```

### fts_snippet() parameters

| Parameter | Description |
|---|---|
| column | The column to extract snippet from |
| query | The search query (same as AGAINST) |
| open tag | Opening highlight tag (e.g., `<mark>`) |
| close tag | Closing highlight tag (e.g., `</mark>`) |
| max length | Maximum snippet length in characters |

## Internal design

See [FTS Internals](../internals/fts-internals.md) for implementation details.
