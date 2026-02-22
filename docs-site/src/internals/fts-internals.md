# FTS Internals

## Tokenization

- **Normalization**: NFKC unicode normalization
- **Tokenizer**: Bigram (n=2) - each text is split into overlapping 2-character sequences
- Example: "東京タワー" → ["東京", "京タ", "タワ", "ワー"]

## Term ID Blinding

Term IDs are computed using HMAC-SHA256:

- No plaintext tokens are stored on disk
- Term ID = HMAC-SHA256(master_key, normalized_token)
- This provides privacy: the disk contents do not reveal what terms are indexed

## Postings Storage

Postings lists are stored in B-tree with compression:

- **Delta encoding**: Document IDs are stored as deltas from the previous ID
- **Varint compression**: Deltas are encoded as variable-length integers
- Postings are stored in the same B-tree infrastructure as regular data

## Scoring

- **Algorithm**: BM25 (Okapi BM25)
- Used in NATURAL LANGUAGE MODE for relevance ranking

## Phrase Matching

Phrase queries (e.g., `"東京タワー"`) verify consecutive bigram positions:

1. Tokenize the phrase into bigrams
2. Find postings for each bigram
3. Verify that positions are consecutive across all bigrams

## Snippet Generation

`fts_snippet()` uses a local scan approach with a UTF-8 offset map:

1. Find matching positions in the document
2. Build a char<->byte offset map for normalized text
3. Convert match byte offsets to char windows via binary search
4. Slice and apply highlight tags (open/close) around matched regions
5. Truncate to the specified maximum length

Memory note:

- Offset map size is bounded by `(normalized_chars + 1) * sizeof(usize)` bytes per call.
