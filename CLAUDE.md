# MuroDB - Development Guide

## Build & Test

```bash
cargo build          # ビルド
cargo test           # 全テスト実行 (135 tests)
cargo test <name>    # 特定テスト実行 (例: cargo test fts)
cargo clippy         # lint
```

## Architecture

暗号化組み込みSQL DB。レイヤー構成:

```
sql/ (lexer → parser → planner → executor)
  ↓
schema/ (catalog: テーブル/インデックス定義)
  ↓
tx/ (トランザクション: dirty page buffer, commit/rollback)
  ↓
btree/ (B-tree: insert/split, delete, search, scan)
  ↓
wal/ (WAL: 暗号化レコード, crash recovery)
  ↓
storage/ (pager: 暗号化ページI/O, LRU cache, freelist)
  ↓
crypto/ (AES-256-GCM-SIV, Argon2 KDF, HMAC-SHA256)
```

- `fts/` - 全文検索 (bigram tokenizer, postings B-tree, BM25, BOOLEAN/NATURAL mode)
- `concurrency/` - parking_lot::RwLock (スレッド) + fs4 file lock (プロセス)

## Key Design Decisions

- **ページサイズ**: 4096B (slotted page layout)
- **暗号化**: 全ページ AES-256-GCM-SIV, AAD = (page_id, epoch)
- **SQLパーサー**: nom + 自前再帰下降 (MATCH/AGAINST, fts_snippet等の独自構文対応)
- **FTS term ID**: HMAC-SHA256 blinded (ディスク上に平文トークンなし)
- **Postings**: delta + varint圧縮, B-tree格納

## Module Map

| Module | Files | Role |
|---|---|---|
| `storage/` | page.rs, pager.rs, freelist.rs | 4096B暗号化ページI/O |
| `crypto/` | aead.rs, kdf.rs, hmac_util.rs | 暗号化プリミティブ |
| `btree/` | node.rs, ops.rs, cursor.rs, key_encoding.rs | B-tree操作 |
| `wal/` | record.rs, writer.rs, reader.rs, recovery.rs | WAL + crash recovery |
| `tx/` | transaction.rs, lock_manager.rs | トランザクション |
| `schema/` | catalog.rs, column.rs, index.rs | システムカタログ |
| `sql/` | lexer.rs, parser.rs, ast.rs, planner.rs, executor.rs, eval.rs | SQL処理 |
| `fts/` | tokenizer.rs, postings.rs, index.rs, query.rs, scoring.rs, snippet.rs | 全文検索 |
| `concurrency/` | mod.rs | 並行性制御 |

## Known Limitations

- Posting list が 4096B ページを超えるとエラー (大量文書の共通bigramで発生しうる)
- JOIN / subquery 未対応
- Composite PK / Composite UNIQUE 未対応
