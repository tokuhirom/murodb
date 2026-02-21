# MuroDB - Development Guide

## Build & Test

```bash
cargo build          # ビルド
cargo test           # 全テスト実行
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

## Documentation

ドキュメントは以下の構成。機能追加時には全て更新を検討すること。

- **`README.md`** — プロジェクト概要、簡易リファレンス、ロードマップ
- **`docs-site/`** — mdBook ベースのドキュメントサイト (ユーザーズマニュアル + 内部構造解説)
  - User Guide (ユーザーズマニュアル):
    - `src/user-guide/sql-reference.md` — SQL 構文リファレンス (型、DDL、DML、関数、集約、JOIN等)
    - `src/user-guide/cli.md` — CLI オプション
    - `src/user-guide/full-text-search.md` — 全文検索ガイド
    - `src/user-guide/recovery.md` — リカバリ手順
  - Internals (内部構造解説):
    - `src/internals/architecture.md` — アーキテクチャ概要
    - `src/internals/storage.md` — ストレージエンジン
    - `src/internals/btree.md` — B-tree 実装
    - `src/internals/wal.md` — WAL・クラッシュリカバリ設計
    - `src/internals/fts-internals.md` — 全文検索エンジン内部
    - `src/internals/format-migration.md` — フォーマットマイグレーション
  - `src/roadmap.md` — ロードマップ
- **`docs/`** — 旧技術ドキュメント (crash-resilience, format-migration)

機能追加時の更新チェックリスト:
1. `docs-site/src/user-guide/sql-reference.md` に新構文のドキュメントを追加
2. `docs-site/src/roadmap.md` の該当項目を `[x]` に更新
3. `README.md` の SQL Surface セクションと Roadmap も同期更新
4. 内部構造に影響する変更 (WAL、ストレージ、B-tree等) は `docs-site/src/internals/` に追記・新規作成

## Git Workflow

main ブランチに直接コミットしないこと。必ずフィーチャーブランチを作成し、PR 経由でマージする。

## Pre-commit Review Rule

コミット前に必ず、DB/SQL専門家のsubagent (subagent_type=general-purpose) を起動してレビューを実施すること。
subagent には「あなたはDB/SQLの専門家です」というペルソナを与え、`git diff --staged` の内容を渡して以下の観点でレビューさせる:

1. **耐障害性**: クラッシュリカバリ、データ整合性、WAL の正しさ、エラーハンドリング
2. **MySQL互換性**: MySQL の挙動・構文との互換性 (MuroDBはMySQL互換を目指す)
3. **SQL標準互換性**: SQL標準 (ISO/IEC 9075) との準拠度
4. **ユーザビリティ**: エラーメッセージのわかりやすさ、APIの直感性、ドキュメントとの整合性

レビューで問題が指摘された場合は、修正してから再レビューを行うこと。
レビューで問題がなければ、そのままコミットして良い。

## Known Limitations

- Posting list が 4096B ページを超えるとエラー (大量文書の共通bigramで発生しうる)
- Subquery は非相関サブクエリのみ対応 (外部行参照は未対応)
- ALTER TABLE ADD/DROP PRIMARY KEY 未対応
- ALTER TABLE はトランザクション非対応 (DDL全般と同様)
- ON DUPLICATE KEY UPDATE で VALUES() 関数は未対応 (INSERT値の参照不可)
- EXPLAIN は SELECT のみ対応、JOIN/サブクエリ含む場合は1行のみ出力
