# Crash Resilience Design

## Current Status

**MuroDB は WAL ベースのクラッシュ耐性を持つ。** すべての書き込みは WAL を経由し、クラッシュ後のリカバリが可能。

## Write Path の概要

### Auto-Commit モード（BEGIN なし）

各 SQL ステートメントが暗黙のトランザクションで囲まれる:

```
Session::execute_auto_commit(stmt)
  1. Transaction::begin(txid, snapshot_lsn)
  2. TxPageStore を作成（dirty page buffer）
  3. execute_statement(stmt, tx_page_store, catalog)
       → BTree::insert(tx_page_store, key, value)
         → TxPageStore::write_page()
           → Transaction::write_page()  ← HashMap に保存（メモリのみ）
  4. tx.commit(&mut pager, &mut wal)    ← WAL-first commit
       → WAL に Begin + PagePut + MetaUpdate + Commit レコード書き込み
       → wal.sync()                     ← WAL を fsync
       → dirty pages をデータファイルに書き込み
       → pager.flush_meta()             ← データファイルを fsync
  (エラー時: tx.rollback_no_wal() + カタログ復元)
```

### 明示トランザクション（BEGIN ... COMMIT）

```
BEGIN
  → Transaction::begin(txid, wal.current_lsn())

exec_insert() / exec_update() / exec_delete()
  → TxPageStore 経由で dirty page buffer に書き込み

COMMIT
  → tx.commit(&mut pager, &mut wal)   ← WAL-first commit
    1. WAL に Begin レコード書き込み
    2. WAL に各 dirty page の PagePut レコード書き込み
    3. WAL に MetaUpdate（catalog_root, page_count）書き込み
    4. WAL に Commit レコード書き込み
    5. wal.sync()                      ← WAL を fsync
    6. dirty pages をデータファイルに書き込み
    7. pager.flush_meta()              ← データファイルを fsync

ROLLBACK
  → tx.rollback(&mut wal)             ← WAL に Abort レコード書き込み
  → カタログをディスクから再読み込み
```

### WAL Recovery（Database::open 時）

```
Database::open(path, master_key)
  1. WAL ファイルが存在すれば recovery::recover() を実行
     → WAL をスキャンしトランザクション遷移を検証
       (Begin 前の PagePut/MetaUpdate/Commit/Abort を拒否)
       (Commit/Abort 後の追加レコードを拒否)
       (Commit.lsn と実LSNの不一致を拒否)
     → committed トランザクションの最新ページイメージを収集
     → データファイルにリプレイ
  2. WAL ファイルをトランケート（空にする）
     → WAL ファイルを fsync
     → 親ディレクトリを best-effort fsync
  3. Pager + Catalog + WalWriter を生成して Session を構築
```

### Recovery Mode（strict / permissive）

- `strict`（デフォルト）:
  - WAL プロトコル不整合を検出したら `open` を失敗させる
  - 例: Begin 前レコード、Commit LSN 不一致、終端重複、PagePut 整合性不一致
- `permissive`:
  - 不正なトランザクションを無視し、有効な committed トランザクションのみ復旧する
  - 破損環境からの救出用途（調査・緊急復旧）向け
  - `RecoveryResult.skipped` で無視した txid と理由を取得できる
  - `skipped` がある場合、元 WAL は `*.wal.quarantine.*` に退避される

API:

- `Database::open(path, key)` は strict
- `Database::open_with_recovery_mode(path, key, RecoveryMode::Permissive)` で permissive を選択可能
- `Database::open_with_recovery_mode_and_report(...)` で recovery report を取得可能
- CLI: `murodb <db> --inspect-wal <wal> --recovery-mode permissive` で WAL 診断のみ実行可能
- CLI: `--format json` で WAL 診断結果を機械可読形式で出力可能
  - JSON は `schema_version=1` を含み、`skipped[].code` で機械向け分類を提供
- `--inspect-wal` の終了コード規約:
  - `0`: 問題なし
  - `10`: malformed tx 検出（診断成功）
  - `20`: 致命エラー（復号失敗/IOエラー/strict検証失敗など）

## TLA+ と実装の対応

TLA+ モデルで使っている中心不変条件を、実装側で明示チェックする構成にした。

| TLA+ 側の意図 | 実装での担保 | 回帰テスト |
|---|---|---|
| `Init -> Begin -> ... -> Commit/Abort` 以外は無効 | `src/wal/recovery.rs` で状態遷移検証（Begin 前レコード拒否） | `test_recovery_rejects_pageput_before_begin` |
| `Commit/Abort` は終端（終端後遷移禁止） | 同一 txid の重複終端/終端後レコードを拒否 | `test_recovery_rejects_duplicate_terminal_record_for_tx` |
| Commit は整合した終端情報を持つ | `Commit.lsn == 実LSN` を検証し不一致は拒否 | `test_recovery_rejects_commit_lsn_mismatch` |
| Commit にはメタデータ確定が必要 | `MetaUpdate` なし Commit を拒否 | `test_recovery_rejects_commit_without_meta_update` |
| PagePut は対象ページIDと内容が一致する | `PagePut.page_id` とページヘッダ `page_id` の不一致を拒否 | `test_recovery_rejects_pageput_page_id_mismatch` |
| tail 破損は許容、mid-log 破損は拒否 | `src/wal/reader.rs` で tail のみ許容、途中破損はエラー | `test_tail_truncation_tolerated`, `test_mid_log_corruption_is_error` |
| 異常フレーム長で安全性を落とさない | WAL フレーム長上限チェックを導入 | `test_oversized_tail_frame_tolerated` |

この対応により、TLA+ で想定した「有効な遷移のみを復旧対象にする」方針を実装でも強制している。

## セカンダリインデックスの整合性

### INSERT

```
exec_insert()
  → データ B-tree に行を挿入
  → 各セカンダリインデックスにエントリを挿入（カラム値 → PK キー）
  → UNIQUE インデックスの場合、挿入前に重複チェック
```

### DELETE

```
exec_delete()
  → スキャンで削除対象の行（PK キー + 全カラム値）を収集
  → 各行について:
    → 各セカンダリインデックスからエントリを削除
    → データ B-tree から行を削除
```

### UPDATE

```
exec_update()
  → スキャンで更新対象の行（PK キー + 旧カラム値）を収集
  → 各行について:
    → 新しい値を計算
    → UNIQUE インデックスの重複チェック（値が変更された場合）
    → 各セカンダリインデックスの更新（値が変更された場合）:
      → 旧エントリ削除 + 新エントリ挿入
    → データ B-tree に新しい行データを書き込み
```

## 残存する制約

### 1. fsync の粒度

`Pager::write_page_to_disk()` は個別に `sync_all()` を呼ばない。
`flush_meta()` のみが `sync_all()` を呼ぶ。
WAL の `sync()` がデータ永続性を保証するため、通常運用では問題ない。

### 2. allocate_page のカウント管理

`Pager::allocate_page()` はメモリ上の `page_count` をインクリメントするが、
`flush_meta()` が呼ばれるまでディスクに永続化されない。
WAL commit 後の `flush_meta()` で永続化される。

### 3. WAL ファイルサイズ

WAL は `Database::open()` 時にトランケートされるが、
長時間のセッション中に WAL が肥大化する可能性がある。
チェックポイント機構（WAL の定期的なトランケート）は未実装。

## 関連コード

| ファイル | 役割 |
|---|---|
| `src/lib.rs` | Database: Session ベースの構成、WAL recovery 統合 |
| `src/sql/session.rs` | Session: WalWriter 保持、auto-commit 暗黙 TX、BEGIN/COMMIT/ROLLBACK |
| `src/tx/transaction.rs` | Transaction: dirty page buffer、WAL-aware commit/rollback |
| `src/tx/page_store.rs` | TxPageStore（dirty buffer 経由の PageStore） |
| `src/storage/pager.rs` | Pager: 暗号化ページ I/O、flush_meta |
| `src/sql/executor.rs` | SQL 実行: INSERT/DELETE/UPDATE のセカンダリインデックス管理 |
| `src/wal/writer.rs` | WAL 書き込み |
| `src/wal/recovery.rs` | WAL リカバリ |
| `src/btree/ops.rs` | B-tree insert/split/delete（PageStore 経由で書き込み） |
