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
       → WAL に Begin + PagePut + Commit レコード書き込み
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
    3. WAL に Commit レコード書き込み
    4. wal.sync()                      ← WAL を fsync
    5. dirty pages をデータファイルに書き込み
    6. pager.flush_meta()              ← データファイルを fsync

ROLLBACK
  → tx.rollback(&mut wal)             ← WAL に Abort レコード書き込み
  → カタログをディスクから再読み込み
```

### WAL Recovery（Database::open 時）

```
Database::open(path, master_key)
  1. WAL ファイルが存在すれば recovery::recover() を実行
     → WAL をスキャンし committed/aborted トランザクションを識別
     → committed トランザクションの最新ページイメージを収集
     → データファイルにリプレイ
  2. WAL ファイルをトランケート（空にする）
  3. Pager + Catalog + WalWriter を生成して Session を構築
```

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
