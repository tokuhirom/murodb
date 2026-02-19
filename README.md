# murodb

Design Doc: Encrypted Embedded SQL DB with B-Tree + FTS (Japanese Bigram)

0. Goals

Goals

組み込み用途で使える 単一ファイルDB

透過暗号化（DB/WAL/一時領域含めて平文断片を残さない）

B-tree による

PRIMARY KEY (INT64)

UNIQUE（単一列、後で拡張可能）


FTS（日本語 bigram n=2）

MySQL風 MATCH(col) AGAINST(...)

フレーズ検索（"..."）

スニペット＋ハイライト


トランザクション（ACIDのうち、特にA/D重視）

並行性：Read並列 / Write単一（WAL + single-writer）


Non-goals (MVP)

JOIN / サブクエリ / 複雑なSQL最適化

複合PK・複合UNIQUE

collation（日本語ソートなど）

ネットワーク越しのサーバープロトコル

完全なアクセスパターン秘匿（ORAM等）



---

1. Public SQL Surface

1.1 Types

INT64

VARCHAR

VARBINARY

値として NULL を許容（型は3つでも NULL は必須）


1.2 DDL

CREATE TABLE t (
  id INT64 PRIMARY KEY,
  body VARCHAR,
  blob VARBINARY,
  uniq VARCHAR UNIQUE
);

CREATE UNIQUE INDEX ... ON t(col); -- 追加のUNIQUE/INDEX用（MVPでは単一列）

1.3 FTS DDL

MySQLっぽさ優先で以下を採用：

CREATE FULLTEXT INDEX t_body_fts ON t(body)
  WITH PARSER ngram
  OPTIONS (n=2, normalize='nfkc');

1.4 FTS Query API

-- NATURAL (ranking)
SELECT id, MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) AS score
FROM t
WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0
ORDER BY score DESC
LIMIT 20;

-- BOOLEAN (phrase / +/-)
SELECT id
FROM t
WHERE MATCH(body) AGAINST('"東京タワー" +夜景 -混雑' IN BOOLEAN MODE) > 0;

1.5 Snippet API (独自)

MySQLには標準が弱いので、DB拡張関数として提供：

SELECT
  id,
  fts_snippet(body, '"東京タワー"', '<mark>', '</mark>', 30) AS snippet
FROM t
WHERE MATCH(body) AGAINST('"東京タワー"' IN BOOLEAN MODE) > 0
ORDER BY MATCH(body) AGAINST('"東京タワー"' IN BOOLEAN MODE) DESC
LIMIT 10;

仕様：

fts_snippet(col, query, pre_tag, post_tag, context_chars)

query は AGAINST に渡した文字列と同じでOK



---

2. Concurrency & Locking

2.1 Model

Multiple readers, single writer

Read TX は開始時点のスナップショット（LSN）を見る

Write TX は同時に1つだけ


2.2 Locks (3-layer)

1. Thread-level lock（同一プロセス内）



RWLock：read shared / write exclusive


2. Process-level lock（複数プロセス対応）



OSファイルロック

共有ロック：reader

排他ロック：writer / DDL



3. DB-internal sequencing



WALの追記順序、commit record の原子性、schema世代管理


2.3 DDL Locking

DDLは 完全排他

MVPではオンラインDDLはしない（全トランザクション停止 → 実行）



---

3. Storage Format

3.1 File Layout (single file + WAL file)

dbfile：ページストア（B-tree、メタ等）

walfile：追記ログ（暗号化）


（SQLiteに寄せて、WALは別ファイルが実装も運用も楽。将来単一ファイルに統合は可能だが後回し）

3.2 Pages

固定ページサイズ（例：4096 or 8192）

各ページは暗号化して格納（後述）


3.3 Core B-tree

テーブル本体：rowid(INT64) でクラスタ化（もしくは PRIMARY KEY が INT64 単一ならそれをrowidとして扱う）

secondary index / unique index：同一B-tree実装、is_unique フラグで制約化


キーエンコード：

INT64：符号付きを順序保存するエンコード（big-endian + bias）

VARCHAR：UTF-8 bytes のバイナリ比較（NFKCなどはFTS側だけ。通常比較は生UTF-8）

VARBINARY：bytes


> UNIQUE の後変更を可能にするため、UNIQUEは「B-tree index の属性」に寄せる。




---

4. Transaction, WAL, Crash Recovery

4.1 WAL records (logical view)

BEGIN(txid)

PAGE_PUT(page_id, page_ciphertext)  ※実際は暗号化済みページ

COMMIT(txid, commit_lsn)

ABORT(txid)


4.2 Snapshot

Read TX: snapshot_lsn = current_committed_lsn

Readは snapshot_lsn までのWALを反映した論理状態を読む


4.3 Checkpoint

WALが閾値超え（サイズ or record数）でチェックポイント

チェックポイントは writerが実行（MVP）

将来：低優先度バックグラウンドで自動化


4.4 Atomicity

COMMIT レコードが書けたら確定

COMMIT 前にクラッシュしたTXは無視（redo only）



---

5. Encryption

5.1 Scope

暗号化対象（必須）：

DBページ

WAL

FTS postings / 統計 / pending

一時領域（ソートやインデックス構築で必要なら）


5.2 Primitive

AEAD（認証付き暗号）

AADに「ページID・世代」を入れる


例（概念）：

ciphertext = AEAD_Encrypt(key, nonce, aad=(page_id, epoch), plaintext=page_bytes)


> 目的：改ざん検出 + リプレイ抑止の足場。



5.3 Key Management (MVP)

master key は外部供給（アプリが渡す）

追加で「パスフレーズ → KDF → master key」もサポート（任意）

将来：OSキーチェーン統合（macOS Keychain等）


5.4 Key Rotation (Roadmap item)

epoch を導入して、ページ再暗号化・段階移行を可能にする



---

6. FTS (Japanese Bigram, Commit-time Update)

6.1 Tokenization

normalize: NFKC

ngram: 2-gram

入力 "東京タワー" → 東京 京タ タワ ワー


6.2 Query Language

NATURAL: 演算子なしの文字列 → bigram化 → BM25

BOOLEAN:

"..." phrase（隣接一致）

+term must

-term must-not

空白はAND（MVP）



6.3 Index Data Model

term_id = HMAC(term_key, ngram_bytes)（DB内に平文トークンを残さない）

postings：term_id -> [(doc_id, positions...)]


positions：

ngram index（0,1,2...）として保持

圧縮：delta + varint


6.4 Phrase matching

phrase "東京タワー" は bigram列が連続していること：

pos(東京) と pos(京タ)-1 と pos(タワ)-2 と pos(ワー)-3 の交差があればヒット


6.5 Snippet

文書本文は ブロック単位暗号化（4KB〜16KB）

ヒット位置近辺のブロックのみ復号し、前後 context_chars を切り出してハイライト


（MVPでは pos -> byte_offset を簡易に推定しても良いが、品質のために次のいずれかを採用）

Option A: docごとに pos -> byte_offset の圧縮マップを持つ（暗号化）

Option B: ブロック復号後に局所的に走査してオフセットを探す（実装は簡単だが遅い）


MVP推奨：Option B（まず動かす）→ RoadmapでAへ

6.6 Commit-time update flow

TX内：

変更分を fts_pending に蓄積（add/update/delete） COMMIT時：


1. pending を確定順に適用


2. postingsを term_id ごとにマージ


3. 統計（df/doc_len/avg）更新


4. WALに書く



ROLLBACK時：

pending破棄のみ（FTS本体は触らない）



---

7. UNIQUE constraint evolution

前提：スキーマ変更は自分の制御下のみ、DDLは排他でOK。

UNIQUE追加：全表スキャン → 重複チェック → UNIQUE index 構築

UNIQUE削除：index drop で完了

UNIQUE→non-unique：dropして作り直し（MVP）

NULLの扱い：複数NULLを許可（UNIQUE上でNULLは相互に非衝突）



---

8. Minimal Planner / Execution

MVPのプランナはルールベースで十分：

WHERE id = ? → PK B-tree seek

WHERE col = ? かつ indexあり → index seek

WHERE MATCH(col) AGAINST(...) → FTSノード

ORDER BY score DESC LIMIT N → Top-N（スコア計算しながら）



---

9. Testing Strategy

crash recovery テスト（commit直前/直後/途中でプロセスkill）

WAL再生の整合性

暗号化：改ざん検出（1 byte flipでエラー）

UNIQUE：追加・削除・重複検出

FTS：bigram一致、フレーズ一致、スニペット生成



---

Roadmap

Phase 0 (MVP)

B-tree (table + single-column indexes)

PK(INT64), UNIQUE(single column)

WAL + single writer + process file lock

ページ暗号化 + WAL暗号化（AEAD）

FTS bigram n=2

MATCH/AGAINST NATURAL/BOOLEAN

phrase "...", +/-（最低限）

snippet（Option B: 局所走査）



Phase 1

自動チェックポイント（閾値ベース）

fts_snippet 高速化（Option A: pos→offset map）

FTS stop-ngram（頻出2-gram除外）でサイズ/速度改善

CREATE INDEX（non-unique）を一般化


Phase 2

OSキーチェーン統合（プラットフォーム別）

キーローテーション（epoch導入、段階再暗号化）

複合UNIQUE/複合INDEX（タプルキーエンコード）


Phase 3

JOIN / サブクエリ / もう少しまともなオプティマイザ

オンラインDDL（必要なら）

サーバー組み込み向けAPI（接続プール想定、メトリクス）



---

Open Decisions (後で良いがメモ)

ページサイズ（4KB/8KB）

WALの物理フォーマット（record framing、checksum）

snippetのoffset mapの形式

BOOLEAN MODEの演算子互換（MySQLにどこまで寄せるか）
