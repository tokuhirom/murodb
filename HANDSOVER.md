# Resilience Hardening Handover

## Active Branch / PR
- Branch: `resilience/hardening-track`
- Draft PR: `https://github.com/tokuhirom/murodb/pull/38`
- 方針: 耐障害性改善をこのブランチに継続的に積み上げる

## Current Focus
- WAL リカバリの堅牢性強化
- checkpoint 失敗時の耐性と可観測性強化
- `murodb-wal-inspect --format json` の機械可読契約の安定化

## Implementation Plan (Next)
1. Checkpoint reliability hardening
- checkpoint retry の backoff 追加（現状は即時リトライ）
- retry 上限到達時のメトリクス化しやすいログフォーマット統一
- commit/rollback 双方の failure-path テスト拡張

2. WAL recovery strictness and salvage
- permissive 復旧時の `skipped` 分類コードを拡張
- tail 破損許容と mid-log 破損拒否の境界ケーステスト追加
- quarantine 後の運用導線（inspect での検出容易化）を改善

3. Inspect JSON contract stabilization
- `status / exit_code / fatal_error_code` を含む JSON 契約の回帰テスト拡張
- JSON schema versioning 運用ルールの明文化（互換性ポリシー）
- fatal 系の全経路が JSON 契約を満たすことを保証

4. Crash simulation coverage
- 疑似クラッシュシナリオ（途中中断・再起動）を増やす
- recovery 後の catalog / page_count / data 整合性を検証
- 長時間運用相当（多数 transaction）で WAL 周りの健全性確認

5. TLA+ feedback loop
- 実装変更に合わせて `specs/tla/CrashResilience.tla` の不変条件を更新
- 追加した failure mode を TLA モデルに反映
- モデル上の反例を実装テストに落とし込む

## Definition of Done (for this hardening track)
- CI（build/test/clippy/fmt）緑
- 主要 failure-path がテストで固定されている
- `docs/crash-resilience.md` が実装と一致
- `murodb-wal-inspect` JSON 契約が機械利用で安定

## Working Agreement
- 小粒で止めず、関連変更をまとめた中〜大粒コミットを継続
- 重要変更は先にテストを足してから実装
- Draft PR #38 に積み上げ、区切りで ready 化
