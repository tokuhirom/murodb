use murodb::crypto::aead::MasterKey;
use murodb::fts::index::{FtsIndex, FtsPendingOp};
use murodb::fts::query::{query_boolean, query_natural};
use murodb::fts::snippet::fts_snippet;
use murodb::fts::tokenizer::tokenize_bigram;
use murodb::storage::pager::Pager;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn term_key() -> [u8; 32] {
    [0x55u8; 32]
}

fn setup_fts(docs: &[(u64, &str)]) -> (Pager, FtsIndex, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    let ops: Vec<FtsPendingOp> = docs
        .iter()
        .map(|(id, text)| FtsPendingOp::Add {
            doc_id: *id,
            text: text.to_string(),
        })
        .collect();
    idx.apply_pending(&mut pager, &ops).unwrap();

    (pager, idx, dir)
}

#[test]
fn test_japanese_bigram_tokenization() {
    let tokens = tokenize_bigram("東京タワーで夜景を見た");
    let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
    assert_eq!(
        texts,
        vec!["東京", "京タ", "タワ", "ワー", "ーで", "で夜", "夜景", "景を", "を見", "見た"]
    );
}

#[test]
fn test_natural_language_search() {
    let (mut pager, idx, _dir) = setup_fts(&[
        (1, "東京タワーは東京の有名な観光スポットです"),
        (2, "京都の金閣寺は美しい"),
        (3, "東京スカイツリーは高い"),
        (4, "大阪城は歴史的な建物です"),
    ]);

    let results = query_natural(&idx, &mut pager, "東京タワー").unwrap();
    assert!(!results.is_empty());

    // Doc 1 should rank highest (contains "東京タワー")
    assert_eq!(results[0].doc_id, 1);
}

#[test]
fn test_boolean_search_must_and_must_not() {
    let (mut pager, idx, _dir) = setup_fts(&[
        (1, "東京の夜景がきれい"),
        (2, "東京は混雑している"),
        (3, "大阪の夜景もきれい"),
    ]);

    // +東京 -混雑: must contain 東京, must not contain 混雑
    let results = query_boolean(&idx, &mut pager, "+東京 -混雑").unwrap();
    let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
    assert!(doc_ids.contains(&1));
    assert!(!doc_ids.contains(&2));
}

#[test]
fn test_phrase_search() {
    let (mut pager, idx, _dir) = setup_fts(&[
        (1, "東京タワーに行きたい"),
        (2, "タワーの東京ビュー"), // Not "東京タワー" as a phrase
        (3, "東京タワーから見える景色"),
    ]);

    let results = query_boolean(&idx, &mut pager, "\"東京タワー\"").unwrap();
    let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
    assert!(doc_ids.contains(&1));
    assert!(doc_ids.contains(&3));
}

#[test]
fn test_snippet_highlight() {
    let text = "今日は天気がいいので東京タワーに行きました。とても楽しかったです。";
    let snippet = fts_snippet(text, "東京タワー", "<mark>", "</mark>", 5);

    assert!(snippet.contains("<mark>東京タワー</mark>"));
    // Should have context around the match
    assert!(snippet.len() > "<mark>東京タワー</mark>".len());
}

#[test]
fn test_snippet_with_boolean_query() {
    let text = "東京タワーの夜景が素晴らしいです";
    let snippet = fts_snippet(text, "\"東京タワー\" +夜景", "<b>", "</b>", 10);

    // Should find "東京タワー" or "夜景" and highlight
    assert!(snippet.contains("<b>"));
}

#[test]
fn test_fts_update_document() {
    let (mut pager, mut idx, _dir) = setup_fts(&[
        (1, "古い内容です"),
    ]);

    // Remove old content and add new
    idx.apply_pending(&mut pager, &[
        FtsPendingOp::Remove { doc_id: 1, text: "古い内容です".to_string() },
        FtsPendingOp::Add { doc_id: 1, text: "新しい内容です".to_string() },
    ]).unwrap();

    // Old content should not match
    let pl = idx.get_postings(&mut pager, "古い").unwrap();
    assert_eq!(pl.df(), 0);

    // New content should match
    let pl = idx.get_postings(&mut pager, "新し").unwrap();
    assert_eq!(pl.df(), 1);
}

#[test]
fn test_fts_stats() {
    let (mut pager, idx, _dir) = setup_fts(&[
        (1, "短いテキスト"),
        (2, "これは少し長いテキストです。いくつかの文があります。"),
    ]);

    let stats = idx.get_stats(&mut pager).unwrap();
    assert_eq!(stats.total_docs, 2);
    assert!(stats.total_tokens > 0);
    assert!(stats.avg_doc_len() > 0.0);
}

#[test]
fn test_fts_many_documents() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

    // Insert 30 documents (keeping posting lists within page size)
    let ops: Vec<FtsPendingOp> = (1..=30)
        .map(|i| FtsPendingOp::Add {
            doc_id: i,
            text: format!("文書{}の内容", i),
        })
        .collect();
    idx.apply_pending(&mut pager, &ops).unwrap();

    let stats = idx.get_stats(&mut pager).unwrap();
    assert_eq!(stats.total_docs, 30);

    // Search should find results
    let results = query_natural(&idx, &mut pager, "文書").unwrap();
    assert!(!results.is_empty());
}
