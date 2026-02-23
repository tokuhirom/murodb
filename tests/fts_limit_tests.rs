use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult};
use murodb::storage::pager::Pager;
use murodb::types::Value;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn setup() -> (Pager, SystemCatalog, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    (pager, catalog, dir)
}

fn exec(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) {
    execute(sql, pager, catalog).unwrap();
}

fn exec_err(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> String {
    execute(sql, pager, catalog).unwrap_err().to_string()
}

fn query_rows(
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
    sql: &str,
) -> Vec<murodb::sql::executor::Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    }
}

/// Empty string in FTS-indexed column should not crash.
#[test]
fn test_fts_empty_string() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, '')");
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER ngram",
    );

    // Query should return no matches (empty string has no tokens)
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('test' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows.len(), 0);
}

/// Large text (3500 bytes) in FTS-indexed column.
#[test]
fn test_fts_large_text() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );

    let text = "東京タワー".repeat(200); // 5 chars * 3 bytes * 200 = 3000 bytes
    let sql = format!("INSERT INTO t VALUES (1, '{}')", text);
    exec(&mut pager, &mut catalog, &sql);
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER ngram",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('東京' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

/// FTS query on table without FTS index → error.
#[test]
fn test_fts_no_index_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'hello world')",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('hello' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert!(
        err.to_lowercase().contains("fulltext")
            || err.to_lowercase().contains("fts")
            || err.to_lowercase().contains("index"),
        "Expected FTS index error, got: {}",
        err
    );
}

/// Punctuation-only document: tokenization should not crash.
#[test]
fn test_fts_punctuation_only() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '!!!...???---')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER ngram",
    );

    // Should not crash; may or may not match
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('!!!' IN NATURAL LANGUAGE MODE) > 0",
    );
    // We just verify it doesn't panic; result count is implementation-dependent
    let _ = rows;
}

/// FTS with increasing document counts: verify query works with various table sizes.
#[test]
fn test_fts_two_docs_query() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );

    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '東京タワーの夜景がきれい')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, '京都の金閣寺は素晴らしい')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

/// FTS with NULL body: should not crash.
#[test]
fn test_fts_null_body() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, '検索可能テキスト')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER ngram",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('検索' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(2)));
}
