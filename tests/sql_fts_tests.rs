use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult, Row};
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

fn query_rows(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> Vec<Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_sql_fulltext_create_and_natural_query() {
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
        "INSERT INTO t VALUES (2, '京都の金閣寺')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) AS score FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0 ORDER BY score DESC",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert!(matches!(rows[0].get("score"), Some(Value::Integer(n)) if *n > 0));
}

#[test]
fn test_sql_fulltext_boolean_and_snippet() {
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
        "INSERT INTO t VALUES (2, '東京は混雑している')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, fts_snippet(body, '\"東京タワー\" +夜景', '<mark>', '</mark>', 30) AS s FROM t WHERE MATCH(body) AGAINST('\"東京タワー\" +夜景 -混雑' IN BOOLEAN MODE) > 0",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert!(matches!(rows[0].get("s"), Some(Value::Varchar(s)) if s.contains("<mark>")));
}

#[test]
fn test_sql_fulltext_tracks_update_and_delete() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '東京タワーの夜景')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );

    let rows_before = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows_before.len(), 1);

    exec(
        &mut pager,
        &mut catalog,
        "UPDATE t SET body = '京都の寺院' WHERE id = 1",
    );

    let rows_old = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('東京タワー' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert!(rows_old.is_empty());

    let rows_new = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('京都寺院' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert_eq!(rows_new.len(), 1);

    exec(&mut pager, &mut catalog, "DELETE FROM t WHERE id = 1");

    let rows_after_delete = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE MATCH(body) AGAINST('京都寺院' IN NATURAL LANGUAGE MODE) > 0",
    );
    assert!(rows_after_delete.is_empty());
}

#[test]
fn test_sql_fulltext_create_failure_does_not_leave_broken_metadata() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body TEXT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (-1, 'broken doc id')",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );
    assert!(err.contains("non-negative BIGINT"));

    exec(&mut pager, &mut catalog, "DELETE FROM t WHERE id = -1");
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '東京タワー')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body ON t(body) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );
}

#[test]
fn test_sql_fulltext_multiple_match_terms_are_evaluated() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, body1 TEXT, body2 TEXT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '東京タワー', '京都の寺院')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, '東京駅', '大阪城')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body1 ON t(body1) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE FULLTEXT INDEX ft_body2 ON t(body2) WITH PARSER ngram OPTIONS (n=2, normalize='nfkc')",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, MATCH(body1) AGAINST('東京' IN NATURAL LANGUAGE MODE) AS s1, MATCH(body2) AGAINST('京都' IN NATURAL LANGUAGE MODE) AS s2 FROM t WHERE MATCH(body1) AGAINST('東京' IN NATURAL LANGUAGE MODE) > 0 AND MATCH(body2) AGAINST('京都' IN NATURAL LANGUAGE MODE) > 0",
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert!(matches!(rows[0].get("s1"), Some(Value::Integer(n)) if *n > 0));
    assert!(matches!(rows[0].get("s2"), Some(Value::Integer(n)) if *n > 0));
}
