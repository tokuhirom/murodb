#![cfg(feature = "test-utils")]
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

/// VARCHAR(100) with exactly 100 bytes succeeds.
#[test]
fn test_varchar_100_exact_fit() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR(100))",
    );
    let data = "a".repeat(100);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(data)));
}

/// VARCHAR(100) with 101 bytes fails.
#[test]
fn test_varchar_100_one_over() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR(100))",
    );
    let data = "a".repeat(101);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    let err = exec_err(&mut pager, &mut catalog, &sql);
    assert!(
        err.contains("exceeds") || err.contains("VARCHAR"),
        "Expected VARCHAR length error, got: {}",
        err
    );
}

/// Empty string inserts and reads back correctly.
#[test]
fn test_empty_string() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, '')");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(String::new())));
}

/// VARBINARY column can store NULL values.
#[test]
fn test_varbinary_null() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARBINARY(256))",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Null));
}

/// Multibyte UTF-8 (Japanese 3-byte chars) — VARCHAR(n) checks character count (MySQL-compatible).
#[test]
fn test_varchar_multibyte_char_count() {
    let (mut pager, mut catalog, _dir) = setup();
    // VARCHAR(3) should fit 3 Japanese chars (3 characters, even though 9 bytes)
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR(3))",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'あいう')",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar("あいう".into())));
}

/// VARCHAR(2) cannot fit 3 Japanese chars (3 characters).
#[test]
fn test_varchar_multibyte_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR(2))",
    );
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'あいう')",
    );
    assert!(
        err.contains("exceeds") || err.contains("VARCHAR"),
        "Expected VARCHAR length error, got: {}",
        err
    );
}

/// Emoji (4-byte UTF-8) counted as characters, not bytes.
#[test]
fn test_varchar_emoji_char_count() {
    let (mut pager, mut catalog, _dir) = setup();
    // VARCHAR(2) should fit 2 emoji (2 characters, 8 bytes)
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR(2))",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, '😀🎉')");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar("😀🎉".into())));
}

/// TEXT type with large value (within page limit).
#[test]
fn test_text_large_value() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT)",
    );
    let data = "t".repeat(3000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(data)));
}

/// TEXT type with page-exceeding value uses overflow pages.
#[test]
fn test_text_large_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val TEXT)",
    );
    let data = "t".repeat(5000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("val"),
        Some(&murodb::types::Value::Varchar(data))
    );
}

/// VARCHAR with unlimited length (no size specified) and large value.
#[test]
fn test_varchar_unlimited_large() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    let data = "u".repeat(3500);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(data)));
}
