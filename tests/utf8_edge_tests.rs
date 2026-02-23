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

/// Japanese text round-trip.
#[test]
fn test_japanese_roundtrip() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '東京タワーの夜景がきれい')",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(
        rows[0].get("val"),
        Some(&Value::Varchar("東京タワーの夜景がきれい".into()))
    );
}

/// Emoji round-trip (4-byte UTF-8).
#[test]
fn test_emoji_roundtrip() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, '🎉🚀💯')",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar("🎉🚀💯".into())));
}

/// Empty string round-trip.
#[test]
fn test_empty_string_roundtrip() {
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

/// Long UTF-8 string (within page limit).
#[test]
fn test_long_utf8_string() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    // Each Japanese char is 3 bytes; 1000 chars = 3000 bytes (fits in page)
    let data: String = "あ".repeat(1000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(data)));
}

/// Zero-width characters round-trip.
#[test]
fn test_zero_width_chars() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    // Zero-width space (U+200B) and zero-width joiner (U+200D)
    let text = "a\u{200B}b\u{200D}c";
    let sql = format!("INSERT INTO t VALUES (1, '{}')", text);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(text.into())));
}

/// Combining characters round-trip (e.g., é as e + combining acute).
#[test]
fn test_combining_chars() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    // e followed by combining acute accent (U+0301)
    let text = "e\u{0301}";
    let sql = format!("INSERT INTO t VALUES (1, '{}')", text);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar(text.into())));
}

/// SQL single-quote escape: two single-quotes represent one.
#[test]
fn test_single_quote_escape() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'it''s a test')",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(
        rows[0].get("val"),
        Some(&Value::Varchar("it's a test".into()))
    );
}

/// Mixed ASCII and multibyte text.
#[test]
fn test_mixed_ascii_multibyte() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'Hello世界🌍test')",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(
        rows[0].get("val"),
        Some(&Value::Varchar("Hello世界🌍test".into()))
    );
}
