#![cfg(feature = "test-utils")]
use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult};
use murodb::sql::lexer::{tokenize, Token};
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

#[test]
fn test_hex_literal_tokenize() {
    let tokens = tokenize("X'DEADBEEF'").unwrap();
    assert_eq!(
        tokens,
        vec![Token::HexLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF])]
    );
}

#[test]
fn test_hex_literal_lowercase() {
    let tokens = tokenize("x'deadbeef'").unwrap();
    assert_eq!(
        tokens,
        vec![Token::HexLiteral(vec![0xDE, 0xAD, 0xBE, 0xEF])]
    );
}

#[test]
fn test_hex_literal_empty() {
    let tokens = tokenize("X''").unwrap();
    assert_eq!(tokens, vec![Token::HexLiteral(vec![])]);
}

#[test]
fn test_hex_literal_odd_digits_error() {
    let result = tokenize("X'DEA'");
    assert!(result.is_err());
}

#[test]
fn test_hex_literal_invalid_chars_error() {
    let result = tokenize("X'GGGG'");
    assert!(result.is_err());
}

#[test]
fn test_hex_literal_insert_and_select() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE bin_data (id BIGINT PRIMARY KEY, val VARBINARY(256))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO bin_data VALUES (1, X'DEADBEEF')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT val FROM bin_data WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("val"),
            Some(&Value::Varbinary(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        );
    } else {
        panic!("Expected Rows result");
    }
}

#[test]
fn test_hex_literal_empty_insert() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE bin_data (id BIGINT PRIMARY KEY, val VARBINARY(256))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO bin_data VALUES (1, X'')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT val FROM bin_data WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("val"), Some(&Value::Varbinary(vec![])));
    } else {
        panic!("Expected Rows result");
    }
}

#[test]
fn test_hex_literal_length_exceeded() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE bin_data (id BIGINT PRIMARY KEY, val VARBINARY(4))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "INSERT INTO bin_data VALUES (1, X'DEADBEEFCAFE')",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
}

#[test]
fn test_hex_literal_where_clause() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE bin_data (id BIGINT PRIMARY KEY, val VARBINARY(256))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO bin_data VALUES (1, X'DEADBEEF')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO bin_data VALUES (2, X'CAFEBABE')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT id FROM bin_data WHERE val = X'DEADBEEF'",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    } else {
        panic!("Expected Rows result");
    }
}
