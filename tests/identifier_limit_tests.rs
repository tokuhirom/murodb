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

/// Table name with 255 characters.
#[test]
fn test_table_name_255_chars() {
    let (mut pager, mut catalog, _dir) = setup();
    let name = "t".repeat(255);
    let sql = format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, val INT)", name);
    exec(&mut pager, &mut catalog, &sql);

    let insert_sql = format!("INSERT INTO {} VALUES (1, 42)", name);
    exec(&mut pager, &mut catalog, &insert_sql);

    let select_sql = format!("SELECT val FROM {} WHERE id = 1", name);
    let rows = query_rows(&mut pager, &mut catalog, &select_sql);
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(42)));
}

/// Column name with 255 characters.
#[test]
fn test_column_name_255_chars() {
    let (mut pager, mut catalog, _dir) = setup();
    let col_name = "c".repeat(255);
    let sql = format!("CREATE TABLE t (id BIGINT PRIMARY KEY, {} INT)", col_name);
    exec(&mut pager, &mut catalog, &sql);

    let insert_sql = format!("INSERT INTO t (id, {}) VALUES (1, 99)", col_name);
    exec(&mut pager, &mut catalog, &insert_sql);

    let select_sql = format!("SELECT {} FROM t WHERE id = 1", col_name);
    let rows = query_rows(&mut pager, &mut catalog, &select_sql);
    assert_eq!(rows[0].get(&col_name), Some(&Value::Integer(99)));
}

/// Long SQL with many VALUES (batch insert).
#[test]
fn test_long_sql_many_values() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let values: Vec<String> = (1..=500).map(|i| format!("({}, {})", i, i * 2)).collect();
    let sql = format!("INSERT INTO t VALUES {}", values.join(", "));
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(500)));

    // Verify first and last
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(2)));
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 500");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(1000)));
}

/// Empty identifier should fail at parse time.
#[test]
fn test_empty_table_name() {
    let (mut pager, mut catalog, _dir) = setup();
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "CREATE TABLE  (id BIGINT PRIMARY KEY)",
    );
    assert!(
        err.contains("parse")
            || err.contains("Parse")
            || err.contains("syntax")
            || err.contains("expected"),
        "Expected parse error, got: {}",
        err
    );
}

/// Index name with 255 characters.
#[test]
fn test_index_name_255_chars() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    let idx_name = "i".repeat(255);
    let sql = format!("CREATE INDEX {} ON t(val)", idx_name);
    exec(&mut pager, &mut catalog, &sql);

    // Verify table still works
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 42)");
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE val = 42");
    assert_eq!(rows.len(), 1);
}

/// SQL reserved words as column names (should be rejected or handled).
#[test]
fn test_reserved_word_as_column_name() {
    let (mut pager, mut catalog, _dir) = setup();
    // "select" is a reserved word — this might fail or succeed depending on parser
    let result = execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, `select` INT)",
        &mut pager,
        &mut catalog,
    );
    match result {
        Ok(_) => {
            // If backtick-quoting works, verify it
            exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 42)");
        }
        Err(_) => {
            // It's acceptable for the parser to reject this
        }
    }
}

/// Multiple tables can be created and queried independently.
#[test]
fn test_multiple_tables_independent() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE alpha (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE beta (id BIGINT PRIMARY KEY, val INT)",
    );

    exec(&mut pager, &mut catalog, "INSERT INTO alpha VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO beta VALUES (1, 20)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT val FROM alpha WHERE id = 1",
    );
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(10)));

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT val FROM beta WHERE id = 1",
    );
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(20)));
}
