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

fn exec_affected(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> u64 {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::RowsAffected(n) => n,
        other => panic!("Expected RowsAffected, got {:?}", other),
    }
}

/// SELECT on empty table returns empty result set.
#[test]
fn test_select_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t");
    assert_eq!(rows.len(), 0);
}

/// COUNT(*) on empty table returns 0.
#[test]
fn test_count_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(0)));
}

/// SUM/AVG/MIN/MAX on empty table return NULL.
#[test]
fn test_aggregates_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT SUM(val) AS s, MIN(val) AS mi, MAX(val) AS ma FROM t",
    );
    assert_eq!(rows[0].get("s"), Some(&Value::Null));
    assert_eq!(rows[0].get("mi"), Some(&Value::Null));
    assert_eq!(rows[0].get("ma"), Some(&Value::Null));
}

/// DELETE on empty table affects 0 rows.
#[test]
fn test_delete_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let affected = exec_affected(&mut pager, &mut catalog, "DELETE FROM t");
    assert_eq!(affected, 0);
}

/// UPDATE on empty table affects 0 rows.
#[test]
fn test_update_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let affected = exec_affected(&mut pager, &mut catalog, "UPDATE t SET val = 99");
    assert_eq!(affected, 0);
}

/// JOIN with empty table produces empty result.
#[test]
fn test_join_with_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE a (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE b (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO a VALUES (1, 10)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT a.id FROM a INNER JOIN b ON a.id = b.id",
    );
    assert_eq!(rows.len(), 0);
}

/// DROP TABLE and re-create with different schema.
#[test]
fn test_drop_and_recreate_different_schema() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'hello')",
    );
    exec(&mut pager, &mut catalog, "DROP TABLE t");

    // Re-create with different schema
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, value INT, flag TINYINT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 42, 1)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT value, flag FROM t WHERE id = 1",
    );
    assert_eq!(rows[0].get("value"), Some(&Value::Integer(42)));
    assert_eq!(rows[0].get("flag"), Some(&Value::Integer(1)));
}

/// Empty transaction (BEGIN; COMMIT with no operations) via Database API.
#[test]
fn test_empty_transaction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("COMMIT").unwrap();

    // Table should still be accessible
    let rows = db.query("SELECT COUNT(*) AS cnt FROM t").unwrap();
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(0)));
}

/// DELETE all rows → table behaves like empty.
#[test]
fn test_delete_all_then_empty_behavior() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 20)");
    exec(&mut pager, &mut catalog, "DELETE FROM t");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(0)));

    // Can insert again
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, 30)");
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 3");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(30)));
}
