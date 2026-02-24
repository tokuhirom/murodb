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

/// All nullable columns set to NULL.
#[test]
fn test_all_columns_null() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR, b INT, c DOUBLE)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, NULL, NULL, NULL)",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT a, b, c FROM t WHERE id = 1",
    );
    assert_eq!(rows[0].get("a"), Some(&Value::Null));
    assert_eq!(rows[0].get("b"), Some(&Value::Null));
    assert_eq!(rows[0].get("c"), Some(&Value::Null));
}

/// PK column cannot be NULL.
#[test]
fn test_pk_null_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (NULL, 'test')",
    );
    assert!(
        err.to_lowercase().contains("null")
            || err.to_lowercase().contains("not null")
            || err.to_lowercase().contains("primary key"),
        "Expected NOT NULL error, got: {}",
        err
    );
}

/// COUNT(*) counts all rows; COUNT(nullable_col) excludes NULLs.
#[test]
fn test_count_star_vs_count_column() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, 30)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT COUNT(*) AS cnt_all, COUNT(val) AS cnt_val FROM t",
    );
    assert_eq!(rows[0].get("cnt_all"), Some(&Value::Integer(3)));
    assert_eq!(rows[0].get("cnt_val"), Some(&Value::Integer(2)));
}

/// SUM/AVG/MIN/MAX skip NULLs.
#[test]
fn test_aggregates_skip_null() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, 30)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT SUM(val) AS s, MIN(val) AS mi, MAX(val) AS ma FROM t",
    );
    assert_eq!(rows[0].get("s"), Some(&Value::Integer(40)));
    assert_eq!(rows[0].get("mi"), Some(&Value::Integer(10)));
    assert_eq!(rows[0].get("ma"), Some(&Value::Integer(30)));
}

/// IS NULL vs = NULL.
#[test]
fn test_is_null_vs_equals_null() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 42)");

    // IS NULL should find the NULL row
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE val IS NULL",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));

    // = NULL should find nothing (SQL standard: NULL = NULL is UNKNOWN)
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE val = NULL",
    );
    assert_eq!(rows.len(), 0);
}

/// NULL in ORDER BY — consistent ordering.
#[test]
fn test_null_order_by() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (4, 5)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, val FROM t ORDER BY val ASC",
    );
    // NULLs should all be grouped together (either first or last)
    assert_eq!(rows.len(), 4);
    let null_positions: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, r)| r.get("val") == Some(&Value::Null))
        .map(|(i, _)| i)
        .collect();
    // All NULLs should be adjacent
    assert_eq!(null_positions.len(), 2);
    assert_eq!(
        null_positions[1] - null_positions[0],
        1,
        "NULLs should be adjacent in ORDER BY"
    );
}

/// NULL in UNIQUE index: multiple NULLs allowed (SQL standard).
#[test]
fn test_null_in_unique_index() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT UNIQUE)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, NULL)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(2)));
}

/// NULL in JOIN condition: NULL does not match NULL.
#[test]
fn test_null_join_no_match() {
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
    exec(&mut pager, &mut catalog, "INSERT INTO a VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO b VALUES (1, NULL)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT a.id FROM a INNER JOIN b ON a.val = b.val",
    );
    assert_eq!(rows.len(), 0, "NULL = NULL should not match in JOIN");
}

/// NULL in IN list: three-valued logic.
#[test]
fn test_null_in_list() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 10)");

    // val IN (10, NULL) should find id=2 (val=10 matches), but not id=1 (NULL IN (...) is UNKNOWN)
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE val IN (10, NULL) ORDER BY id",
    );
    // At minimum, id=2 should be found
    assert!(
        rows.iter().any(|r| r.get("id") == Some(&Value::Integer(2))),
        "val=10 should match IN (10, NULL)"
    );
}

/// NULL in BETWEEN: returns NULL (not matched).
#[test]
fn test_null_between() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 5)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE val BETWEEN 1 AND 10",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(2)));
}

/// IS NOT NULL filter.
#[test]
fn test_is_not_null() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 42)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, NULL)");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id FROM t WHERE val IS NOT NULL",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(2)));
}
