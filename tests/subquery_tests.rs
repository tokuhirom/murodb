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

fn setup_tables(pager: &mut Pager, catalog: &mut SystemCatalog) {
    execute(
        "CREATE TABLE t1 (id BIGINT PRIMARY KEY, val BIGINT)",
        pager,
        catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val BIGINT)",
        pager,
        catalog,
    )
    .unwrap();

    execute("INSERT INTO t1 VALUES (1, 10)", pager, catalog).unwrap();
    execute("INSERT INTO t1 VALUES (2, 20)", pager, catalog).unwrap();
    execute("INSERT INTO t1 VALUES (3, 30)", pager, catalog).unwrap();

    execute("INSERT INTO t2 VALUES (1, 10)", pager, catalog).unwrap();
    execute("INSERT INTO t2 VALUES (2, 20)", pager, catalog).unwrap();
    execute("INSERT INTO t2 VALUES (4, 40)", pager, catalog).unwrap();
}

fn get_rows(result: ExecResult) -> Vec<Vec<Value>> {
    match result {
        ExecResult::Rows(rows) => rows
            .into_iter()
            .map(|r| r.values.into_iter().map(|(_, v)| v).collect())
            .collect(),
        _ => panic!("Expected rows"),
    }
}

fn get_ids(result: ExecResult) -> Vec<i64> {
    let rows = get_rows(result);
    rows.into_iter()
        .map(|r| match &r[0] {
            Value::Integer(n) => *n,
            _ => panic!("Expected integer"),
        })
        .collect()
}

// --- IN (SELECT ...) ---

#[test]
fn test_in_subquery_basic() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // Find rows in t1 whose id is also in t2
    let result = execute(
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn test_in_subquery_empty_result() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // Subquery returns no rows
    let result = execute(
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE val = 999)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert!(ids.is_empty());
}

#[test]
fn test_in_subquery_with_duplicates() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // Subquery returns values that might match multiple rows
    let result = execute(
        "SELECT id FROM t1 WHERE val IN (SELECT val FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 2]); // val 10 and 20 match
}

// --- NOT IN (SELECT ...) ---

#[test]
fn test_not_in_subquery_basic() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert_eq!(ids, vec![3]);
}

#[test]
fn test_not_in_subquery_with_nulls() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // Insert a NULL val into t2
    execute("INSERT INTO t2 VALUES (5, NULL)", &mut pager, &mut catalog).unwrap();

    // SQL standard: NOT IN with NULL in the list returns UNKNOWN for non-matching values.
    // t1 vals: 10, 20, 30. t2 vals: 10, 20, 40, NULL.
    // val=10 → found → NOT IN → FALSE
    // val=20 → found → NOT IN → FALSE
    // val=30 → not found, but NULL in list → UNKNOWN → filtered out by WHERE
    let result = execute(
        "SELECT id FROM t1 WHERE val NOT IN (SELECT val FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    // No rows should be returned: matching values are excluded, and non-matching
    // values get UNKNOWN due to NULL in the subquery result set.
    assert!(ids.is_empty());
}

// --- EXISTS (SELECT ...) ---

#[test]
fn test_exists_true() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // EXISTS with matching rows (uncorrelated — always true if t2 has any rows)
    let result = execute(
        "SELECT id FROM t1 WHERE EXISTS (SELECT id FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]); // all rows returned since EXISTS is true
}

#[test]
fn test_exists_false() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE EXISTS (SELECT id FROM t2 WHERE val = 999)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert!(ids.is_empty());
}

// --- NOT EXISTS (SELECT ...) ---

#[test]
fn test_not_exists_true() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE NOT EXISTS (SELECT id FROM t2 WHERE val = 999)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn test_not_exists_false() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE NOT EXISTS (SELECT id FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert!(ids.is_empty());
}

// --- Scalar subquery in SELECT list ---

#[test]
fn test_scalar_subquery_in_select() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, (SELECT MAX(val) FROM t2) AS max_t2 FROM t1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 3);
    // All rows should have max_t2 = 40
    for row in &rows {
        assert_eq!(row[1], Value::Integer(40));
    }
}

// --- Scalar subquery in WHERE ---

#[test]
fn test_scalar_subquery_in_where() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE val = (SELECT MAX(val) FROM t2 WHERE val <= 20)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert_eq!(ids, vec![2]); // val 20 matches MAX(val) WHERE val <= 20 = 20
}

// --- Scalar subquery returning 0 rows → NULL ---

#[test]
fn test_scalar_subquery_zero_rows() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT (SELECT val FROM t2 WHERE val = 999) AS result FROM t1 WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Null);
}

// --- Scalar subquery returning >1 rows → error ---

#[test]
fn test_scalar_subquery_too_many_rows() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT (SELECT val FROM t2) AS result FROM t1 WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("more than one row"));
}

// --- Nested subquery ---

#[test]
fn test_nested_subquery() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // t1 rows where id is in t2 AND t2 has any rows (EXISTS is always true here)
    let result = execute(
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE EXISTS (SELECT id FROM t1))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

// --- Subquery with aggregation ---

#[test]
fn test_subquery_with_aggregation() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE val > (SELECT MIN(val) FROM t2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![2, 3]); // val > 10
}

// --- Subquery with AND/OR in outer WHERE ---

#[test]
fn test_subquery_with_and_or() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) AND val > 10",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ids = get_ids(result);
    assert_eq!(ids, vec![2]); // id in t2 AND val > 10
}

#[test]
fn test_subquery_with_or() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id = 1) OR val = 30",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

// --- Subquery referencing a different table ---

#[test]
fn test_subquery_different_table() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    // Create a third table
    execute(
        "CREATE TABLE t3 (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t3 VALUES (1, 'one')", &mut pager, &mut catalog).unwrap();
    execute(
        "INSERT INTO t3 VALUES (3, 'three')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT id, val FROM t1 WHERE id IN (SELECT id FROM t3)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let mut ids = get_ids(result);
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}
