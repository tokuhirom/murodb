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
        "CREATE TABLE t1 (id BIGINT PRIMARY KEY, name VARCHAR)",
        pager,
        catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY, name VARCHAR)",
        pager,
        catalog,
    )
    .unwrap();

    execute("INSERT INTO t1 VALUES (1, 'alice')", pager, catalog).unwrap();
    execute("INSERT INTO t1 VALUES (2, 'bob')", pager, catalog).unwrap();
    execute("INSERT INTO t1 VALUES (3, 'charlie')", pager, catalog).unwrap();

    execute("INSERT INTO t2 VALUES (2, 'bob')", pager, catalog).unwrap();
    execute("INSERT INTO t2 VALUES (3, 'charlie')", pager, catalog).unwrap();
    execute("INSERT INTO t2 VALUES (4, 'dave')", pager, catalog).unwrap();
}

fn get_rows(result: ExecResult) -> Vec<Vec<Value>> {
    match result {
        ExecResult::Rows(rows) => rows
            .into_iter()
            .map(|r| r.values.into_iter().map(|(_, v)| v).collect())
            .collect(),
        _ => panic!("Expected Rows"),
    }
}

#[test]
fn test_union_removes_duplicates() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION SELECT id, name FROM t2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    // t1: (1,alice), (2,bob), (3,charlie)
    // t2: (2,bob), (3,charlie), (4,dave)
    // UNION should deduplicate: 4 unique rows
    assert_eq!(rows.len(), 4);
}

#[test]
fn test_union_all_keeps_duplicates() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    // 3 from t1 + 3 from t2 = 6
    assert_eq!(rows.len(), 6);
}

#[test]
fn test_union_column_count_mismatch() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id FROM t1 UNION SELECT id, name FROM t2",
        &mut pager,
        &mut catalog,
    );

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("same number of columns"), "got: {}", err);
}

#[test]
fn test_union_with_order_by() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id DESC",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    assert_eq!(rows.len(), 4);
    // Should be sorted by id DESC: 4, 3, 2, 1
    assert_eq!(rows[0][0], Value::Integer(4));
    assert_eq!(rows[1][0], Value::Integer(3));
    assert_eq!(rows[2][0], Value::Integer(2));
    assert_eq!(rows[3][0], Value::Integer(1));
}

#[test]
fn test_union_with_limit() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id LIMIT 2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
}

#[test]
fn test_union_with_offset() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id LIMIT 2 OFFSET 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
}

#[test]
fn test_union_chain_three_selects() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    execute(
        "CREATE TABLE t3 (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t3 VALUES (5, 'eve')", &mut pager, &mut catalog).unwrap();

    let result = execute(
        "SELECT id, name FROM t1 UNION SELECT id, name FROM t2 UNION SELECT id, name FROM t3 ORDER BY id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    // 1,2,3 from t1 + 4 from t2 + 5 from t3 = 5 unique
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[4][0], Value::Integer(5));
}

#[test]
fn test_union_with_where() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 WHERE id <= 2 UNION SELECT id, name FROM t2 WHERE id >= 4 ORDER BY id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    // t1 WHERE id<=2: (1,alice), (2,bob)
    // t2 WHERE id>=4: (4,dave)
    // No duplicates, 3 rows
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[1][0], Value::Integer(2));
    assert_eq!(rows[2][0], Value::Integer(4));
}

#[test]
fn test_union_all_with_order_by() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 ORDER BY id",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(result);
    assert_eq!(rows.len(), 6);
    // Sorted by id: 1, 2, 2, 3, 3, 4
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[5][0], Value::Integer(4));
}

#[test]
fn test_union_column_names_from_first_select() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_tables(&mut pager, &mut catalog);

    let result = execute(
        "SELECT id AS user_id, name AS user_name FROM t1 UNION SELECT id, name FROM t2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    if let ExecResult::Rows(rows) = result {
        // Column names should come from the first SELECT
        assert_eq!(rows[0].values[0].0, "user_id");
        assert_eq!(rows[0].values[1].0, "user_name");
    } else {
        panic!("Expected Rows");
    }
}
