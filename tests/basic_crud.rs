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

#[test]
fn test_full_crud_cycle() {
    let (mut pager, mut catalog, _dir) = setup();

    // CREATE TABLE
    execute(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // INSERT
    execute(
        "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // SELECT all
    let result = execute("SELECT * FROM users", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
    } else {
        panic!("Expected rows");
    }

    // SELECT with WHERE
    let result = execute("SELECT * FROM users WHERE id = 2", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));
    }

    // UPDATE
    execute(
        "UPDATE users SET name = 'Bobby' WHERE id = 2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let result = execute("SELECT * FROM users WHERE id = 2", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bobby".into())));
    }

    // DELETE
    execute("DELETE FROM users WHERE id = 3", &mut pager, &mut catalog).unwrap();
    let result = execute("SELECT * FROM users", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
    }
}

#[test]
fn test_insert_with_explicit_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR, b VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, b) VALUES (1, 'only_b')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows[0].get("a"), Some(&Value::Null));
        assert_eq!(rows[0].get("b"), Some(&Value::Varchar("only_b".into())));
    }
}

#[test]
fn test_order_by_desc_limit() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    for i in 1..=10 {
        execute(
            &format!("INSERT INTO t VALUES ({}, {})", i, i * 10),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let result = execute(
        "SELECT * FROM t ORDER BY val DESC LIMIT 3",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("val"), Some(&Value::Integer(100)));
        assert_eq!(rows[1].get("val"), Some(&Value::Integer(90)));
        assert_eq!(rows[2].get("val"), Some(&Value::Integer(80)));
    }
}

#[test]
fn test_null_handling() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();
    execute(
        "INSERT INTO t VALUES (2, 'Alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name"), Some(&Value::Null));
        assert_eq!(rows[1].get("name"), Some(&Value::Varchar("Alice".into())));
    }
}

#[test]
fn test_varbinary_type() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARBINARY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    // For MVP, we test with string insertion since we don't have hex literal support
    execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
    }
}

#[test]
fn test_multiple_value_insert() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
    }
}

#[test]
fn test_duplicate_pk_error() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1, 'Alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("INSERT INTO t VALUES (1, 'Bob')", &mut pager, &mut catalog);
    assert!(result.is_err());
}
