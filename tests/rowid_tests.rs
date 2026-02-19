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
fn test_rowid_hidden_in_select_star() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (name VARCHAR, age BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t VALUES ('Alice', 30)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        // _rowid should NOT appear in SELECT *
        assert!(rows[0].get("_rowid").is_none());
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
        assert_eq!(rows[0].get("age"), Some(&Value::Integer(30)));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_rowid_explicit_select() {
    let (mut pager, mut catalog, _dir) = setup();

    execute("CREATE TABLE t (name VARCHAR)", &mut pager, &mut catalog).unwrap();

    execute("INSERT INTO t VALUES ('Alice')", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT _rowid, name FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_rowid"), Some(&Value::Integer(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_rowid_sequential() {
    let (mut pager, mut catalog, _dir) = setup();

    execute("CREATE TABLE t (name VARCHAR)", &mut pager, &mut catalog).unwrap();

    execute("INSERT INTO t VALUES ('a')", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES ('b')", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES ('c')", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT _rowid, name FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get("_rowid"), Some(&Value::Integer(1)));
        assert_eq!(rows[1].get("_rowid"), Some(&Value::Integer(2)));
        assert_eq!(rows[2].get("_rowid"), Some(&Value::Integer(3)));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_pk_table_unchanged() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t VALUES (10, 'Alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (20, 'Bob')", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(10)));
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
    } else {
        panic!("Expected rows");
    }
}

#[test]
fn test_rowid_insert_with_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (name VARCHAR, age BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t (name, age) VALUES ('Alice', 25)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT _rowid, name, age FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("_rowid"), Some(&Value::Integer(1)));
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
        assert_eq!(rows[0].get("age"), Some(&Value::Integer(25)));
    } else {
        panic!("Expected rows");
    }
}
