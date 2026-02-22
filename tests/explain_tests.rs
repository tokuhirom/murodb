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

fn query_rows(
    sql: &str,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Vec<Vec<(String, Value)>> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows.into_iter().map(|r| r.values).collect(),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

#[test]
fn test_explain_full_scan() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows("EXPLAIN SELECT * FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    // Check column names
    assert_eq!(row[0].0, "id");
    assert_eq!(row[1].0, "select_type");
    assert_eq!(row[2].0, "table");
    assert_eq!(row[3].0, "type");
    assert_eq!(row[4].0, "key");
    assert_eq!(row[5].0, "rows");
    assert_eq!(row[6].0, "Extra");

    // Check values for full scan
    assert_eq!(row[1].1, Value::Varchar("SIMPLE".to_string()));
    assert_eq!(row[2].1, Value::Varchar("t".to_string()));
    assert_eq!(row[3].1, Value::Varchar("ALL".to_string()));
    assert_eq!(row[4].1, Value::Null); // no key used
}

#[test]
fn test_explain_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("const".to_string()));
    assert_eq!(row[4].1, Value::Varchar("PRIMARY".to_string()));
}

#[test]
fn test_explain_index_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "CREATE UNIQUE INDEX idx_email ON t(email)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE email = 'test@example.com'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("ref".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_email".to_string()));
}

#[test]
fn test_explain_full_scan_with_where() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE name = 'Alice'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("ALL".to_string())); // full scan, no index on name
    assert_eq!(row[6].1, Value::Varchar("Using where".to_string()));
}

#[test]
fn test_explain_update_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = query_rows(
        "EXPLAIN UPDATE t SET name = 'x' WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[1].1, Value::Varchar("UPDATE".to_string()));
    assert_eq!(row[3].1, Value::Varchar("const".to_string()));
    assert_eq!(row[4].1, Value::Varchar("PRIMARY".to_string()));
}

#[test]
fn test_explain_delete_index_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_name ON t(name)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN DELETE FROM t WHERE name = 'Bob'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[1].1, Value::Varchar("DELETE".to_string()));
    assert_eq!(row[3].1, Value::Varchar("ref".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_name".to_string()));
}

#[test]
fn test_explain_composite_index_range_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_ab ON t(a, b)", &mut pager, &mut catalog).unwrap();

    let rows = query_rows(
        "EXPLAIN SELECT * FROM t WHERE a = 10 AND b >= 3 AND b <= 7",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row[3].1, Value::Varchar("range".to_string()));
    assert_eq!(row[4].1, Value::Varchar("idx_ab".to_string()));
    assert!(matches!(row[5].1, Value::Integer(_)));
}
