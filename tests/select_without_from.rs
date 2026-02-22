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
fn test_select_literals_without_from() {
    let (mut pager, mut catalog, _dir) = setup();
    let rows = match execute("SELECT 3.14, 1563", &mut pager, &mut catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values.len(), 2);
    assert_eq!(rows[0].values[0].1, Value::Float(3.14));
    assert_eq!(rows[0].values[1].1, Value::Integer(1563));
}

#[test]
fn test_select_where_filters_without_from() {
    let (mut pager, mut catalog, _dir) = setup();
    let rows = match execute("SELECT 1 WHERE 0", &mut pager, &mut catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    };
    assert!(rows.is_empty());

    let rows = match execute("SELECT 1 WHERE 1", &mut pager, &mut catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_select_aggregates_without_from() {
    let (mut pager, mut catalog, _dir) = setup();
    let rows = match execute(
        "SELECT COUNT(*) AS cnt, SUM(2) AS total",
        &mut pager,
        &mut catalog,
    )
    .unwrap()
    {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("total"), Some(&Value::Integer(2)));
}
