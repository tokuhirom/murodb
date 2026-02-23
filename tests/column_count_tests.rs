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

/// Single column (PK only).
#[test]
fn test_single_column() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
}

/// 100 columns: create, insert, and retrieve all.
#[test]
fn test_100_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    let cols: Vec<String> = (0..100)
        .map(|i| {
            if i == 0 {
                "c0 BIGINT PRIMARY KEY".to_string()
            } else {
                format!("c{} INT", i)
            }
        })
        .collect();
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    let vals: Vec<String> = (0..100).map(|i| i.to_string()).collect();
    let insert_sql = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&mut pager, &mut catalog, &insert_sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("c0"), Some(&Value::Integer(0)));
    assert_eq!(rows[0].get("c99"), Some(&Value::Integer(99)));
}

/// NULL bitmap boundary: 8 nullable columns (exactly 1 byte bitmap).
#[test]
fn test_null_bitmap_8_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    let mut cols = vec!["id BIGINT PRIMARY KEY".to_string()];
    for i in 1..=8 {
        cols.push(format!("c{} INT", i));
    }
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    // Insert with alternating NULLs
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, NULL, 2, NULL, 4, NULL, 6, NULL, 8)",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("c1"), Some(&Value::Null));
    assert_eq!(rows[0].get("c2"), Some(&Value::Integer(2)));
    assert_eq!(rows[0].get("c3"), Some(&Value::Null));
    assert_eq!(rows[0].get("c4"), Some(&Value::Integer(4)));
    assert_eq!(rows[0].get("c5"), Some(&Value::Null));
    assert_eq!(rows[0].get("c6"), Some(&Value::Integer(6)));
    assert_eq!(rows[0].get("c7"), Some(&Value::Null));
    assert_eq!(rows[0].get("c8"), Some(&Value::Integer(8)));
}

/// NULL bitmap boundary: 9 nullable columns (crosses 1-byte boundary → 2 bytes).
#[test]
fn test_null_bitmap_9_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    let mut cols = vec!["id BIGINT PRIMARY KEY".to_string()];
    for i in 1..=9 {
        cols.push(format!("c{} INT", i));
    }
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    // Insert with last column NULL (tests the 9th bit)
    let mut vals: Vec<String> = vec!["1".to_string()];
    for i in 1..=8 {
        vals.push(i.to_string());
    }
    vals.push("NULL".to_string());
    let insert_sql = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&mut pager, &mut catalog, &insert_sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("c8"), Some(&Value::Integer(8)));
    assert_eq!(rows[0].get("c9"), Some(&Value::Null));
}

/// NULL bitmap boundary: 16 columns (exactly 2 bytes bitmap).
#[test]
fn test_null_bitmap_16_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    let mut cols = vec!["id BIGINT PRIMARY KEY".to_string()];
    for i in 1..=16 {
        cols.push(format!("c{} INT", i));
    }
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    // All NULLs
    let mut vals = vec!["1".to_string()];
    for _ in 1..=16 {
        vals.push("NULL".to_string());
    }
    let insert_sql = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&mut pager, &mut catalog, &insert_sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    for i in 1..=16 {
        assert_eq!(
            rows[0].get(&format!("c{}", i)),
            Some(&Value::Null),
            "c{} should be NULL",
            i
        );
    }
}

/// 17 columns: crosses 2-byte bitmap boundary.
#[test]
fn test_null_bitmap_17_columns() {
    let (mut pager, mut catalog, _dir) = setup();

    let mut cols = vec!["id BIGINT PRIMARY KEY".to_string()];
    for i in 1..=17 {
        cols.push(format!("c{} INT", i));
    }
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    // Only c17 is non-NULL
    let mut vals = vec!["1".to_string()];
    for _ in 1..=16 {
        vals.push("NULL".to_string());
    }
    vals.push("42".to_string());
    let insert_sql = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&mut pager, &mut catalog, &insert_sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("c16"), Some(&Value::Null));
    assert_eq!(rows[0].get("c17"), Some(&Value::Integer(42)));
}

/// Large column count: all columns with values, verify round-trip.
#[test]
fn test_50_columns_all_values() {
    let (mut pager, mut catalog, _dir) = setup();

    let cols: Vec<String> = (0..50)
        .map(|i| {
            if i == 0 {
                "c0 BIGINT PRIMARY KEY".to_string()
            } else {
                format!("c{} INT", i)
            }
        })
        .collect();
    let create_sql = format!("CREATE TABLE t ({})", cols.join(", "));
    exec(&mut pager, &mut catalog, &create_sql);

    let vals: Vec<String> = (0..50).map(|i| (i * 10).to_string()).collect();
    let insert_sql = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&mut pager, &mut catalog, &insert_sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    for i in 0..50 {
        assert_eq!(
            rows[0].get(&format!("c{}", i)),
            Some(&Value::Integer(i as i64 * 10)),
            "c{} mismatch",
            i
        );
    }
}
