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

/// VARCHAR 4000 bytes fits within a single page.
#[test]
fn test_varchar_4000_bytes_fits_in_page() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let data = "a".repeat(4000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar(data)));
}

/// VARCHAR 5000 bytes exceeds page capacity → PageOverflow.
#[test]
fn test_varchar_5000_bytes_page_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let data = "a".repeat(5000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    let err = exec_err(&mut pager, &mut catalog, &sql);
    assert!(
        err.contains("overflow") || err.contains("Overflow") || err.contains("capacity"),
        "Expected page overflow error, got: {}",
        err
    );
}

/// Two medium-sized rows that together exceed page capacity trigger B-tree split.
#[test]
fn test_two_rows_cause_btree_split() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let data = "b".repeat(2500);
    let sql1 = format!("INSERT INTO t VALUES (1, '{}')", data);
    let sql2 = format!("INSERT INTO t VALUES (2, '{}')", data);
    exec(&mut pager, &mut catalog, &sql1);
    exec(&mut pager, &mut catalog, &sql2);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
}

/// Multiple medium-sized rows fill pages with splits; all rows remain accessible.
#[test]
fn test_multiple_medium_rows_all_accessible() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let data = "c".repeat(500);
    for i in 1..=20 {
        let sql = format!("INSERT INTO t VALUES ({}, '{}')", i, data);
        exec(&mut pager, &mut catalog, &sql);
    }

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(20)));
}

/// Many small rows that span multiple pages via B-tree splits.
#[test]
fn test_many_small_rows_spanning_pages() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );

    for i in 1..=200 {
        let sql = format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i);
        exec(&mut pager, &mut catalog, &sql);
    }

    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(200)));

    // Verify first and last row
    let rows = query_rows(&mut pager, &mut catalog, "SELECT name FROM t WHERE id = 1");
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("row_1".into())));
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT name FROM t WHERE id = 200",
    );
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("row_200".into())));
}

/// Rows near the page boundary: insert rows of increasing size until overflow.
#[test]
fn test_gradual_size_increase_until_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    // Start at 3900 bytes and increase. At some point the row won't fit in a single page.
    // The B-tree should handle splits for rows that fit and error for rows that are too large.
    let mut max_success = 0;
    for size in (3900..=4200).step_by(50) {
        let data = "x".repeat(size);
        let sql = format!("INSERT INTO t VALUES ({}, '{}')", size, data);
        match execute(&sql, &mut pager, &mut catalog) {
            Ok(_) => max_success = size,
            Err(_) => break,
        }
    }
    // We should have succeeded for at least 3900 bytes
    assert!(max_success >= 3900, "Expected at least 3900 bytes to fit");
    // And it should have failed before 4200 bytes
    assert!(max_success < 4200, "Expected overflow before 4200 bytes");
}
