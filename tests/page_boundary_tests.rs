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

/// VARCHAR 5000 bytes uses overflow pages and is stored/retrieved correctly.
#[test]
fn test_varchar_5000_bytes_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let data = "a".repeat(5000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar(data)));
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

/// Large values (10KB, 100KB) via overflow pages: insert and retrieve correctly.
#[test]
fn test_large_overflow_values() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    // 10KB value
    let data_10k = "x".repeat(10_000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", data_10k);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar(data_10k.clone())));

    // 100KB value
    let data_100k = "y".repeat(100_000);
    let sql = format!("INSERT INTO t VALUES (2, '{}')", data_100k);
    exec(&mut pager, &mut catalog, &sql);

    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 2");
    assert_eq!(
        rows[0].get("data"),
        Some(&Value::Varchar(data_100k.clone()))
    );
}

/// Update from inline to overflow and overflow to inline.
#[test]
fn test_update_inline_to_overflow_and_back() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    // Insert small (inline)
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'small')",
    );
    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar("small".into())));

    // Update to large (overflow)
    let big = "z".repeat(10_000);
    let sql = format!("UPDATE t SET data = '{}' WHERE id = 1", big);
    exec(&mut pager, &mut catalog, &sql);
    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar(big)));

    // Update back to small (inline)
    exec(
        &mut pager,
        &mut catalog,
        "UPDATE t SET data = 'tiny' WHERE id = 1",
    );
    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar("tiny".into())));
}

/// Delete overflow entries; pages should be freed.
#[test]
fn test_delete_overflow_entry() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    let big = "d".repeat(10_000);
    let sql = format!("INSERT INTO t VALUES (1, '{}')", big);
    exec(&mut pager, &mut catalog, &sql);

    exec(&mut pager, &mut catalog, "DELETE FROM t WHERE id = 1");
    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(0)));
}

/// Mixed inline and overflow entries with splits.
#[test]
fn test_mixed_inline_and_overflow_with_splits() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    for i in 1..=10 {
        let data = if i % 3 == 0 {
            "L".repeat(8000) // overflow
        } else {
            format!("small_{}", i) // inline
        };
        let sql = format!("INSERT INTO t VALUES ({}, '{}')", i, data);
        exec(&mut pager, &mut catalog, &sql);
    }

    // All 10 rows should be accessible
    let rows = query_rows(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(10)));

    // Check an overflow row
    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 3");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar("L".repeat(8000))));

    // Check an inline row
    let rows = query_rows(&mut pager, &mut catalog, "SELECT data FROM t WHERE id = 1");
    assert_eq!(rows[0].get("data"), Some(&Value::Varchar("small_1".into())));
}

/// Rows near the page boundary: insert rows of increasing size.
/// With overflow pages, all sizes should succeed.
#[test]
fn test_gradual_size_increase_with_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)",
    );

    for size in (3900..=5000).step_by(50) {
        let data = "x".repeat(size);
        let sql = format!("INSERT INTO t VALUES ({}, '{}')", size, data);
        exec(&mut pager, &mut catalog, &sql);
    }

    // All rows should be accessible
    for size in (3900..=5000).step_by(50) {
        let rows = query_rows(
            &mut pager,
            &mut catalog,
            &format!("SELECT data FROM t WHERE id = {}", size),
        );
        assert_eq!(rows.len(), 1);
        let expected = "x".repeat(size);
        assert_eq!(rows[0].get("data"), Some(&Value::Varchar(expected)));
    }
}
