use murodb::types::Value;
use murodb::Database;
use tempfile::TempDir;

#[test]
fn test_query_refreshes_after_other_handle_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freshness.db");
    let password = "pw";

    let mut db1 = Database::create_with_password(&db_path, password).unwrap();
    db1.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    db1.execute("INSERT INTO t VALUES (1, 'one')").unwrap();

    let mut db2 = Database::open_with_password(&db_path, password).unwrap();

    // Warm db1 page cache with the pre-change view.
    let before = db1
        .query("SELECT id FROM t ORDER BY id")
        .expect("warm-up select must succeed");
    assert_eq!(before.len(), 1);

    db2.execute("INSERT INTO t VALUES (2, 'two')").unwrap();

    // db1 must observe db2's committed row without reopening.
    let after = db1
        .query("SELECT id FROM t ORDER BY id")
        .expect("select after external commit must succeed");
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(after[1].get("id"), Some(&Value::Integer(2)));
}

#[test]
fn test_execute_refresh_prevents_stale_page_overwrite() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("freshness_write.db");
    let password = "pw";

    let mut db1 = Database::create_with_password(&db_path, password).unwrap();
    db1.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    db1.execute("INSERT INTO t VALUES (1, 'one')").unwrap();

    let mut db2 = Database::open_with_password(&db_path, password).unwrap();
    db2.execute("INSERT INTO t VALUES (2, 'two')").unwrap();

    // db1 has stale cached pages from its first insert; the next write must refresh first.
    db1.execute("INSERT INTO t VALUES (3, 'three')").unwrap();

    let mut verify = Database::open_with_password(&db_path, password).unwrap();
    let rows = verify
        .query("SELECT id FROM t ORDER BY id")
        .expect("verification select must succeed");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
    assert_eq!(rows[2].get("id"), Some(&Value::Integer(3)));
}
