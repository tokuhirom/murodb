use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::ExecResult;
use murodb::sql::session::Session;
use murodb::storage::pager::Pager;
use murodb::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn setup_session() -> (Session, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    (Session::new(pager, catalog, wal), dir)
}

fn count_rows(session: &mut Session, sql: &str) -> usize {
    match session.execute(sql).unwrap() {
        ExecResult::Rows(rows) => rows.len(),
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_begin_commit() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();
    session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    session.execute("COMMIT").unwrap();

    // After commit, rows should be visible
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 2);
}

#[test]
fn test_begin_rollback() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Insert one row without transaction (auto-commit)
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();

    // Start transaction, insert, then rollback
    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    session.execute("ROLLBACK").unwrap();

    // Only the auto-committed row should be visible
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 1);
}

#[test]
fn test_autocommit() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Without BEGIN, INSERT should be immediately committed
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();

    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 1);
}

#[test]
fn test_nested_begin_error() {
    let (mut session, _dir) = setup_session();

    session.execute("BEGIN").unwrap();
    let result = session.execute("BEGIN");
    assert!(result.is_err());
    // Clean up
    session.execute("ROLLBACK").unwrap();
}

#[test]
fn test_commit_without_begin() {
    let (mut session, _dir) = setup_session();
    let result = session.execute("COMMIT");
    assert!(result.is_err());
}

#[test]
fn test_rollback_without_begin() {
    let (mut session, _dir) = setup_session();
    let result = session.execute("ROLLBACK");
    assert!(result.is_err());
}

#[test]
fn test_ddl_in_transaction() {
    let (mut session, _dir) = setup_session();

    session.execute("BEGIN").unwrap();
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();
    session.execute("COMMIT").unwrap();

    // Table and data should be visible after commit
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 1);
}

#[test]
fn test_select_in_transaction() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    // SELECT within the transaction should see both rows (committed + dirty)
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 2);

    session.execute("COMMIT").unwrap();
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 2);
}

#[test]
fn test_update_in_transaction() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();

    session.execute("BEGIN").unwrap();
    session
        .execute("UPDATE t SET name = 'Alicia' WHERE id = 1")
        .unwrap();
    session.execute("COMMIT").unwrap();

    match session.execute("SELECT name FROM t WHERE id = 1").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("name"),
                Some(&murodb::types::Value::Varchar("Alicia".into()))
            );
        }
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_delete_in_transaction() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();
    session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    session.execute("BEGIN").unwrap();
    session.execute("DELETE FROM t WHERE id = 1").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 1);
}

#[test]
fn test_rollback_preserves_prior_data() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'Alice')")
        .unwrap();
    session.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

    session.execute("BEGIN").unwrap();
    session.execute("DELETE FROM t WHERE id = 1").unwrap();
    session
        .execute("INSERT INTO t VALUES (3, 'Charlie')")
        .unwrap();
    session.execute("ROLLBACK").unwrap();

    // Original data should be intact
    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 2);
}

#[test]
fn test_database_into_session() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let db = murodb::Database::create(&db_path, &test_key()).unwrap();
    let mut session = db.into_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    session.execute("COMMIT").unwrap();

    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 1);
}

#[test]
fn test_delete_updates_secondary_index() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'a@b.com')")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (2, 'c@d.com')")
        .unwrap();

    // Delete the row with email 'a@b.com'
    session.execute("DELETE FROM t WHERE id = 1").unwrap();

    // Now re-inserting the same email should succeed (index entry was removed)
    session
        .execute("INSERT INTO t VALUES (3, 'a@b.com')")
        .unwrap();

    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 2);
}

#[test]
fn test_update_updates_secondary_index() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'a@b.com')")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (2, 'c@d.com')")
        .unwrap();

    // Update email for id=1
    session
        .execute("UPDATE t SET email = 'new@b.com' WHERE id = 1")
        .unwrap();

    // Old email should now be available
    session
        .execute("INSERT INTO t VALUES (3, 'a@b.com')")
        .unwrap();

    assert_eq!(count_rows(&mut session, "SELECT * FROM t"), 3);
}

#[test]
fn test_update_unique_constraint_check() {
    let (mut session, _dir) = setup_session();

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE)")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'a@b.com')")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (2, 'c@d.com')")
        .unwrap();

    // Updating to a duplicate email should fail
    let result = session.execute("UPDATE t SET email = 'c@d.com' WHERE id = 1");
    assert!(result.is_err());
}

#[test]
fn test_wal_session_commit_rollback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create database, insert data, close
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
    }

    // Reopen and verify data persisted via WAL
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 2),
            _ => panic!("Expected rows"),
        }
    }
}
