use murodb::crypto::aead::MasterKey;
use murodb::sql::executor::ExecResult;
use murodb::types::Value;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn query_rows(db: &mut murodb::Database, sql: &str) -> Vec<murodb::sql::executor::Row> {
    match db.execute(sql).unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_backup_encrypted_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    // Create and populate source DB
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();

        db.backup(&backup_path).unwrap();
    }

    // Open backup and verify data
    {
        let mut db = murodb::Database::open(&backup_path, &test_key()).unwrap();
        let rows = query_rows(&mut db, "SELECT id, name FROM users ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[1].1, Value::Varchar("alice".to_string()));
        assert_eq!(rows[1].values[1].1, Value::Varchar("bob".to_string()));
    }
}

#[test]
fn test_backup_plaintext_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    {
        let mut db = murodb::Database::create_plaintext(&db_path).unwrap();
        db.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, val VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO items VALUES (10, 'hello')")
            .unwrap();

        db.backup(&backup_path).unwrap();
    }

    {
        let mut db = murodb::Database::open_plaintext(&backup_path).unwrap();
        let rows = query_rows(&mut db, "SELECT id, val FROM items");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[1].1, Value::Varchar("hello".to_string()));
    }
}

#[test]
fn test_backup_independence_from_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    // Create DB, insert data, backup
    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'before')").unwrap();
    db.backup(&backup_path).unwrap();

    // Write more data to source AFTER backup
    db.execute("INSERT INTO t VALUES (2, 'after')").unwrap();
    db.execute("UPDATE t SET v = 'modified' WHERE id = 1")
        .unwrap();
    drop(db);

    // Backup should still have original data only
    let mut backup_db = murodb::Database::open(&backup_path, &test_key()).unwrap();
    let rows = query_rows(&mut backup_db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[1].1, Value::Varchar("before".to_string()));
}

#[test]
fn test_backup_with_password() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    {
        let mut db = murodb::Database::create_with_password(&db_path, "secret123").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (42)").unwrap();
        db.backup(&backup_path).unwrap();
    }

    // Backup should open with the same password
    {
        let mut db = murodb::Database::open_with_password(&backup_path, "secret123").unwrap();
        let rows = query_rows(&mut db, "SELECT id FROM t");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0].1, Value::Integer(42));
    }
}

#[test]
fn test_backup_empty_db() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.backup(&backup_path).unwrap();
    }

    // Backup of empty DB should open successfully
    let mut db = murodb::Database::open(&backup_path, &test_key()).unwrap();
    let rows = query_rows(&mut db, "SHOW TABLES");
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_backup_with_indexes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let backup_path = dir.path().join("backup.db");

    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, age BIGINT)")
            .unwrap();
        db.execute("CREATE INDEX idx_name ON t (name)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice', 30)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob', 25)").unwrap();
        db.backup(&backup_path).unwrap();
    }

    {
        let mut db = murodb::Database::open(&backup_path, &test_key()).unwrap();
        // Verify data accessible via index
        let rows = query_rows(&mut db, "SELECT id FROM t WHERE name = 'alice'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0].1, Value::Integer(1));
    }
}

#[test]
fn test_backup_to_same_file_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // Backup to the same path must fail, not corrupt the source
    let err = db.backup(&db_path).unwrap_err();
    assert!(
        err.to_string().contains("same file"),
        "expected same-file error, got: {}",
        err
    );

    // Source DB must still be intact
    let rows = query_rows(&mut db, "SELECT id FROM t");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_backup_to_symlink_of_source_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("source.db");
    let link_path = dir.path().join("link.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    std::os::unix::fs::symlink(&db_path, &link_path).unwrap();

    let err = db.backup(&link_path).unwrap_err();
    assert!(
        err.to_string().contains("same file"),
        "expected same-file error via symlink, got: {}",
        err
    );
}
