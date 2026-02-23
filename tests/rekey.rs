use murodb::types::Value;
use murodb::Database;
use tempfile::TempDir;

#[test]
fn test_basic_rekey() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create with old password, insert data
    {
        let mut db = Database::create_with_password(&db_path, "old_pass").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

        // Rekey with new password
        db.rekey_with_password("new_pass").unwrap();
    }

    // Reopen with new password — should work
    {
        let mut db = Database::open_with_password(&db_path, "new_pass").unwrap();
        let rows = db.query("SELECT * FROM t ORDER BY id ASC").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
        assert_eq!(rows[1].get("name"), Some(&Value::Varchar("Bob".into())));
    }

    // Reopen with old password — should fail
    {
        let result = Database::open_with_password(&db_path, "old_pass");
        assert!(result.is_err());
    }
}

#[test]
fn test_multi_page_rekey() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Insert enough data to span many pages
    {
        let mut db = Database::create_with_password(&db_path, "old").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, data VARCHAR)")
            .unwrap();
        for i in 0..200 {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, '{}')",
                i,
                "x".repeat(100)
            ))
            .unwrap();
        }
        db.rekey_with_password("new").unwrap();
    }

    // Verify all data after rekey
    {
        let mut db = Database::open_with_password(&db_path, "new").unwrap();
        let rows = db.query("SELECT COUNT(*) AS cnt FROM t").unwrap();
        assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(200)));
    }
}

#[test]
fn test_fts_after_rekey() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut db = Database::create_with_password(&db_path, "old").unwrap();
        db.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE FULLTEXT INDEX fts_body ON docs (body)")
            .unwrap();
        db.execute("INSERT INTO docs VALUES (1, '東京タワーは東京にあります')")
            .unwrap();
        db.execute("INSERT INTO docs VALUES (2, '大阪城は大阪にあります')")
            .unwrap();

        db.rekey_with_password("new").unwrap();
    }

    // Verify FTS queries work after rekey
    {
        let mut db = Database::open_with_password(&db_path, "new").unwrap();
        let rows = db
            .query(
                "SELECT id FROM docs WHERE MATCH(body) AGAINST('東京' IN BOOLEAN MODE) > 0 ORDER BY id",
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    }
}

#[test]
fn test_double_rekey() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut db = Database::create_with_password(&db_path, "pass1").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();

        // Rekey twice
        db.rekey_with_password("pass2").unwrap();
        db.rekey_with_password("pass3").unwrap();
    }

    // Only pass3 should work
    assert!(Database::open_with_password(&db_path, "pass1").is_err());
    assert!(Database::open_with_password(&db_path, "pass2").is_err());

    let mut db = Database::open_with_password(&db_path, "pass3").unwrap();
    let rows = db.query("SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(rows[0].get("v"), Some(&Value::Varchar("hello".into())));
}

#[test]
fn test_rekey_reject_in_transaction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = Database::create_with_password(&db_path, "pass").unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.execute("BEGIN").unwrap();

    let result = db.rekey_with_password("new");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("cannot be used inside a transaction"));

    // Rollback to clean up
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn test_rekey_reject_plaintext() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = Database::create_plaintext(&db_path).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    let result = db.rekey_with_password("new");
    assert!(result.is_err());
    let err_msg = format!("{}", result.unwrap_err());
    assert!(err_msg.contains("not supported for plaintext"));
}

#[test]
fn test_crash_recovery_completed_rekey() {
    use murodb::storage::pager::{rekey_marker_path, Pager};
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB, insert data, do a normal rekey
    {
        let mut db = Database::create_with_password(&db_path, "old").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
        db.rekey_with_password("new").unwrap();
    }

    // Simulate a stale marker file left over (rekey completed but marker not deleted)
    let info = Pager::read_encryption_info_from_file(&db_path).unwrap();
    let marker_path = rekey_marker_path(&db_path);
    {
        // Write a marker with the current salt (rekey completed)
        let mut buf = [0u8; 36];
        buf[0..4].copy_from_slice(b"REKY");
        buf[4..20].copy_from_slice(&info.salt);
        buf[20..28].copy_from_slice(&1u64.to_le_bytes()); // epoch doesn't matter
        let checksum = murodb::wal::record::crc32(&buf[0..32]);
        buf[32..36].copy_from_slice(&checksum.to_le_bytes());
        let mut f = std::fs::File::create(&marker_path).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }

    // Open should succeed (marker is cleaned up)
    {
        let mut db = Database::open_with_password(&db_path, "new").unwrap();
        let rows = db.query("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(rows[0].get("v"), Some(&Value::Integer(42)));
    }

    // Marker should be removed
    assert!(!marker_path.exists());
}

#[test]
fn test_crash_recovery_interrupted_rekey_completes_with_new_password() {
    use murodb::crypto::aead::PageCrypto;
    use murodb::crypto::kdf;
    use murodb::crypto::suite::{EncryptionSuite, PageCipher};
    use murodb::storage::pager::{rekey_marker_path, Pager};
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let old_password = "old_pass";
    let new_password = "new_pass";

    // Create DB with old password and data.
    {
        let mut db = Database::create_with_password(&db_path, old_password).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'before')").unwrap();
    }

    // Simulate an interrupted rekey before any page rewrite:
    // header still has old salt/epoch, but marker exists with new salt/epoch
    // and wrapped old master key.
    let info = Pager::read_encryption_info_from_file(&db_path).unwrap();
    let old_epoch = {
        let mut f = std::fs::File::open(&db_path).unwrap();
        let mut hdr = [0u8; 76];
        use std::io::Read;
        f.read_exact(&mut hdr).unwrap();
        u64::from_le_bytes(hdr[44..52].try_into().unwrap())
    };
    let new_epoch = old_epoch + 1;
    let new_salt = [0x55u8; 16];
    let old_key = kdf::derive_key(old_password.as_bytes(), &info.salt).unwrap();
    let new_key = kdf::derive_key(new_password.as_bytes(), &new_salt).unwrap();

    let wrap_cipher = PageCipher::new(EncryptionSuite::Aes256GcmSiv, Some(&new_key)).unwrap();
    let wrapped_old_key = wrap_cipher
        .encrypt(u64::MAX, new_epoch, old_key.as_bytes())
        .unwrap();
    assert_eq!(wrapped_old_key.len(), PageCrypto::overhead() + 32);

    let marker_path = rekey_marker_path(&db_path);
    {
        // Marker layout (v2):
        // 0..4 magic, 4..20 new_salt, 20..28 new_epoch, 28..32 flags=1, 32..36 crc32(0..32), 36.. wrapped_old_key
        let mut buf = vec![0u8; 36 + wrapped_old_key.len()];
        buf[0..4].copy_from_slice(b"REKY");
        buf[4..20].copy_from_slice(&new_salt);
        buf[20..28].copy_from_slice(&new_epoch.to_le_bytes());
        buf[28..32].copy_from_slice(&1u32.to_le_bytes());
        let checksum = murodb::wal::record::crc32(&buf[0..32]);
        buf[32..36].copy_from_slice(&checksum.to_le_bytes());
        buf[36..].copy_from_slice(&wrapped_old_key);
        let mut f = std::fs::File::create(&marker_path).unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
    }

    // Open with new password should recover interrupted rekey and succeed.
    {
        let mut db = Database::open_with_password(&db_path, new_password).unwrap();
        let rows = db.query("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("v"), Some(&Value::Varchar("before".into())));
    }

    // Old password should no longer work after recovery.
    assert!(Database::open_with_password(&db_path, old_password).is_err());
    assert!(!marker_path.exists());
}

#[test]
fn test_rekey_data_integrity_with_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut db = Database::create_with_password(&db_path, "old").unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, age BIGINT)")
            .unwrap();
        db.execute("CREATE INDEX idx_name ON t (name)").unwrap();
        for i in 0..50 {
            db.execute(&format!(
                "INSERT INTO t VALUES ({}, 'user_{}', {})",
                i,
                i,
                20 + i
            ))
            .unwrap();
        }

        db.rekey_with_password("new").unwrap();
    }

    // Verify data and index work after rekey
    {
        let mut db = Database::open_with_password(&db_path, "new").unwrap();
        let rows = db.query("SELECT * FROM t WHERE name = 'user_25'").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("age"), Some(&Value::Integer(45)));

        let rows = db.query("SELECT COUNT(*) AS cnt FROM t").unwrap();
        assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(50)));
    }
}

#[test]
fn test_parse_alter_database_rekey_removed() {
    use murodb::sql::parser::parse_sql;

    let err = parse_sql("ALTER DATABASE REKEY WITH PASSWORD 'mypass'").unwrap_err();
    assert!(err.contains("Only ALTER TABLE is supported"));
}

#[test]
fn test_parse_alter_database_rekey_case_insensitive_removed() {
    use murodb::sql::parser::parse_sql;

    let err = parse_sql("alter database rekey with password 'test'").unwrap_err();
    assert!(err.contains("Only ALTER TABLE is supported"));
}
