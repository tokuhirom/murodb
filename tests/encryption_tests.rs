use murodb::crypto::aead::{MasterKey, PageCrypto};
use murodb::crypto::kdf;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult};
use murodb::storage::pager::Pager;
use murodb::types::Value;
use tempfile::TempDir;

#[test]
fn test_encryption_roundtrip_full_page() {
    let key = MasterKey::new([0xAB; 32]);
    let crypto = PageCrypto::new(&key);

    // Test with various page sizes
    for size in [0, 1, 100, 1024, 4096] {
        let plaintext = vec![0x42u8; size];
        let encrypted = crypto.encrypt(0, 0, &plaintext).unwrap();
        let decrypted = crypto.decrypt(0, 0, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext, "Failed for size {}", size);
    }
}

#[test]
fn test_tamper_detection_every_byte() {
    let key = MasterKey::new([0xAB; 32]);
    let crypto = PageCrypto::new(&key);
    let plaintext = vec![0x42u8; 256];
    let encrypted = crypto.encrypt(0, 0, &plaintext).unwrap();

    // Flip each byte individually and verify decryption fails
    for i in 0..encrypted.len() {
        let mut tampered = encrypted.clone();
        tampered[i] ^= 0x01;
        assert!(
            crypto.decrypt(0, 0, &tampered).is_err(),
            "Tamper at byte {} not detected",
            i
        );
    }
}

#[test]
fn test_kdf_produces_valid_key() {
    let salt = kdf::generate_salt();
    let key = kdf::derive_key(b"my secret passphrase", &salt).unwrap();

    // Key should be usable for encryption
    let crypto = PageCrypto::new(&key);
    let encrypted = crypto.encrypt(0, 0, b"test data").unwrap();
    let decrypted = crypto.decrypt(0, 0, &encrypted).unwrap();
    assert_eq!(decrypted, b"test data");
}

#[test]
fn test_wrong_key_cannot_open_database() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let correct_key = MasterKey::new([0x42; 32]);
    let wrong_key = MasterKey::new([0x99; 32]);

    // Create database with correct key
    {
        let mut pager = Pager::create(&db_path, &correct_key).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();
        execute(
            "CREATE TABLE t (id INT64 PRIMARY KEY)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (1)", &mut pager, &mut catalog).unwrap();
        pager.flush_meta().unwrap();
    }

    // Try to open with wrong key - should fail
    {
        let result = Pager::open(&db_path, &wrong_key);
        assert!(result.is_err());
    }

    // Open with correct key - should work
    {
        let pager = Pager::open(&db_path, &correct_key);
        assert!(pager.is_ok());
    }
}

#[test]
fn test_data_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let key = MasterKey::new([0x42; 32]);

    let catalog_root;
    {
        let mut pager = Pager::create(&db_path, &key).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();

        execute(
            "CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'persistent')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        catalog_root = catalog.root_page_id();
        pager.flush_meta().unwrap();
    }

    // Reopen and verify data
    {
        let mut pager = Pager::open(&db_path, &key).unwrap();
        let mut catalog = SystemCatalog::open(catalog_root);

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].get("name"),
                Some(&Value::Varchar("persistent".into()))
            );
        } else {
            panic!("Expected rows");
        }
    }
}
