/// Tests for database format validation policy (v4-only).
use murodb::crypto::aead::MasterKey;
use murodb::crypto::suite::EncryptionSuite;
use murodb::storage::pager::Pager;
use murodb::wal::record::crc32;
use std::io::{Seek, SeekFrom, Write};
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn write_raw(path: &std::path::Path, data: &[u8]) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.write_all(data).unwrap();
    file.sync_all().unwrap();
}

fn read_raw_header_v4(path: &std::path::Path) -> [u8; 76] {
    let mut file = std::fs::File::open(path).unwrap();
    let mut header = [0u8; 76];
    use std::io::Read;
    file.read_exact(&mut header).unwrap();
    header
}

#[test]
fn test_v4_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.set_catalog_root(42);
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 42);
        assert_eq!(pager.page_count(), 0);
    }

    let header = read_raw_header_v4(&db_path);
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    assert_eq!(version, 4);
    let suite_id = u32::from_le_bytes(header[68..72].try_into().unwrap());
    assert_eq!(suite_id, EncryptionSuite::Aes256GcmSiv.id());
    let stored_crc = u32::from_le_bytes(header[72..76].try_into().unwrap());
    let computed_crc = crc32(&header[0..72]);
    assert_eq!(stored_crc, computed_crc);
}

#[test]
fn test_future_version_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut header = [0u8; 76];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&99u32.to_le_bytes());
    write_raw(&db_path, &header);

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let msg = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(msg.contains("unsupported database format version 99"));
}

#[test]
fn test_pre_v4_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    for version in [1u32, 2u32, 3u32] {
        let mut header = [0u8; 76];
        header[0..8].copy_from_slice(b"MURODB01");
        header[8..12].copy_from_slice(&version.to_le_bytes());
        write_raw(&db_path, &header);

        let result = Pager::open(&db_path, &test_key());
        assert!(result.is_err());
        let msg = format!(
            "{}",
            match result {
                Err(e) => e,
                Ok(_) => panic!("expected error"),
            }
        );
        assert!(
            msg.contains(&format!("unsupported database format version {}", version)),
            "unexpected error for version {}: {}",
            version,
            msg
        );
    }
}

#[test]
fn test_truncated_header_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut header = [0u8; 12];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&4u32.to_le_bytes());
    write_raw(&db_path, &header);

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
}

#[test]
fn test_v4_header_crc_mismatch_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.set_catalog_root(42);
        pager.flush_meta().unwrap();
    }

    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        file.seek(SeekFrom::Start(28)).unwrap();
        file.write_all(&0xDEADu64.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let msg = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(msg.contains("header corrupted"));
}

#[test]
fn test_unknown_suite_id_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        file.seek(SeekFrom::Start(68)).unwrap();
        file.write_all(&999u32.to_le_bytes()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut header = [0u8; 76];
        use std::io::Read;
        file.read_exact(&mut header).unwrap();
        let checksum = crc32(&header[0..72]);
        file.seek(SeekFrom::Start(72)).unwrap();
        file.write_all(&checksum.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let msg = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(msg.contains("unsupported encryption suite id 999"));
}
