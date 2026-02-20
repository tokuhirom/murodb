/// Tests for database format version migration policy.
use murodb::crypto::aead::MasterKey;
use murodb::storage::pager::Pager;
use std::io::Write;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

/// Helper: write a raw v1 header (no CRC, no freelist_page_id).
fn write_v1_header(
    path: &std::path::Path,
    salt: [u8; 16],
    catalog_root: u64,
    page_count: u64,
    epoch: u64,
) {
    let mut header = [0u8; 64];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&1u32.to_le_bytes()); // version 1
    header[12..28].copy_from_slice(&salt);
    header[28..36].copy_from_slice(&catalog_root.to_le_bytes());
    header[36..44].copy_from_slice(&page_count.to_le_bytes());
    header[44..52].copy_from_slice(&epoch.to_le_bytes());
    // bytes 52..64 are zero (no freelist_page_id, no CRC in v1)

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.write_all(&header).unwrap();
    file.sync_all().unwrap();
}

/// Helper: read the raw header from a file (v3 = 72 bytes).
fn read_raw_header(path: &std::path::Path) -> Vec<u8> {
    let mut file = std::fs::File::open(path).unwrap();
    let mut header = vec![0u8; 72];
    use std::io::Read;
    file.read_exact(&mut header).unwrap();
    header
}

#[test]
fn test_v1_header_auto_upgrades_to_v3() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Write a v1 header with page_count=0 (no pages needed for key verification)
    write_v1_header(&db_path, [0u8; 16], 0, 0, 0);

    // Open should succeed and auto-upgrade to v3
    {
        let _pager = Pager::open(&db_path, &test_key()).unwrap();
    }

    // Verify the header was upgraded to v3
    let header = read_raw_header(&db_path);
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    assert_eq!(version, 3, "v1 header should be auto-upgraded to v3");

    // Verify CRC is now present and valid (v3: CRC over 0..68 at offset 68..72)
    let stored_crc = u32::from_le_bytes(header[68..72].try_into().unwrap());
    let computed_crc = murodb::wal::record::crc32(&header[0..68]);
    assert_eq!(
        stored_crc, computed_crc,
        "upgraded header should have valid CRC"
    );

    // Reopen should succeed without issues
    {
        let _pager = Pager::open(&db_path, &test_key()).unwrap();
    }
}

#[test]
fn test_future_version_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Write a header with version=99
    let mut header = [0u8; 72];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&99u32.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .read(true)
        .open(&db_path)
        .unwrap();
    file.write_all(&header).unwrap();
    file.sync_all().unwrap();

    let result = Pager::open(&db_path, &test_key());
    match result {
        Err(e) => {
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("unsupported database format version 99"),
                "error should mention version: {}",
                err_msg
            );
        }
        Ok(_) => panic!("future version should be rejected"),
    }
}

#[test]
fn test_v3_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create a v3 database with some metadata
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.set_catalog_root(42);
        pager.flush_meta().unwrap();
    }

    // Reopen and verify all fields preserved
    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 42);
        assert_eq!(pager.page_count(), 0);
    }

    // Verify header is v3
    let header = read_raw_header(&db_path);
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    assert_eq!(version, 3);

    // Verify CRC is valid (v3: CRC over 0..68 at offset 68..72)
    let stored_crc = u32::from_le_bytes(header[68..72].try_into().unwrap());
    let computed_crc = murodb::wal::record::crc32(&header[0..68]);
    assert_eq!(stored_crc, computed_crc);
}
