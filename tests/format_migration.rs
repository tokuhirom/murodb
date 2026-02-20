/// Tests for database format version migration policy.
use murodb::crypto::aead::MasterKey;
use murodb::storage::pager::Pager;
use murodb::wal::record::crc32;
use std::io::{Seek, SeekFrom, Write};
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

/// Helper: write a raw v2 header (with freelist_page_id and CRC over 0..60).
fn write_v2_header(
    path: &std::path::Path,
    salt: [u8; 16],
    catalog_root: u64,
    page_count: u64,
    epoch: u64,
    freelist_page_id: u64,
) {
    let mut header = [0u8; 64];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&2u32.to_le_bytes()); // version 2
    header[12..28].copy_from_slice(&salt);
    header[28..36].copy_from_slice(&catalog_root.to_le_bytes());
    header[36..44].copy_from_slice(&page_count.to_le_bytes());
    header[44..52].copy_from_slice(&epoch.to_le_bytes());
    header[52..60].copy_from_slice(&freelist_page_id.to_le_bytes());
    let checksum = crc32(&header[0..60]);
    header[60..64].copy_from_slice(&checksum.to_le_bytes());

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    file.write_all(&header).unwrap();
    file.sync_all().unwrap();
}

/// Helper: write raw bytes to a file (truncating any existing content).
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

// ── Crash-interruption tests for header format auto-migration ──

#[test]
fn test_v2_header_auto_upgrades_to_v3() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    write_v2_header(&db_path, [0u8; 16], 0, 0, 0, 0);

    {
        let _pager = Pager::open(&db_path, &test_key()).unwrap();
    }

    let header = read_raw_header(&db_path);
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    assert_eq!(version, 3, "v2 header should be auto-upgraded to v3");

    let stored_crc = u32::from_le_bytes(header[68..72].try_into().unwrap());
    let computed_crc = crc32(&header[0..68]);
    assert_eq!(stored_crc, computed_crc);
}

#[test]
fn test_v2_header_crc_mismatch_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    write_v2_header(&db_path, [0u8; 16], 0, 0, 0, 0);

    // Corrupt a field covered by the v2 CRC
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        file.seek(SeekFrom::Start(28)).unwrap();
        file.write_all(&99u64.to_le_bytes()).unwrap(); // corrupt catalog_root
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "v2 CRC mismatch should report corruption, got: {}",
        err
    );
}

#[test]
fn test_v3_header_crc_mismatch_rejected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.set_catalog_root(42);
        pager.flush_meta().unwrap();
    }

    // Corrupt catalog_root field (offset 28)
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
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "v3 CRC mismatch should report corruption, got: {}",
        err
    );
}

/// Simulates a crash during v1→v3 upgrade where the version field was
/// updated to 3 but the CRC was not yet written (torn write: only first
/// 68 bytes persisted, CRC at 68..72 is still zero from file extend).
#[test]
fn test_torn_v1_to_v3_upgrade_version_written_crc_missing() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Start with a valid v1 header (64 bytes)
    write_v1_header(&db_path, [0u8; 16], 0, 0, 0);

    // Simulate a torn v3 upgrade: rewrite as v3 but only 68 bytes (no CRC)
    let mut header = [0u8; 72];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&3u32.to_le_bytes()); // version 3
                                                        // salt, catalog_root, page_count, epoch, freelist_page_id, next_txid = 0
                                                        // CRC at 68..72 is zero — does NOT match crc32(header[0..68])
    write_raw(&db_path, &header);

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "torn v1→v3 upgrade with zero CRC should be detected, got: {}",
        err
    );
}

/// Simulates a crash during v2→v3 upgrade where the header was partially
/// extended: version changed to 3 but CRC at the new offset (68..72)
/// is still zero/stale.
#[test]
fn test_torn_v2_to_v3_upgrade_detected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Write a valid v2 header
    write_v2_header(&db_path, [0u8; 16], 42, 5, 1, 3);

    // Simulate torn upgrade: rewrite with version=3 but keep v2 CRC at
    // wrong offset (60..64) and zeros at v3 CRC offset (68..72).
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        // Change version to 3
        file.seek(SeekFrom::Start(8)).unwrap();
        file.write_all(&3u32.to_le_bytes()).unwrap();
        // Extend file to 72 bytes (next_txid + CRC = zeros)
        file.set_len(72).unwrap();
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "torn v2→v3 upgrade should be detected by CRC, got: {}",
        err
    );
}

/// Header shorter than 64 bytes should fail immediately.
#[test]
fn test_truncated_header_too_short() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Write only the magic + version (12 bytes)
    let mut header = [0u8; 12];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&1u32.to_le_bytes());
    write_raw(&db_path, &header);

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err(), "header < 64 bytes should fail");
}

/// v3 header with corrupted next_txid field should fail CRC check.
#[test]
fn test_v3_next_txid_corruption_detected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        pager.flush_meta().unwrap();
    }

    // Corrupt next_txid at offset 60
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        file.seek(SeekFrom::Start(60)).unwrap();
        file.write_all(&0xBADCAFEu64.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "corrupted next_txid should fail CRC, got: {}",
        err
    );
}

/// Simulates crash during v1→v3 upgrade that left file at exactly 64 bytes
/// (original v1 size) but version was changed to 3. The file is too short
/// for a valid v3 header (needs 72 bytes), so read falls back to reading
/// 64 bytes with zeros for 64..72 → CRC mismatch.
#[test]
fn test_v1_to_v3_upgrade_file_not_extended() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Write a 64-byte header with version=3 (simulating version byte
    // written but file not extended to 72 bytes)
    let mut header = [0u8; 64];
    header[0..8].copy_from_slice(b"MURODB01");
    header[8..12].copy_from_slice(&3u32.to_le_bytes());
    write_raw(&db_path, &header);

    // read_plaintext_header reads 72 bytes but only gets 64 → bytes_read=64 ≥ 64,
    // version=3 branch reads CRC at 68..72 from the zero-filled buffer → CRC mismatch
    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "64-byte file claiming v3 should fail CRC, got: {}",
        err
    );
}

/// Successful v1→v3 upgrade followed by reopen proves deterministic behavior.
#[test]
fn test_v1_upgrade_then_reopen_is_deterministic() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    write_v1_header(&db_path, [0u8; 16], 10, 0, 0);

    // First open: auto-upgrades v1→v3
    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 10);
    }

    // Second open: should work with v3 header
    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 10);
    }

    // Third open: still deterministic
    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 10);
    }
}

/// Successful v2→v3 upgrade preserves metadata fields.
#[test]
fn test_v2_upgrade_preserves_metadata() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    write_v2_header(&db_path, [0u8; 16], 7, 0, 0, 0);

    {
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 7);
        assert_eq!(pager.freelist_page_id(), 0);
    }

    // Verify upgraded to v3 and fields preserved in raw header
    let header = read_raw_header(&db_path);
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    assert_eq!(version, 3);
    let catalog_root = u64::from_le_bytes(header[28..36].try_into().unwrap());
    assert_eq!(catalog_root, 7);
    let fl_pid = u64::from_le_bytes(header[52..60].try_into().unwrap());
    assert_eq!(fl_pid, 0);
    // CRC is valid
    let stored_crc = u32::from_le_bytes(header[68..72].try_into().unwrap());
    let computed_crc = crc32(&header[0..68]);
    assert_eq!(stored_crc, computed_crc);
}

/// v2 header with corrupted freelist_page_id field should fail CRC.
#[test]
fn test_v2_freelist_page_id_corruption_detected() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    write_v2_header(&db_path, [0u8; 16], 0, 0, 0, 0);

    // Corrupt freelist_page_id at offset 52
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        file.seek(SeekFrom::Start(52)).unwrap();
        file.write_all(&999u64.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
    }

    let result = Pager::open(&db_path, &test_key());
    assert!(result.is_err());
    let err = format!(
        "{}",
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        }
    );
    assert!(
        err.contains("header corrupted"),
        "corrupted v2 freelist_page_id should fail CRC, got: {}",
        err
    );
}
