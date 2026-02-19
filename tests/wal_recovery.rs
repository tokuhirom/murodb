use murodb::crypto::aead::MasterKey;
use murodb::storage::page::Page;
use murodb::storage::pager::Pager;
use murodb::wal::reader::WalReader;
use murodb::wal::record::WalRecord;
use murodb::wal::recovery::recover;
use murodb::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

#[test]
fn test_wal_write_and_read() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 5,
                data: vec![0xAA; 100],
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 2 })
            .unwrap();

        writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 2,
                page_id: 6,
                data: vec![0xBB; 50],
            })
            .unwrap();
        writer.append(&WalRecord::Abort { txid: 2 }).unwrap();
        writer.sync().unwrap();
    }

    {
        let mut reader = WalReader::open(&wal_path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 6);
    }
}

#[test]
fn test_recovery_replays_committed_only() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create database
    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    // Write WAL with one committed and one uncommitted TX
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();

        // TX 1: committed
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page1 = Page::new(1);
        page1.insert_cell(b"committed data").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: page1.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 2 })
            .unwrap();

        // TX 2: uncommitted (simulating crash)
        writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        let mut page2 = Page::new(2);
        page2.insert_cell(b"uncommitted data").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 2,
                page_id: 2,
                data: page2.data.to_vec(),
            })
            .unwrap();
        // No commit for TX 2

        writer.sync().unwrap();
    }

    // Run recovery
    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids.len(), 1);
    assert!(result.committed_txids.contains(&1));
    assert_eq!(result.pages_replayed, 1);
}

#[test]
fn test_recovery_with_no_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("nonexistent.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(result.committed_txids.is_empty());
    assert_eq!(result.pages_replayed, 0);
}
