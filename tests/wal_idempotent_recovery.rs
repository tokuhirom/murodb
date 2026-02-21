/// Integration tests for idempotent WAL recovery.
///
/// Recovery must be idempotent: running `recover()` multiple times on the same
/// WAL must produce identical logical database state. This is critical because
/// a crash during recovery itself must not corrupt the database.
///
/// Note: We compare decrypted page content rather than raw file bytes because
/// each page write re-encrypts with a new nonce, producing different ciphertext.
use murodb::crypto::aead::MasterKey;
use murodb::storage::page::Page;
use murodb::storage::pager::Pager;
use murodb::wal::record::WalRecord;
use murodb::wal::recovery::recover;
use murodb::wal::writer::WalWriter;
use std::io::Write;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

/// Copy a WAL file so we can run recovery again with the same input.
fn copy_wal(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::copy(src, dst).unwrap();
}

/// Read all pages from the database and return their decrypted content + metadata.
fn read_logical_state(db_path: &std::path::Path) -> (Vec<Vec<u8>>, u64, u64, u64) {
    let mut pager = Pager::open(db_path, &test_key()).unwrap();
    let page_count = pager.page_count();
    let catalog_root = pager.catalog_root();
    let freelist_page_id = pager.freelist_page_id();

    let mut pages = Vec::new();
    for i in 0..page_count {
        let page = pager.read_page(i).unwrap();
        pages.push(page.as_bytes().to_vec());
    }

    (pages, page_count, catalog_root, freelist_page_id)
}

#[test]
fn test_idempotent_recovery_single_tx() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let wal_backup = dir.path().join("test.wal.bak");

    // Create DB and commit one transaction to WAL (no checkpoint)
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        let mut tx = murodb::tx::transaction::Transaction::begin(1, 0);
        let mut page = tx.allocate_page(&mut pager).unwrap();
        page.insert_cell(b"hello").unwrap();
        tx.write_page(page);
        tx.commit(&mut pager, &mut wal, 0).unwrap();
        // No checkpoint — WAL has records for recovery
    }

    // Save a copy of the WAL for the second recovery
    copy_wal(&wal_path, &wal_backup);

    // First recovery
    let rr1 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr1.committed_txids.contains(&1));
    let state1 = read_logical_state(&db_path);

    // Restore WAL from backup for second recovery
    std::fs::copy(&wal_backup, &wal_path).unwrap();

    // Second recovery
    let rr2 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr2.committed_txids.contains(&1));
    let state2 = read_logical_state(&db_path);

    // Logical state must be identical
    assert_eq!(state1.0, state2.0, "page contents must be identical");
    assert_eq!(state1.1, state2.1, "page_count must be identical");
    assert_eq!(state1.2, state2.2, "catalog_root must be identical");
    assert_eq!(state1.3, state2.3, "freelist_page_id must be identical");
}

#[test]
fn test_idempotent_recovery_multiple_tx() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let wal_backup = dir.path().join("test.wal.bak");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // Tx 1: committed
        let mut tx1 = murodb::tx::transaction::Transaction::begin(1, 0);
        let mut page = tx1.allocate_page(&mut pager).unwrap();
        page.insert_cell(b"tx1_data").unwrap();
        tx1.write_page(page);
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        // Tx 2: committed (modifies existing page)
        let mut tx2 = murodb::tx::transaction::Transaction::begin(2, wal.current_lsn());
        let mut page0 = pager.read_page(0).unwrap();
        page0.insert_cell(b"tx2_data").unwrap();
        tx2.write_page(page0);
        tx2.commit(&mut pager, &mut wal, 0).unwrap();

        // Tx 3: uncommitted (Begin + PagePut, no Commit — simulates crash)
        wal.append(&WalRecord::Begin { txid: 3 }).unwrap();
        let abort_page = Page::new(0);
        wal.append(&WalRecord::PagePut {
            txid: 3,
            page_id: 0,
            data: abort_page.data.to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();
        // No checkpoint
    }

    copy_wal(&wal_path, &wal_backup);

    // First recovery
    let rr1 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr1.committed_txids.contains(&1));
    assert!(rr1.committed_txids.contains(&2));
    assert!(!rr1.committed_txids.contains(&3));
    let state1 = read_logical_state(&db_path);

    // Restore WAL and run recovery again
    std::fs::copy(&wal_backup, &wal_path).unwrap();
    let rr2 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr2.committed_txids.contains(&1));
    assert!(rr2.committed_txids.contains(&2));
    let state2 = read_logical_state(&db_path);

    assert_eq!(state1.0, state2.0, "page contents must be identical");
    assert_eq!(state1.1, state2.1, "page_count must be identical");
    assert_eq!(state1.2, state2.2, "catalog_root must be identical");
    assert_eq!(state1.3, state2.3, "freelist_page_id must be identical");
}

#[test]
fn test_idempotent_recovery_with_torn_tail() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let wal_backup = dir.path().join("test.wal.bak");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        let mut tx = murodb::tx::transaction::Transaction::begin(1, 0);
        let mut page = tx.allocate_page(&mut pager).unwrap();
        page.insert_cell(b"durable").unwrap();
        tx.write_page(page);
        tx.commit(&mut pager, &mut wal, 0).unwrap();
        // No checkpoint
    }

    // Append tail corruption (truncated frame + garbage)
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&200u32.to_le_bytes()).unwrap();
        file.write_all(&[0xAB; 15]).unwrap(); // truncated payload
        file.sync_all().unwrap();
    }

    copy_wal(&wal_path, &wal_backup);

    // First recovery
    let rr1 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr1.committed_txids.contains(&1));
    let state1 = read_logical_state(&db_path);

    // Restore WAL (with corruption intact) and recover again
    std::fs::copy(&wal_backup, &wal_path).unwrap();
    let rr2 = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr2.committed_txids.contains(&1));
    let state2 = read_logical_state(&db_path);

    assert_eq!(state1.0, state2.0, "page contents must be identical");
    assert_eq!(state1.1, state2.1, "page_count must be identical");
    assert_eq!(state1.2, state2.2, "catalog_root must be identical");
    assert_eq!(state1.3, state2.3, "freelist_page_id must be identical");
}
