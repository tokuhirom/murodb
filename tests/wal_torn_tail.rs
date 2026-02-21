/// Integration tests for WAL torn tail recovery.
///
/// These tests simulate various kinds of WAL tail corruption that can occur
/// when a crash happens during a WAL write. Each test creates a database with
/// committed data, appends corruption to the WAL tail, runs recovery, and
/// verifies that committed data is intact and no panic occurs.
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

/// Helper: create a DB with one committed tx, then write a second committed tx
/// to WAL (without checkpoint) so recovery has something to replay.
/// Appends corruption after the second tx's WAL records.
fn setup_committed_with_pending_wal() -> (TempDir, std::path::PathBuf, std::path::PathBuf, u64) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    // Tx 1: baseline
    let mut tx1 = murodb::tx::transaction::Transaction::begin(1, 0);
    let mut page = tx1.allocate_page(&mut pager).unwrap();
    page.insert_cell(b"baseline").unwrap();
    tx1.write_page(page);
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    // Tx 2: committed to WAL but not checkpointed
    let mut tx2 = murodb::tx::transaction::Transaction::begin(2, wal.current_lsn());
    let mut page0 = pager.read_page(0).unwrap();
    page0.insert_cell(b"updated").unwrap();
    tx2.write_page(page0);
    tx2.commit(&mut pager, &mut wal, 0).unwrap();
    // Do NOT checkpoint — leave WAL records for recovery

    let next_lsn = wal.current_lsn();
    drop(pager);
    drop(wal);

    (dir, db_path, wal_path, next_lsn)
}

/// Append raw bytes to a file.
fn append_bytes(path: &std::path::Path, bytes: &[u8]) {
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn test_truncated_frame_at_tail() {
    // Simulate a crash that wrote a partial WAL frame: a frame header claiming
    // 500 bytes but only 10 bytes of payload actually written.
    let (_dir, db_path, wal_path, _next_lsn) = setup_committed_with_pending_wal();

    // Append truncated frame
    append_bytes(&wal_path, &500u32.to_le_bytes());
    append_bytes(&wal_path, &[0xDE; 10]);

    // Recovery should succeed and replay tx2
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be recovered despite truncated tail frame"
    );

    // Verify data integrity
    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"baseline".as_slice()));
    assert_eq!(page0.cell(1), Some(b"updated".as_slice()));
}

#[test]
fn test_garbled_bytes_at_tail() {
    // Random garbage bytes appended after valid WAL records.
    let (_dir, db_path, wal_path, _next_lsn) = setup_committed_with_pending_wal();

    // Append random-looking garbage (not a valid frame header)
    let garbage: Vec<u8> = (0..37).map(|i| (i * 7 + 13) as u8).collect();
    append_bytes(&wal_path, &garbage);

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be recovered despite garbled tail"
    );

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"baseline".as_slice()));
    assert_eq!(page0.cell(1), Some(b"updated".as_slice()));
}

#[test]
fn test_plausible_length_garbage_at_tail() {
    // Garbage with a valid-looking frame length (small, within bounds) but
    // undecryptable payload. The reader must use content probing to determine
    // this is tail, not mid-log corruption.
    let (_dir, db_path, wal_path, _next_lsn) = setup_committed_with_pending_wal();

    // Fake frame: length=50 (plausible), payload=50 bytes of garbage
    append_bytes(&wal_path, &50u32.to_le_bytes());
    append_bytes(&wal_path, &[0xCA; 50]);
    // Another fake frame to make structural check uncertain
    append_bytes(&wal_path, &30u32.to_le_bytes());
    append_bytes(&wal_path, &[0xFE; 30]);

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be recovered despite plausible-length garbage at tail"
    );

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"baseline".as_slice()));
    assert_eq!(page0.cell(1), Some(b"updated".as_slice()));
}

#[test]
fn test_zero_filled_tail() {
    // Zero bytes at the tail simulate filesystem pre-allocation.
    // Zero frame length should be treated as end-of-log.
    let (_dir, db_path, wal_path, _next_lsn) = setup_committed_with_pending_wal();

    // Append a block of zero bytes (simulating pre-allocated space)
    append_bytes(&wal_path, &[0u8; 256]);

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be recovered despite zero-filled tail"
    );

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"baseline".as_slice()));
    assert_eq!(page0.cell(1), Some(b"updated".as_slice()));
}

#[test]
fn test_uncommitted_tx_at_tail() {
    // An uncommitted transaction (Begin + PagePut, no Commit) followed by
    // tail corruption. The uncommitted tx should be discarded and the
    // prior committed tx should survive.
    let (_dir, db_path, wal_path, next_lsn) = setup_committed_with_pending_wal();

    // Write an uncommitted transaction to the WAL using the correct LSN
    // so records are decryptable (not treated as garbage).
    {
        let mut wal = WalWriter::open(&wal_path, &test_key(), next_lsn).unwrap();
        wal.append(&WalRecord::Begin { txid: 99 }).unwrap();
        let page = Page::new(0);
        wal.append(&WalRecord::PagePut {
            txid: 99,
            page_id: 0,
            data: page.data.to_vec(),
        })
        .unwrap();
        wal.sync().unwrap();
        // No Commit record — simulates crash mid-transaction
    }

    // Append some garbage after the uncommitted records
    append_bytes(&wal_path, &[0xFF; 20]);

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    // tx2 should be committed, tx99 should not
    assert!(rr.committed_txids.contains(&2), "tx2 should be recovered");
    assert!(
        !rr.committed_txids.contains(&99),
        "uncommitted tx99 should not be committed"
    );

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"baseline".as_slice()));
    assert_eq!(page0.cell(1), Some(b"updated".as_slice()));
}
