#![cfg(feature = "test-utils")]
/// Failpoint tests for post-WAL-sync data/meta write failures.
///
/// After WAL sync (the commit point), the commit flow writes dirty pages to the
/// data file and then calls flush_meta(). If either step fails in-process, the
/// current session sees an error. On restart, WAL recovery must replay the
/// committed transaction and converge to the correct state.
use murodb::crypto::aead::MasterKey;
use murodb::storage::pager::Pager;
use murodb::tx::transaction::Transaction;
use murodb::wal::recovery::recover;
use murodb::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

/// Helper: set up a database with one committed transaction, then attempt a
/// second transaction that will fail at the specified post-WAL-sync point.
/// Returns (dir, db_path, wal_path) for recovery verification.
///
/// `fail_at` controls where the injected failure occurs:
///   - "write_page": Pager::write_page fails after WAL sync
///   - "flush_meta": Pager::flush_meta fails after WAL sync (pages written OK)
fn setup_with_post_sync_failure(
    fail_at: &str,
) -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    // Transaction 1: commit baseline data
    let mut tx1 = Transaction::begin(1, 0);
    let mut page = tx1.allocate_page(&mut pager).unwrap();
    page.insert_cell(b"baseline").unwrap();
    tx1.write_page(page);
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    // Transaction 2: modify data, inject failure after WAL sync
    let mut tx2 = Transaction::begin(2, wal.current_lsn());
    let mut page0 = pager.read_page(0).unwrap();
    page0.insert_cell(b"updated").unwrap();
    tx2.write_page(page0);

    // Inject the failure
    match fail_at {
        "write_page" => {
            pager.set_inject_write_page_failure(Some(std::io::ErrorKind::Other));
        }
        "flush_meta" => {
            pager.set_inject_flush_meta_failure(Some(std::io::ErrorKind::Other));
        }
        _ => panic!("unknown fail_at: {}", fail_at),
    }

    let result = tx2.commit(&mut pager, &mut wal, 0);
    assert!(
        result.is_err(),
        "commit must return error when {} fails",
        fail_at
    );

    // Drop pager and WAL to simulate process crash (WAL is NOT truncated)
    drop(pager);
    drop(wal);

    (dir, db_path, wal_path)
}

// ── write_page failure tests ──

#[test]
fn test_write_page_failure_returns_error() {
    let (_dir, _db_path, _wal_path) = setup_with_post_sync_failure("write_page");
    // The error assertion is inside setup_with_post_sync_failure
}

#[test]
fn test_write_page_failure_recovery_restores_committed_data() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("write_page");

    // Recovery should replay the committed transaction from WAL
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be in committed_txids after recovery"
    );

    // Verify page data is correct after recovery
    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();

    // Page should have both cells: "baseline" from tx1 and "updated" from tx2
    assert_eq!(
        page0.cell(0),
        Some(b"baseline".as_slice()),
        "baseline data must be present"
    );
    assert_eq!(
        page0.cell(1),
        Some(b"updated".as_slice()),
        "updated data must be recovered from WAL"
    );
}

#[test]
fn test_write_page_failure_recovery_metadata_correct() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("write_page");

    recover(&db_path, &wal_path, &test_key()).unwrap();

    let pager = Pager::open(&db_path, &test_key()).unwrap();
    assert!(
        pager.page_count() >= 1,
        "page_count should be at least 1 after recovery"
    );
    assert!(
        pager.freelist_page_id() != 0,
        "freelist_page_id should be set after recovery"
    );
}

// ── flush_meta failure tests ──

#[test]
fn test_flush_meta_failure_returns_error() {
    let (_dir, _db_path, _wal_path) = setup_with_post_sync_failure("flush_meta");
    // The error assertion is inside setup_with_post_sync_failure
}

#[test]
fn test_flush_meta_failure_recovery_restores_committed_data() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("flush_meta");

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "tx2 should be in committed_txids after recovery"
    );

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(
        page0.cell(0),
        Some(b"baseline".as_slice()),
        "baseline data must be present"
    );
    assert_eq!(
        page0.cell(1),
        Some(b"updated".as_slice()),
        "updated data must be recovered from WAL"
    );
}

#[test]
fn test_flush_meta_failure_recovery_metadata_correct() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("flush_meta");

    recover(&db_path, &wal_path, &test_key()).unwrap();

    let pager = Pager::open(&db_path, &test_key()).unwrap();
    assert!(
        pager.page_count() >= 1,
        "page_count should be at least 1 after recovery"
    );
    assert!(
        pager.freelist_page_id() != 0,
        "freelist_page_id should be set after recovery"
    );
}

// ── Tests with freed pages ──

#[test]
fn test_write_page_failure_with_freed_pages_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    // Transaction 1: allocate two pages
    let mut tx1 = Transaction::begin(1, 0);
    let mut page0 = tx1.allocate_page(&mut pager).unwrap();
    page0.insert_cell(b"page0_data").unwrap();
    tx1.write_page(page0);
    let mut page1 = tx1.allocate_page(&mut pager).unwrap();
    page1.insert_cell(b"page1_data").unwrap();
    tx1.write_page(page1);
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    // Transaction 2: free page 1, modify page 0
    let mut tx2 = Transaction::begin(2, wal.current_lsn());
    tx2.free_page(1);
    let mut page0_mod = pager.read_page(0).unwrap();
    page0_mod.insert_cell(b"modified").unwrap();
    tx2.write_page(page0_mod);

    // Inject write_page failure
    pager.set_inject_write_page_failure(Some(std::io::ErrorKind::Other));
    let result = tx2.commit(&mut pager, &mut wal, 0);
    assert!(result.is_err());

    drop(pager);
    drop(wal);

    // Recovery
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr.committed_txids.contains(&2));

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"page0_data".as_slice()));
    assert_eq!(page0.cell(1), Some(b"modified".as_slice()));
}

#[test]
fn test_flush_meta_failure_with_freed_pages_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    // Transaction 1: allocate two pages
    let mut tx1 = Transaction::begin(1, 0);
    let mut page0 = tx1.allocate_page(&mut pager).unwrap();
    page0.insert_cell(b"page0_data").unwrap();
    tx1.write_page(page0);
    let mut page1 = tx1.allocate_page(&mut pager).unwrap();
    page1.insert_cell(b"page1_data").unwrap();
    tx1.write_page(page1);
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    // Transaction 2: free page 1, modify page 0
    let mut tx2 = Transaction::begin(2, wal.current_lsn());
    tx2.free_page(1);
    let mut page0_mod = pager.read_page(0).unwrap();
    page0_mod.insert_cell(b"modified").unwrap();
    tx2.write_page(page0_mod);

    // Inject flush_meta failure (pages are written, but metadata isn't fsynced)
    pager.set_inject_flush_meta_failure(Some(std::io::ErrorKind::Other));
    let result = tx2.commit(&mut pager, &mut wal, 0);
    assert!(result.is_err());

    drop(pager);
    drop(wal);

    // Recovery
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr.committed_txids.contains(&2));

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(page0.cell(0), Some(b"page0_data".as_slice()));
    assert_eq!(page0.cell(1), Some(b"modified".as_slice()));
}

// ── Multiple transactions: verify prior committed data survives ──

#[test]
fn test_write_page_failure_prior_tx_survives() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("write_page");

    recover(&db_path, &wal_path, &test_key()).unwrap();

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    // "baseline" from tx1 must still be there
    assert_eq!(
        page0.cell(0),
        Some(b"baseline".as_slice()),
        "prior committed data must survive write_page failure + recovery"
    );
}

#[test]
fn test_flush_meta_failure_prior_tx_survives() {
    let (_dir, db_path, wal_path) = setup_with_post_sync_failure("flush_meta");

    recover(&db_path, &wal_path, &test_key()).unwrap();

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page0 = pager.read_page(0).unwrap();
    assert_eq!(
        page0.cell(0),
        Some(b"baseline".as_slice()),
        "prior committed data must survive flush_meta failure + recovery"
    );
}
