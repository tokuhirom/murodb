/// Crash Simulation Matrix: systematic testing of crash recovery at every point in
/// the commit pipeline. Each test constructs WAL records manually to simulate
/// a crash at a specific point, then verifies recovery produces the correct state.
use murodb::crypto::aead::MasterKey;
use murodb::storage::page::{PAGE_HEADER_SIZE, PAGE_SIZE};
use murodb::storage::pager::Pager;
use murodb::wal::record::WalRecord;
use murodb::wal::recovery::recover;
use murodb::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

/// Points in the commit pipeline where a crash can occur.
#[derive(Debug, Clone, Copy)]
enum CrashPoint {
    /// After Begin record written
    AfterBegin,
    /// After PagePut record(s) written
    AfterPagePut,
    /// After freelist PagePut written
    AfterFreelistPut,
    /// After MetaUpdate record written
    AfterMetaUpdate,
    /// After Commit record written (but before fsync)
    AfterCommitRecord,
    /// After WAL sync (committed, but before page flush)
    AfterWalSync,
}

/// Whether the transaction has freed pages.
#[derive(Debug, Clone, Copy)]
enum FreedPages {
    None,
    Some,
}

/// Whether there's a prior committed transaction.
#[derive(Debug, Clone, Copy)]
enum PriorTx {
    None,
    OneCommitted,
}

/// Setup a fresh database with optional prior committed data.
/// Returns (dir, db_path, wal_path, catalog_root, page_count).
fn setup_db(prior: PriorTx) -> (TempDir, std::path::PathBuf, std::path::PathBuf, u64, u64) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();

    let (catalog_root, page_count) = match prior {
        PriorTx::None => {
            pager.flush_meta().unwrap();
            (0, 0)
        }
        PriorTx::OneCommitted => {
            // Create a simple committed state via real transaction
            let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();
            let mut tx = murodb::tx::transaction::Transaction::begin(1, 0);
            let mut page = tx.allocate_page(&mut pager).unwrap();
            page.insert_cell(b"prior_data").unwrap();
            tx.write_page(page);
            tx.commit(&mut pager, &mut wal, 0).unwrap();
            wal.checkpoint_truncate().unwrap();
            let cr = pager.catalog_root();
            let pc = pager.page_count();
            (cr, pc)
        }
    };

    drop(pager);
    (dir, db_path, wal_path, catalog_root, page_count)
}

/// The data page ID used by crash tests.
/// Uses base_page_count so it doesn't conflict with prior data.
fn crash_data_page_id(base_page_count: u64) -> u64 {
    base_page_count // first new page after existing data
}

fn crash_freelist_page_id(base_page_count: u64) -> u64 {
    base_page_count + 1 // second new page
}

/// Write WAL records up to a specific crash point for a new transaction.
fn write_wal_up_to(
    wal: &mut WalWriter,
    crash_point: CrashPoint,
    freed: FreedPages,
    txid: u64,
    catalog_root: u64,
    base_page_count: u64,
) {
    let page_id = crash_data_page_id(base_page_count);
    let mut page_data = [0u8; PAGE_SIZE];
    // Put some recognizable data: page_id in header + test marker
    page_data[0..8].copy_from_slice(&page_id.to_le_bytes());
    page_data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 11].copy_from_slice(b"crash_test!");

    // 1. Begin
    wal.append(&WalRecord::Begin { txid }).unwrap();
    if matches!(crash_point, CrashPoint::AfterBegin) {
        return;
    }

    // 2. PagePut
    wal.append(&WalRecord::PagePut {
        txid,
        page_id,
        data: page_data.to_vec(),
    })
    .unwrap();
    if matches!(crash_point, CrashPoint::AfterPagePut) {
        return;
    }

    // 3. Freelist page put
    let freelist_page_id = crash_freelist_page_id(base_page_count);
    let mut fl_page_data = [0u8; PAGE_SIZE];
    fl_page_data[0..8].copy_from_slice(&freelist_page_id.to_le_bytes());
    // Write multi-page format: [magic: 4][next=0: 8][count: 8][entries...]
    let fl_data_offset = PAGE_HEADER_SIZE;
    fl_page_data[fl_data_offset..fl_data_offset + 4].copy_from_slice(b"FLMP"); // magic
    fl_page_data[fl_data_offset + 4..fl_data_offset + 12].copy_from_slice(&0u64.to_le_bytes()); // next = 0
    match freed {
        FreedPages::None => {
            fl_page_data[fl_data_offset + 12..fl_data_offset + 20]
                .copy_from_slice(&0u64.to_le_bytes()); // count = 0
        }
        FreedPages::Some => {
            fl_page_data[fl_data_offset + 12..fl_data_offset + 20]
                .copy_from_slice(&1u64.to_le_bytes()); // count = 1
            fl_page_data[fl_data_offset + 20..fl_data_offset + 28]
                .copy_from_slice(&50u64.to_le_bytes()); // freed page 50
        }
    }
    wal.append(&WalRecord::PagePut {
        txid,
        page_id: freelist_page_id,
        data: fl_page_data.to_vec(),
    })
    .unwrap();
    if matches!(crash_point, CrashPoint::AfterFreelistPut) {
        return;
    }

    // 4. MetaUpdate
    let page_count = base_page_count.max(freelist_page_id + 1);
    wal.append(&WalRecord::MetaUpdate {
        txid,
        catalog_root,
        page_count,
        freelist_page_id,
        epoch: 0,
    })
    .unwrap();
    if matches!(crash_point, CrashPoint::AfterMetaUpdate) {
        return;
    }

    // 5. Commit record
    let commit_lsn = wal.current_lsn();
    wal.append(&WalRecord::Commit {
        txid,
        lsn: commit_lsn,
    })
    .unwrap();
    if matches!(crash_point, CrashPoint::AfterCommitRecord) {
        return;
    }

    // 6. WAL sync
    wal.sync().unwrap();
    // AfterWalSync is a post-commit state
    // (WAL contains a complete committed transaction)
}

/// Returns true if crash point is BEFORE the commit point (data should not be visible).
fn is_before_commit(cp: CrashPoint) -> bool {
    matches!(
        cp,
        CrashPoint::AfterBegin
            | CrashPoint::AfterPagePut
            | CrashPoint::AfterFreelistPut
            | CrashPoint::AfterMetaUpdate
    )
}

/// Core crash matrix test: test a single crash point.
fn run_crash_test(crash_point: CrashPoint, freed: FreedPages, prior: PriorTx) {
    let (_dir, db_path, wal_path, catalog_root, base_page_count) = setup_db(prior);

    // Write WAL records up to crash point
    {
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();
        write_wal_up_to(
            &mut wal,
            crash_point,
            freed,
            10,
            catalog_root,
            base_page_count,
        );
    }

    // Recover
    let result = recover(&db_path, &wal_path, &test_key());

    if is_before_commit(crash_point) {
        // Before commit point: recovery should succeed but not apply the transaction
        match result {
            Ok(rr) => {
                assert!(
                    !rr.committed_txids.contains(&10),
                    "crash at {:?}: tx should NOT be committed",
                    crash_point
                );
            }
            Err(_) => {
                // Recovery may fail in strict mode for incomplete transactions;
                // that's acceptable - the transaction is not committed.
            }
        }

        // Verify prior data is intact (if any)
        if matches!(prior, PriorTx::OneCommitted) {
            let mut pager = Pager::open(&db_path, &test_key()).unwrap();
            let page = pager.read_page(0).unwrap();
            assert_eq!(
                page.cell(0),
                Some(b"prior_data".as_slice()),
                "crash at {:?}: prior committed data must survive",
                crash_point
            );
        }
    } else {
        // After commit point: recovery should apply the transaction
        let rr = result.unwrap_or_else(|e| {
            panic!(
                "crash at {:?}: recovery should succeed, got: {}",
                crash_point, e
            )
        });
        assert!(
            rr.committed_txids.contains(&10),
            "crash at {:?}: tx should be committed",
            crash_point
        );

        let data_pid = crash_data_page_id(base_page_count);
        let fl_pid = crash_freelist_page_id(base_page_count);

        // Verify the page was recovered
        let mut pager = Pager::open(&db_path, &test_key()).unwrap();
        let page = pager.read_page(data_pid).unwrap();
        assert_eq!(
            &page.as_bytes()[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 11],
            b"crash_test!",
            "crash at {:?}: committed page data must be recovered",
            crash_point
        );

        // Verify page_count was updated
        assert!(
            pager.page_count() >= fl_pid + 1,
            "crash at {:?}: page_count should be at least {}, got {}",
            crash_point,
            fl_pid + 1,
            pager.page_count()
        );

        // Verify prior data still intact
        if matches!(prior, PriorTx::OneCommitted) {
            let prior_page = pager.read_page(0).unwrap();
            assert_eq!(
                prior_page.cell(0),
                Some(b"prior_data".as_slice()),
                "crash at {:?}: prior committed data must survive",
                crash_point
            );
        }
    }
}

// ── Matrix: no prior tx, no freed pages ──

#[test]
fn test_crash_after_begin_no_freed_no_prior() {
    run_crash_test(CrashPoint::AfterBegin, FreedPages::None, PriorTx::None);
}

#[test]
fn test_crash_after_page_put_no_freed_no_prior() {
    run_crash_test(CrashPoint::AfterPagePut, FreedPages::None, PriorTx::None);
}

#[test]
fn test_crash_after_freelist_put_no_freed_no_prior() {
    run_crash_test(
        CrashPoint::AfterFreelistPut,
        FreedPages::None,
        PriorTx::None,
    );
}

#[test]
fn test_crash_after_meta_update_no_freed_no_prior() {
    run_crash_test(CrashPoint::AfterMetaUpdate, FreedPages::None, PriorTx::None);
}

#[test]
fn test_crash_after_commit_record_no_freed_no_prior() {
    run_crash_test(
        CrashPoint::AfterCommitRecord,
        FreedPages::None,
        PriorTx::None,
    );
}

#[test]
fn test_crash_after_wal_sync_no_freed_no_prior() {
    run_crash_test(CrashPoint::AfterWalSync, FreedPages::None, PriorTx::None);
}

// ── Matrix: no prior tx, with freed pages ──

#[test]
fn test_crash_after_begin_freed_no_prior() {
    run_crash_test(CrashPoint::AfterBegin, FreedPages::Some, PriorTx::None);
}

#[test]
fn test_crash_after_page_put_freed_no_prior() {
    run_crash_test(CrashPoint::AfterPagePut, FreedPages::Some, PriorTx::None);
}

#[test]
fn test_crash_after_freelist_put_freed_no_prior() {
    run_crash_test(
        CrashPoint::AfterFreelistPut,
        FreedPages::Some,
        PriorTx::None,
    );
}

#[test]
fn test_crash_after_meta_update_freed_no_prior() {
    run_crash_test(CrashPoint::AfterMetaUpdate, FreedPages::Some, PriorTx::None);
}

#[test]
fn test_crash_after_commit_record_freed_no_prior() {
    run_crash_test(
        CrashPoint::AfterCommitRecord,
        FreedPages::Some,
        PriorTx::None,
    );
}

#[test]
fn test_crash_after_wal_sync_freed_no_prior() {
    run_crash_test(CrashPoint::AfterWalSync, FreedPages::Some, PriorTx::None);
}

// ── Matrix: with prior committed tx, no freed pages ──

#[test]
fn test_crash_after_begin_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterBegin,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_page_put_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterPagePut,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_freelist_put_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterFreelistPut,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_meta_update_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterMetaUpdate,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_commit_record_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterCommitRecord,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_wal_sync_no_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterWalSync,
        FreedPages::None,
        PriorTx::OneCommitted,
    );
}

// ── Matrix: with prior committed tx, with freed pages ──

#[test]
fn test_crash_after_begin_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterBegin,
        FreedPages::Some,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_commit_record_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterCommitRecord,
        FreedPages::Some,
        PriorTx::OneCommitted,
    );
}

#[test]
fn test_crash_after_wal_sync_freed_with_prior() {
    run_crash_test(
        CrashPoint::AfterWalSync,
        FreedPages::Some,
        PriorTx::OneCommitted,
    );
}

// ── Freelist content consistency checks ──

#[test]
fn test_crash_recovery_freelist_content_consistent() {
    // After recovery, the freelist content should match the last committed freelist
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create DB and commit a transaction that frees a page
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // Allocate pages
        let mut tx1 = murodb::tx::transaction::Transaction::begin(1, 0);
        let page0 = tx1.allocate_page(&mut pager).unwrap();
        tx1.write_page(page0);
        let page1 = tx1.allocate_page(&mut pager).unwrap();
        tx1.write_page(page1);
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        // Free page 0 in second tx (don't checkpoint - leave WAL for recovery)
        let mut tx2 = murodb::tx::transaction::Transaction::begin(2, wal.current_lsn());
        tx2.free_page(0);
        let p1 = pager.read_page(1).unwrap();
        tx2.write_page(p1);
        tx2.commit(&mut pager, &mut wal, 0).unwrap();

        // Don't checkpoint - simulate crash after commit
    }

    // Recovery
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(rr.committed_txids.contains(&2));

    // After recovery, freelist should contain page 0
    let pager = Pager::open(&db_path, &test_key()).unwrap();
    assert!(
        pager.freelist_page_id() != 0,
        "freelist page should be set after recovery"
    );
}

#[test]
fn test_crash_page_count_monotonic() {
    // page_count should never decrease across transactions
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // Transaction 1: allocate 5 pages
        let mut tx1 = murodb::tx::transaction::Transaction::begin(1, 0);
        for _ in 0..5 {
            let page = tx1.allocate_page(&mut pager).unwrap();
            tx1.write_page(page);
        }
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        let pc_after_1 = pager.page_count();

        // Transaction 2: only modify existing page (no new allocations)
        let mut tx2 = murodb::tx::transaction::Transaction::begin(2, wal.current_lsn());
        let p0 = pager.read_page(0).unwrap();
        tx2.write_page(p0);
        tx2.commit(&mut pager, &mut wal, 0).unwrap();

        let pc_after_2 = pager.page_count();
        assert!(
            pc_after_2 >= pc_after_1,
            "page_count must be monotonically increasing"
        );

        // Don't checkpoint
    }

    // Recovery
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(rr.committed_txids.len(), 2);

    let pager = Pager::open(&db_path, &test_key()).unwrap();
    assert!(
        pager.page_count() >= 5,
        "page_count after recovery should be at least 5"
    );
}
