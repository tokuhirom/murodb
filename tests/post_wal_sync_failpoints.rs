#![cfg(feature = "test-utils")]
/// Failpoint tests for post-WAL-sync data/meta write failures.
///
/// After WAL sync (the commit point), the commit flow writes dirty pages to the
/// data file and then calls flush_meta(). If either step fails in-process, the
/// current session sees an error. On restart, WAL recovery must replay the
/// committed transaction and converge to the correct state.
use murodb::crypto::aead::MasterKey;
use murodb::error::MuroError;
use murodb::sql::executor::ExecResult;
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
        matches!(&result, Err(MuroError::CommitInDoubt(_))),
        "commit must return CommitInDoubt when {} fails, got: {:?}",
        fail_at,
        result
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
    assert!(matches!(&result, Err(MuroError::CommitInDoubt(_))));

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
    assert!(matches!(&result, Err(MuroError::CommitInDoubt(_))));

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

// ── Session poison tests ──

#[test]
fn test_session_poisoned_after_commit_in_doubt() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Get mutable access to pager via session to inject failure
    let mut session = db.into_session();

    // Inject write_page failure so next commit triggers CommitInDoubt
    session
        .pager_mut()
        .set_inject_write_page_failure(Some(std::io::ErrorKind::Other));

    let result = session.execute("INSERT INTO t VALUES (1, 'alice')");
    assert!(
        matches!(&result, Err(MuroError::CommitInDoubt(_))),
        "expected CommitInDoubt, got: {:?}",
        result
    );

    // Subsequent operations must fail with SessionPoisoned
    let result = session.execute("SELECT * FROM t");
    assert!(
        matches!(&result, Err(MuroError::SessionPoisoned(_))),
        "expected SessionPoisoned, got: {:?}",
        result
    );

    // Even DDL must be rejected
    let result = session.execute("CREATE TABLE t2 (id BIGINT PRIMARY KEY)");
    assert!(
        matches!(&result, Err(MuroError::SessionPoisoned(_))),
        "expected SessionPoisoned for DDL, got: {:?}",
        result
    );
}

#[test]
fn test_reopen_after_commit_in_doubt_recovers_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    // Inject failure for the next commit
    let mut session = db.into_session();
    session
        .pager_mut()
        .set_inject_write_page_failure(Some(std::io::ErrorKind::Other));

    let result = session.execute("INSERT INTO t VALUES (2, 'bob')");
    assert!(matches!(&result, Err(MuroError::CommitInDoubt(_))));

    // Drop session to simulate close
    drop(session);

    // Reopen — WAL recovery should replay the committed transaction
    let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
    let rows = match db.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("expected Rows, got: {:?}", other),
    };
    assert_eq!(rows.len(), 2, "both rows should be recovered");
}

#[test]
fn test_explicit_tx_commit_in_doubt_poisons_session() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    let mut session = db.into_session();

    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO t VALUES (1)").unwrap();

    // Inject failure before COMMIT
    session
        .pager_mut()
        .set_inject_write_page_failure(Some(std::io::ErrorKind::Other));

    let result = session.execute("COMMIT");
    assert!(
        matches!(&result, Err(MuroError::CommitInDoubt(_))),
        "expected CommitInDoubt, got: {:?}",
        result
    );

    // Session must be poisoned
    let result = session.execute("SELECT * FROM t");
    assert!(
        matches!(&result, Err(MuroError::SessionPoisoned(_))),
        "expected SessionPoisoned, got: {:?}",
        result
    );
}

// ── checkpoint_truncate failure tests ──

#[test]
fn test_checkpoint_truncate_failure_data_survives() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB and insert baseline data
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    }

    // Reopen, inject checkpoint_truncate failure, commit new data
    {
        let db = murodb::Database::open(&db_path, &test_key()).unwrap();
        let mut session = db.into_session();

        // Inject checkpoint_truncate failure
        session
            .wal_mut()
            .set_inject_checkpoint_truncate_failure(Some(std::io::ErrorKind::Other));

        // This commit should succeed (WAL sync + page writes succeed),
        // but checkpoint_truncate will fail (WAL not truncated).
        let result = session.execute("INSERT INTO t VALUES (2, 'bob')");
        // The commit itself should succeed — checkpoint failure is non-fatal
        assert!(
            result.is_ok(),
            "commit should succeed even if checkpoint_truncate fails: {:?}",
            result
        );

        drop(session);
    }

    // Reopen — WAL recovery replays (idempotently) because WAL wasn't truncated
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        let rows = match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => rows,
            other => panic!("expected Rows, got: {:?}", other),
        };
        assert_eq!(rows.len(), 2, "both rows should be present after recovery");
    }
}

#[test]
fn test_checkpoint_truncate_failure_repeated_open() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB with data, then commit with checkpoint_truncate failure
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();

        let mut session = db.into_session();
        session
            .wal_mut()
            .set_inject_checkpoint_truncate_failure(Some(std::io::ErrorKind::Other));
        session
            .execute("INSERT INTO t VALUES (2, 'second')")
            .unwrap();
        drop(session);
    }

    // First reopen
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        let rows = match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => rows,
            other => panic!("expected Rows, got: {:?}", other),
        };
        assert_eq!(rows.len(), 2, "first reopen: both rows should be present");
    }

    // Second reopen — should also see correct data
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        let rows = match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => rows,
            other => panic!("expected Rows, got: {:?}", other),
        };
        assert_eq!(
            rows.len(),
            2,
            "second reopen: both rows should still be present"
        );
    }
}
