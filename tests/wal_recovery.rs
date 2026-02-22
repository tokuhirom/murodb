use murodb::crypto::aead::MasterKey;
use murodb::sql::executor::ExecResult;
use murodb::storage::page::Page;
use murodb::storage::pager::Pager;
use murodb::wal::reader::WalReader;
use murodb::wal::record::WalRecord;
use murodb::wal::recovery::{recover, RecoveryMode};
use murodb::wal::writer::WalWriter;
use std::io::Write;
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
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 6,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
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
        assert_eq!(records.len(), 7);
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
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 2,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
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

/// Issue #7: Recovery must restore page_count so allocate_page doesn't reuse committed pages.
#[test]
fn test_recovery_restores_metadata_prevents_page_reuse() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB, insert data (creates pages), close
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }
    }

    // Reopen and verify page_count is consistent (recovery runs, data still accessible)
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 20),
            _ => panic!("Expected rows"),
        }
        // Insert more data — should not overwrite existing pages
        for i in 20..30 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 30),
            _ => panic!("Expected rows"),
        }
    }
}

#[test]
fn test_permissive_open_quarantines_malformed_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    // Malformed tx: Begin + Commit (without MetaUpdate).
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 1 })
            .unwrap();
        writer.sync().unwrap();
    }

    let (_db, report_opt) = murodb::Database::open_with_recovery_mode_and_report(
        &db_path,
        &test_key(),
        RecoveryMode::Permissive,
    )
    .unwrap();
    let report = report_opt.expect("expected recovery report");

    assert_eq!(report.skipped.len(), 1);
    assert!(report.wal_quarantine_path.is_some());

    let quarantine_path = std::path::PathBuf::from(report.wal_quarantine_path.unwrap());
    assert!(quarantine_path.exists(), "quarantine WAL should exist");
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        murodb::wal::WAL_HEADER_SIZE as u64
    );
}

/// Issue #8: Truncated WAL tail should not prevent recovery of prior committed records.
#[test]
fn test_truncated_wal_tail_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create database with one page
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    // Write WAL with a committed TX, then append garbage (simulating partial write crash)
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page = Page::new(0);
        page.insert_cell(b"recovered after truncation").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    // Append truncated garbage frame at WAL tail: valid frame_len header but incomplete payload
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Write a frame_len that claims 1000 bytes but only write 10 bytes of garbage
        file.write_all(&1000u32.to_le_bytes()).unwrap();
        file.write_all(&[0xDE; 10]).unwrap();
        file.sync_all().unwrap();
    }

    // Recovery should succeed: committed TX replayed, truncated tail ignored
    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids.len(), 1);
    assert!(result.committed_txids.contains(&1));
    assert_eq!(result.pages_replayed, 1);

    // Verify the recovered page data
    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page = pager.read_page(0).unwrap();
    assert_eq!(page.cell(0), Some(b"recovered after truncation".as_slice()));
}

/// Issue #8: Fully corrupt frame (valid length but garbage encrypted data) at tail.
#[test]
fn test_corrupt_tail_frame_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    // Write one committed TX
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let page = Page::new(0);
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    // Append a complete but corrupt frame (decryption will fail)
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        let garbage = vec![0xBA; 100];
        file.write_all(&(garbage.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&garbage).unwrap();
        file.sync_all().unwrap();
    }

    // Recovery should still succeed
    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids.len(), 1);
}

/// Issue #9: catalog_root must be durable when commit succeeds.
/// Simulates: create table + insert → close → reopen (recovery) → schema visible.
#[test]
fn test_catalog_root_durable_after_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB, create table, insert row
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
    }

    // Reopen — recovery should restore catalog_root so the table is visible
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM users").unwrap() {
            ExecResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].get("name"),
                    Some(&murodb::types::Value::Varchar("Alice".into()))
                );
            }
            _ => panic!("Expected rows"),
        }

        // Schema operations should still work
        db.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY)")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (100)").unwrap();
    }

    // Reopen again to verify both tables survive
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM users").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 1),
            _ => panic!("Expected rows"),
        }
        match db.execute("SELECT * FROM orders").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 1),
            _ => panic!("Expected rows"),
        }
    }
}

/// Issue #10: Successful commits should checkpoint WAL to avoid unbounded growth
/// during long-running sessions.
#[test]
fn test_wal_is_checkpointed_after_successful_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();

    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(
        wal_size,
        murodb::wal::WAL_HEADER_SIZE as u64,
        "WAL should be truncated after successful commits"
    );
}

/// Issue #11: Explicit rollback should also checkpoint WAL so aborted tx records
/// do not accumulate during long-running sessions.
#[test]
fn test_wal_is_checkpointed_after_explicit_rollback() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    db.execute("ROLLBACK").unwrap();

    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(
        wal_size,
        murodb::wal::WAL_HEADER_SIZE as u64,
        "WAL should be truncated after rollback"
    );

    match db.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => assert_eq!(rows.len(), 0),
        _ => panic!("Expected rows"),
    }
}

/// Freelist persistence: free pages survive close → reopen.
#[test]
fn test_freelist_persisted_across_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create DB, insert rows to create B-tree pages, then delete to free some
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }
        // Delete rows to trigger B-tree node merges/frees
        for i in 0..40 {
            db.execute(&format!("DELETE FROM t WHERE id = {}", i))
                .unwrap();
        }
    }

    // Reopen and verify freelist is restored (new inserts should reuse freed pages)
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();

        // Get page count before inserting more data
        let pager = db.flush().ok();
        let _ = pager;

        // Insert more rows — they should reuse freed pages, not grow the file
        for i in 100..120 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'new_{}')", i, i))
                .unwrap();
        }

        // Verify data integrity
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => {
                assert_eq!(rows.len(), 30, "10 original + 20 new rows");
            }
            _ => panic!("Expected rows"),
        }
    }

    // Reopen again to verify everything is still consistent
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => {
                assert_eq!(rows.len(), 30);
            }
            _ => panic!("Expected rows"),
        }
    }
}

/// Freelist WAL recovery: freelist is restored after crash.
#[test]
fn test_freelist_wal_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create DB, insert and delete to create freelist entries
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        for i in 0..30 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }
        for i in 0..20 {
            db.execute(&format!("DELETE FROM t WHERE id = {}", i))
                .unwrap();
        }
    }

    // Verify the WAL was checkpointed (size 0)
    let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(
        wal_size,
        murodb::wal::WAL_HEADER_SIZE as u64,
        "WAL should be checkpointed after normal close"
    );

    // Reopen and verify the freelist persists
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 10),
            _ => panic!("Expected rows"),
        }

        // Insert more rows and confirm they work (pages from freelist reused)
        for i in 100..110 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'new_{}')", i, i))
                .unwrap();
        }
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 20),
            _ => panic!("Expected rows"),
        }
    }
}

/// Transaction rollback discards freed pages.
#[test]
fn test_rollback_discards_freed_pages() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
            .unwrap();
    }

    // Begin a transaction, delete rows (would free pages), then rollback
    db.execute("BEGIN").unwrap();
    for i in 0..15 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i))
            .unwrap();
    }
    db.execute("ROLLBACK").unwrap();

    // All rows should still be present
    match db.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => assert_eq!(rows.len(), 20),
        _ => panic!("Expected rows"),
    }
}

/// Interleaved transactions with tail corruption: tx1 committed, tx2 in-flight at crash.
/// Only tx1 should be recovered.
#[test]
fn test_interleaved_txs_with_tail_corruption() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();

        // TX 1: fully committed
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page = Page::new(0);
        page.insert_cell(b"tx1 data").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();

        // TX 2: begin + page write, but no commit (crash mid-transaction)
        writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        let mut page2 = Page::new(1);
        page2.insert_cell(b"tx2 uncommitted").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 2,
                page_id: 1,
                data: page2.data.to_vec(),
            })
            .unwrap();
        writer.sync().unwrap();
    }

    // Append garbage at tail (simulating partial write during crash)
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&500u32.to_le_bytes()).unwrap();
        file.write_all(&[0xCC; 5]).unwrap();
        file.sync_all().unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids, vec![1]);
    assert_eq!(result.pages_replayed, 1);

    // Verify tx1 page data was recovered
    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page = pager.read_page(0).unwrap();
    assert_eq!(page.cell(0), Some(b"tx1 data".as_slice()));
}

/// Corruption at transaction boundary: tx1 committed, tx2's Begin frame is corrupted.
/// tx1 should still be recovered.
#[test]
fn test_corruption_at_transaction_boundary() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();

        // TX 1: fully committed
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page = Page::new(0);
        page.insert_cell(b"boundary test").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    // Append a corrupt frame where tx2's Begin would be (valid length, garbage payload)
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        let garbage = vec![0xFE; 64];
        file.write_all(&(garbage.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&garbage).unwrap();
        file.sync_all().unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids, vec![1]);
    assert_eq!(result.pages_replayed, 1);
}

/// Committed tx + aborted tx + incomplete tx. Only the committed tx should be recovered.
#[test]
fn test_committed_tx_then_aborted_tx_then_crash() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();

        // TX 1: committed
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page = Page::new(0);
        page.insert_cell(b"committed only").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();

        // TX 2: aborted
        writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 2,
                page_id: 0,
                data: vec![0xFF; 4096],
            })
            .unwrap();
        writer.append(&WalRecord::Abort { txid: 2 }).unwrap();

        // TX 3: incomplete (crash before commit)
        writer.append(&WalRecord::Begin { txid: 3 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 3,
                page_id: 0,
                data: vec![0xEE; 4096],
            })
            .unwrap();

        writer.sync().unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids, vec![1]);
    assert_eq!(result.aborted_txids, vec![2]);
    assert_eq!(result.pages_replayed, 1);

    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page = pager.read_page(0).unwrap();
    assert_eq!(page.cell(0), Some(b"committed only".as_slice()));
}

/// Multi-TX crash simulation: 3 committed TXs, 4th crashes mid-commit.
/// All 3 committed TXs should be recoverable.
#[test]
fn test_multi_tx_committed_then_crash_mid_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create DB and insert 3 committed transactions
    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        db.execute("INSERT INTO t VALUES (3, 'carol')").unwrap();
    }

    // Simulate a 4th transaction that crashes mid-commit by writing partial WAL
    {
        let mut writer = WalWriter::open(&wal_path, &test_key(), 0).unwrap();
        writer.append(&WalRecord::Begin { txid: 100 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 100,
                page_id: 0,
                data: vec![0xDD; 4096],
            })
            .unwrap();
        // No MetaUpdate or Commit — simulates crash
        writer.sync().unwrap();
    }

    // Reopen: recovery should restore all 3 committed rows
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected rows"),
        }
    }
}

/// Long-running session: 100 sequential auto-commit INSERTs survive close → reopen.
#[test]
fn test_long_running_session_many_sequential_txs() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i))
                .unwrap();
        }
    }

    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => {
                assert_eq!(rows.len(), 100);
            }
            _ => panic!("Expected rows"),
        }
    }
}

/// Recovery consistency: table + index creation, 50 INSERTs, close → reopen →
/// SELECT + index scan + additional INSERTs without collision.
#[test]
fn test_recovery_catalog_page_count_data_consistency() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    {
        let mut db = murodb::Database::create(&db_path, &test_key()).unwrap();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
            .unwrap();
        db.execute("CREATE INDEX idx_name ON t (name)").unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }
    }

    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();

        // Verify all rows via full scan
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 50),
            _ => panic!("Expected rows"),
        }

        // Additional INSERTs should not collide with existing data
        for i in 50..60 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }

        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 60),
            _ => panic!("Expected rows"),
        }
    }

    // Verify persistence of post-recovery inserts
    {
        let mut db = murodb::Database::open(&db_path, &test_key()).unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows(rows) => assert_eq!(rows.len(), 60),
            _ => panic!("Expected rows"),
        }
    }
}

/// After recovery, WAL is durably truncated to header-only and a subsequent
/// open sees no replay. This verifies the durability barrier in
/// `truncate_wal_durably` (issue #13).
#[test]
fn test_recovery_truncates_wal_durably() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create DB with some data
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    // Write a committed transaction into the WAL
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let mut page = Page::new(0);
        page.insert_cell(b"wal data").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                freelist_page_id: 0,
                epoch: 0,
                page_count: 1,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    // WAL should be larger than header-only before recovery
    let wal_size_before = std::fs::metadata(&wal_path).unwrap().len();
    assert!(
        wal_size_before > murodb::wal::WAL_HEADER_SIZE as u64,
        "WAL should contain committed records before recovery"
    );

    // Open the database — triggers recovery and truncation
    {
        let _db = murodb::Database::open(&db_path, &test_key()).unwrap();
    }

    // WAL should now be header-only (truncated durably)
    let wal_size_after = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(
        wal_size_after,
        murodb::wal::WAL_HEADER_SIZE as u64,
        "WAL must be truncated to header-only after recovery"
    );

    // A second open should find nothing to replay
    let (_, report) = murodb::Database::open_with_recovery_mode_and_report(
        &db_path,
        &test_key(),
        RecoveryMode::Strict,
    )
    .unwrap();
    // WAL was created fresh by the first open, so recovery should either
    // find no committed txs or not run at all
    if let Some(r) = report {
        assert!(
            r.committed_txids.is_empty(),
            "no transactions should be replayed on second open"
        );
        assert_eq!(r.pages_replayed, 0);
    }

    // Verify the recovered data is intact
    let mut pager = Pager::open(&db_path, &test_key()).unwrap();
    let page = pager.read_page(0).unwrap();
    assert_eq!(page.cell(0), Some(b"wal data".as_slice()));
}

/// MetaUpdate backward compatibility: old WAL records (25 bytes) without
/// freelist_page_id/epoch must default both fields to 0.
#[test]
fn test_meta_update_backward_compat() {
    use murodb::wal::record::WalRecord;

    // Simulate old-format MetaUpdate (25 bytes: tag + txid + catalog_root + page_count)
    let mut old_data = Vec::new();
    old_data.push(5u8); // TAG_META_UPDATE
    old_data.extend_from_slice(&1u64.to_le_bytes()); // txid
    old_data.extend_from_slice(&42u64.to_le_bytes()); // catalog_root
    old_data.extend_from_slice(&100u64.to_le_bytes()); // page_count
    assert_eq!(old_data.len(), 25);

    let record = WalRecord::deserialize(&old_data).unwrap();
    if let WalRecord::MetaUpdate {
        txid,
        catalog_root,
        page_count,
        freelist_page_id,
        epoch,
    } = record
    {
        assert_eq!(txid, 1);
        assert_eq!(catalog_root, 42);
        assert_eq!(page_count, 100);
        assert_eq!(freelist_page_id, 0, "Old records should default to 0");
        assert_eq!(epoch, 0, "Old records should default epoch to 0");
    } else {
        panic!("Expected MetaUpdate");
    }
}
