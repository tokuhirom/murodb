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
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
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
        wal_size, 0,
        "WAL should be truncated after successful commits"
    );
}
