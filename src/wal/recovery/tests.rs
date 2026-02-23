use super::*;
use crate::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

#[test]
fn test_recovery_committed_tx() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create initial database
    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    // Write WAL with committed transaction
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();

        // Write a full page of data
        let mut page = Page::new(1);
        page.insert_cell(b"recovered data").unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                page_count: 2,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    // Run recovery
    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids.len(), 1);
    assert_eq!(result.pages_replayed, 1);
    assert!(result.skipped.is_empty());
}

#[test]
fn test_recovery_uncommitted_tx_discarded() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    // Write WAL with uncommitted transaction (no Commit record = crash simulation)
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let page = Page::new(1);
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: page.data.to_vec(),
            })
            .unwrap();
        // No commit - simulating crash
        writer.sync().unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(result.committed_txids.is_empty());
    assert_eq!(result.pages_replayed, 0);
    assert!(result.skipped.is_empty());
}

#[test]
fn test_recovery_no_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(result.committed_txids.is_empty());
    assert_eq!(result.pages_replayed, 0);
    assert!(result.skipped.is_empty());
}

#[test]
fn test_recover_permissive_alias_skips_invalid_transaction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer.sync().unwrap();
    }

    assert!(recover(&db_path, &wal_path, &test_key()).is_err());

    let result = recover_permissive(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.pages_replayed, 0);
    assert_eq!(result.skipped.len(), 1);
    assert_eq!(result.skipped[0].code, RecoverySkipCode::DuplicateBegin);
}

#[test]
fn test_recovery_restores_page_count() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    // Create initial database, allocate a page so page_count=1
    let page_data;
    {
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut page = pager.allocate_page().unwrap();
        page.insert_cell(b"initial").unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();

        // Create another page image for WAL (simulating a tx that wrote page 1)
        let mut p = Page::new(1);
        p.insert_cell(b"from wal").unwrap();
        page_data = p.data.to_vec();
    }

    // Write WAL with committed tx that updates page 1 and metadata.
    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: page_data,
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 42,
                page_count: 2,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    recover(&db_path, &wal_path, &test_key()).unwrap();

    // Verify metadata was restored
    let pager = Pager::open(&db_path, &test_key()).unwrap();
    assert!(pager.page_count() >= 2);
    assert_eq!(pager.catalog_root(), 42);
    assert_eq!(pager.epoch(), 0);
}

#[test]
fn test_recovery_rejects_commit_lsn_mismatch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                page_count: 1,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        // Actual LSN here is 2, but declared as 999.
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 999 })
            .unwrap();
        writer.sync().unwrap();
    }

    let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
    match err {
        MuroError::Wal(msg) => assert!(msg.contains("Commit LSN mismatch")),
        other => panic!("Expected WAL error, got: {:?}", other),
    }
}

#[test]
fn test_recovery_rejects_duplicate_terminal_record_for_tx() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: Page::new(0).data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                page_count: 1,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        // Conflicting terminal record should be rejected.
        writer.append(&WalRecord::Abort { txid: 1 }).unwrap();
        writer.sync().unwrap();
    }

    let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
    match err {
        MuroError::Wal(msg) => assert!(msg.contains("Duplicate terminal record")),
        other => panic!("Expected WAL error, got: {:?}", other),
    }
}

#[test]
fn test_recovery_rejects_commit_without_meta_update() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 1 })
            .unwrap();
        writer.sync().unwrap();
    }

    let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
    match err {
        MuroError::Wal(msg) => assert!(msg.contains("Commit without MetaUpdate")),
        other => panic!("Expected WAL error, got: {:?}", other),
    }
}

#[test]
fn test_recovery_rejects_pageput_before_begin() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 0,
                data: Page::new(0).data.to_vec(),
            })
            .unwrap();
        writer.sync().unwrap();
    }

    let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
    match err {
        MuroError::Wal(msg) => assert!(msg.contains("PagePut before Begin")),
        other => panic!("Expected WAL error, got: {:?}", other),
    }
}

#[test]
fn test_recovery_rejects_pageput_page_id_mismatch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();

        let page = Page::new(999);
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                page_count: 2,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
    match err {
        MuroError::Wal(msg) => assert!(msg.contains("page_id mismatch")),
        other => panic!("Expected WAL error, got: {:?}", other),
    }
}

#[test]
fn test_recovery_permissive_ignores_commit_without_meta() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 1 })
            .unwrap();
        writer.sync().unwrap();
    }

    let result =
        recover_with_mode(&db_path, &wal_path, &test_key(), RecoveryMode::Permissive).unwrap();
    assert_eq!(result.pages_replayed, 0);
    assert!(result.committed_txids.is_empty());
    assert!(result.aborted_txids.is_empty());
    assert_eq!(result.skipped.len(), 1);
    assert_eq!(result.skipped[0].txid, 1);
    assert_eq!(
        result.skipped[0].code,
        RecoverySkipCode::CommitWithoutMetaUpdate
    );
    assert!(result.skipped[0]
        .reason
        .contains("Commit without MetaUpdate"));
}

#[test]
fn test_recovery_permissive_ignores_page_id_mismatch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        let bad_page = Page::new(999);
        writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 1,
                data: bad_page.data.to_vec(),
            })
            .unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 1,
                catalog_root: 0,
                page_count: 2,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 3 })
            .unwrap();
        writer.sync().unwrap();
    }

    let result =
        recover_with_mode(&db_path, &wal_path, &test_key(), RecoveryMode::Permissive).unwrap();
    assert_eq!(result.pages_replayed, 0);
    assert_eq!(result.committed_txids, vec![1]);
    assert!(result.skipped.is_empty());
}

#[test]
fn test_inspect_wal_permissive_reports_skipped_reason() {
    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("test.wal");

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer
            .append(&WalRecord::Commit { txid: 1, lsn: 1 })
            .unwrap();
        writer.sync().unwrap();
    }

    let result = inspect_wal(&wal_path, &test_key(), RecoveryMode::Permissive).unwrap();
    assert_eq!(result.skipped.len(), 1);
    assert_eq!(
        result.skipped[0].code,
        RecoverySkipCode::CommitWithoutMetaUpdate
    );
    assert!(result.skipped[0]
        .reason
        .contains("Commit without MetaUpdate"));
    assert!(result.committed_txids.is_empty());
    assert_eq!(result.pages_replayed, 0);
}

#[test]
fn test_recovery_terminal_txids_are_sorted() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    {
        let _pager = Pager::create(&db_path, &test_key()).unwrap();
    }

    {
        let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();

        writer.append(&WalRecord::Begin { txid: 10 }).unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 10,
                catalog_root: 0,
                page_count: 1,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 10, lsn: 2 })
            .unwrap();

        writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        writer
            .append(&WalRecord::MetaUpdate {
                txid: 2,
                catalog_root: 0,
                page_count: 1,
                freelist_page_id: 0,
                epoch: 0,
            })
            .unwrap();
        writer
            .append(&WalRecord::Commit { txid: 2, lsn: 5 })
            .unwrap();

        writer.append(&WalRecord::Begin { txid: 7 }).unwrap();
        writer.append(&WalRecord::Abort { txid: 7 }).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer.append(&WalRecord::Abort { txid: 1 }).unwrap();

        writer.sync().unwrap();
    }

    let result = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert_eq!(result.committed_txids, vec![2, 10]);
    assert_eq!(result.aborted_txids, vec![1, 7]);
}
