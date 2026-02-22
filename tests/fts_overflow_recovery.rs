use murodb::crypto::aead::MasterKey;
#[cfg(feature = "test-utils")]
use murodb::error::MuroError;
use murodb::fts::index::{FtsIndex, FtsPendingOp};
use murodb::fts::query::query_natural;
use murodb::storage::pager::Pager;
use murodb::tx::page_store::TxPageStore;
use murodb::tx::transaction::Transaction;
use murodb::wal::recovery::recover;
use murodb::wal::writer::WalWriter;
use std::io::Write;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn term_key() -> [u8; 32] {
    [0x55u8; 32]
}

fn assert_overflow_doc_searchable(db_path: &std::path::Path, fts_root: u64) {
    let mut pager = Pager::open(db_path, &test_key()).unwrap();
    let idx = FtsIndex::open(fts_root, term_key());
    let results = query_natural(&idx, &mut pager, "aa").unwrap();
    assert!(
        results.iter().any(|r| r.doc_id == 1),
        "overflow-backed posting list should remain searchable after recovery"
    );
    let pl = idx.get_postings(&mut pager, "aa").unwrap();
    assert_eq!(pl.df(), 1);
    assert!(
        pl.get(1).unwrap().positions.len() >= 5000,
        "expected large posting positions to survive recovery"
    );
}

fn setup_committed_overflow_wal() -> (TempDir, std::path::PathBuf, std::path::PathBuf, u64) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    let mut tx1 = Transaction::begin(1, wal.current_lsn());
    let fts_root = {
        let mut store = TxPageStore::new(tx1, &mut pager);
        let idx = FtsIndex::create(&mut store, term_key()).unwrap();
        tx1 = store.into_tx();
        idx.root_page_id()
    };
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    let mut tx2 = Transaction::begin(2, wal.current_lsn());
    {
        let mut store = TxPageStore::new(tx2, &mut pager);
        let mut idx = FtsIndex::open(fts_root, term_key());
        idx.apply_pending(
            &mut store,
            &[FtsPendingOp::Add {
                doc_id: 1,
                text: "a".repeat(12_000),
            }],
        )
        .unwrap();
        tx2 = store.into_tx();
    }
    tx2.commit(&mut pager, &mut wal, 0).unwrap();

    drop(pager);
    drop(wal);
    (dir, db_path, wal_path, fts_root)
}

#[test]
fn test_fts_overflow_recovery_with_torn_wal_tail() {
    let (_dir, db_path, wal_path, fts_root) = setup_committed_overflow_wal();

    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Simulate crash during WAL frame append.
        file.write_all(&1024u32.to_le_bytes()).unwrap();
        file.write_all(&[0xAB; 11]).unwrap();
        file.sync_all().unwrap();
    }

    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "committed overflow transaction must survive torn WAL tail"
    );
    assert_overflow_doc_searchable(&db_path, fts_root);
}

#[cfg(feature = "test-utils")]
#[test]
fn test_fts_overflow_recovery_after_post_wal_sync_partial_write() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

    let mut tx1 = Transaction::begin(1, wal.current_lsn());
    let fts_root = {
        let mut store = TxPageStore::new(tx1, &mut pager);
        let idx = FtsIndex::create(&mut store, term_key()).unwrap();
        tx1 = store.into_tx();
        idx.root_page_id()
    };
    tx1.commit(&mut pager, &mut wal, 0).unwrap();
    wal.checkpoint_truncate().unwrap();

    let mut tx2 = Transaction::begin(2, wal.current_lsn());
    {
        let mut store = TxPageStore::new(tx2, &mut pager);
        let mut idx = FtsIndex::open(fts_root, term_key());
        idx.apply_pending(
            &mut store,
            &[FtsPendingOp::Add {
                doc_id: 1,
                text: "a".repeat(12_000),
            }],
        )
        .unwrap();
        tx2 = store.into_tx();
    }

    pager.set_inject_write_page_failure(Some(std::io::ErrorKind::Other));
    let commit_result = tx2.commit(&mut pager, &mut wal, 0);
    assert!(
        matches!(commit_result, Err(MuroError::CommitInDoubt(_))),
        "commit should be durable in WAL but fail during data-file write"
    );

    drop(pager);
    drop(wal);
    let rr = recover(&db_path, &wal_path, &test_key()).unwrap();
    assert!(
        rr.committed_txids.contains(&2),
        "committed overflow transaction should be replayed after partial write"
    );
    assert_overflow_doc_searchable(&db_path, fts_root);
}
