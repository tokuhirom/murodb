use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::crypto::aead::MasterKey;
use crate::error::Result;
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::storage::pager::Pager;
use crate::wal::reader::WalReader;
use crate::wal::record::{TxId, WalRecord};

/// Recover the database from WAL.
/// Replays committed transactions, discards uncommitted ones.
/// Restores page data, catalog_root, and page_count metadata.
pub fn recover(db_path: &Path, wal_path: &Path, master_key: &MasterKey) -> Result<RecoveryResult> {
    if !wal_path.exists() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
        });
    }

    let mut reader = WalReader::open(wal_path, master_key)?;
    let records = reader.read_all()?;

    if records.is_empty() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
        });
    }

    // Phase 1: Identify committed and aborted transactions
    let mut committed: HashSet<TxId> = HashSet::new();
    let mut aborted: HashSet<TxId> = HashSet::new();

    for (_, record) in &records {
        match record {
            WalRecord::Commit { txid, .. } => {
                committed.insert(*txid);
            }
            WalRecord::Abort { txid } => {
                aborted.insert(*txid);
            }
            _ => {}
        }
    }

    // Phase 2: Collect the latest page data and metadata from committed transactions
    let mut page_updates: HashMap<PageId, Vec<u8>> = HashMap::new();
    let mut latest_catalog_root: Option<u64> = None;
    let mut latest_page_count: Option<u64> = None;

    for (_, record) in &records {
        match record {
            WalRecord::PagePut {
                txid,
                page_id,
                data,
            } => {
                if committed.contains(txid) {
                    page_updates.insert(*page_id, data.clone());
                }
            }
            WalRecord::MetaUpdate {
                txid,
                catalog_root,
                page_count,
            } => {
                if committed.contains(txid) {
                    latest_catalog_root = Some(*catalog_root);
                    latest_page_count = Some(*page_count);
                }
            }
            _ => {}
        }
    }

    // Phase 3: Apply page updates to the database
    let mut pager = Pager::open(db_path, master_key)?;
    let mut pages_replayed = 0;

    for data in page_updates.values() {
        if data.len() == PAGE_SIZE {
            let mut page_data = [0u8; PAGE_SIZE];
            page_data.copy_from_slice(data);
            let page = Page::from_bytes(page_data);
            pager.write_page(&page)?;
            pages_replayed += 1;
        }
    }

    // Phase 4: Restore metadata from WAL MetaUpdate records
    if let Some(catalog_root) = latest_catalog_root {
        pager.set_catalog_root(catalog_root);
    }
    if let Some(page_count) = latest_page_count {
        // Only increase page_count, never decrease it
        if page_count > pager.page_count() {
            pager.set_page_count(page_count);
        }
    }

    // Also ensure page_count covers all replayed pages (fallback safety)
    for &page_id in page_updates.keys() {
        let needed = page_id + 1;
        if needed > pager.page_count() {
            pager.set_page_count(needed);
        }
    }

    pager.flush_meta()?;

    Ok(RecoveryResult {
        committed_txids: committed.into_iter().collect(),
        aborted_txids: aborted.into_iter().collect(),
        pages_replayed,
    })
}

#[derive(Debug)]
pub struct RecoveryResult {
    pub committed_txids: Vec<TxId>,
    pub aborted_txids: Vec<TxId>,
    pub pages_replayed: usize,
}

#[cfg(test)]
mod tests {
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

        // Write WAL with committed tx that updates page 1 and sets catalog_root=42
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
    }
}
