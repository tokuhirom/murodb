use std::collections::HashMap;

use crate::error::{MuroError, Result};
use crate::storage::page::{Page, PageId};
use crate::storage::pager::Pager;
use crate::wal::record::{Lsn, TxId, WalRecord};
use crate::wal::writer::WalWriter;

/// Transaction states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    Aborted,
}

/// A write transaction that buffers dirty pages and writes them
/// to the WAL on commit.
pub struct Transaction {
    txid: TxId,
    state: TxState,
    snapshot_lsn: Lsn,
    dirty_pages: HashMap<PageId, Page>,
}

impl Transaction {
    pub fn begin(txid: TxId, snapshot_lsn: Lsn) -> Self {
        Transaction {
            txid,
            state: TxState::Active,
            snapshot_lsn,
            dirty_pages: HashMap::new(),
        }
    }

    pub fn txid(&self) -> TxId {
        self.txid
    }

    pub fn state(&self) -> TxState {
        self.state
    }

    pub fn snapshot_lsn(&self) -> Lsn {
        self.snapshot_lsn
    }

    /// Read a page: first check dirty buffer, then fall back to pager.
    pub fn read_page(&self, pager: &mut Pager, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.dirty_pages.get(&page_id) {
            return Ok(page.clone());
        }
        pager.read_page(page_id)
    }

    /// Write a page into the dirty buffer.
    pub fn write_page(&mut self, page: Page) {
        self.dirty_pages.insert(page.page_id(), page);
    }

    /// Allocate a new page through the pager.
    pub fn allocate_page(&mut self, pager: &mut Pager) -> Result<Page> {
        let page = pager.allocate_page()?;
        Ok(page)
    }

    /// Commit: write dirty pages to WAL, then flush to pager.
    pub fn commit(&mut self, pager: &mut Pager, wal: &mut WalWriter) -> Result<Lsn> {
        if self.state != TxState::Active {
            return Err(MuroError::Transaction(
                "Cannot commit non-active transaction".into(),
            ));
        }

        // Write Begin record
        wal.append(&WalRecord::Begin { txid: self.txid })?;

        // Write all dirty pages to WAL
        for (page_id, page) in &self.dirty_pages {
            wal.append(&WalRecord::PagePut {
                txid: self.txid,
                page_id: *page_id,
                data: page.data.to_vec(),
            })?;
        }

        // Write Commit record
        let commit_lsn = wal.current_lsn();
        wal.append(&WalRecord::Commit {
            txid: self.txid,
            lsn: commit_lsn,
        })?;

        // Fsync the WAL
        wal.sync()?;

        // Now flush dirty pages to the data file
        for page in self.dirty_pages.values() {
            pager.write_page(page)?;
        }
        pager.flush_meta()?;

        self.state = TxState::Committed;
        self.dirty_pages.clear();

        Ok(commit_lsn)
    }

    /// Rollback: discard dirty pages.
    pub fn rollback(&mut self, wal: &mut WalWriter) -> Result<()> {
        if self.state != TxState::Active {
            return Err(MuroError::Transaction(
                "Cannot rollback non-active transaction".into(),
            ));
        }

        wal.append(&WalRecord::Abort { txid: self.txid })?;
        self.dirty_pages.clear();
        self.state = TxState::Aborted;
        Ok(())
    }

    /// Number of dirty pages.
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_pages.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    #[test]
    fn test_transaction_commit() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        let mut tx = Transaction::begin(1, 0);

        // Allocate and modify a page within the transaction
        let mut page = tx.allocate_page(&mut pager).unwrap();
        page.insert_cell(b"tx data").unwrap();
        tx.write_page(page);

        assert_eq!(tx.dirty_page_count(), 1);

        // Commit
        let lsn = tx.commit(&mut pager, &mut wal).unwrap();
        assert_eq!(tx.state(), TxState::Committed);
        assert!(lsn > 0);

        // Verify data is persisted
        let page = pager.read_page(0).unwrap();
        assert_eq!(page.cell(0), Some(b"tx data".as_slice()));
    }

    #[test]
    fn test_transaction_rollback() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        let mut tx = Transaction::begin(1, 0);

        let mut page = tx.allocate_page(&mut pager).unwrap();
        page.insert_cell(b"will be rolled back").unwrap();
        tx.write_page(page);

        tx.rollback(&mut wal).unwrap();
        assert_eq!(tx.state(), TxState::Aborted);
        assert_eq!(tx.dirty_page_count(), 0);
    }

    #[test]
    fn test_dirty_page_read() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();

        let mut tx = Transaction::begin(1, 0);

        let mut page = tx.allocate_page(&mut pager).unwrap();
        let page_id = page.page_id();
        page.insert_cell(b"dirty data").unwrap();
        tx.write_page(page);

        // Reading from tx should return dirty page
        let read_page = tx.read_page(&mut pager, page_id).unwrap();
        assert_eq!(read_page.cell(0), Some(b"dirty data".as_slice()));
    }
}
