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
    freed_pages: Vec<PageId>,
}

impl Transaction {
    pub fn begin(txid: TxId, snapshot_lsn: Lsn) -> Self {
        Transaction {
            txid,
            state: TxState::Active,
            snapshot_lsn,
            dirty_pages: HashMap::new(),
            freed_pages: Vec::new(),
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

    /// Record a page as freed within this transaction.
    /// The page will be added to the pager freelist on commit, or discarded on rollback.
    pub fn free_page(&mut self, page_id: PageId) {
        self.freed_pages.push(page_id);
    }

    /// Allocate a new page through the pager.
    pub fn allocate_page(&mut self, pager: &mut Pager) -> Result<Page> {
        let page = pager.allocate_page()?;
        Ok(page)
    }

    /// Commit: write dirty pages to WAL, then flush to pager.
    ///
    /// `catalog_root` is included in the WAL MetaUpdate record so that recovery
    /// can restore it atomically with the committed pages.
    pub fn commit(
        &mut self,
        pager: &mut Pager,
        wal: &mut WalWriter,
        catalog_root: u64,
    ) -> Result<Lsn> {
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

        // Compute page_count: max of current pager page_count and any dirty page ids + 1
        let mut page_count = pager.page_count();
        for &page_id in self.dirty_pages.keys() {
            let needed = page_id + 1;
            if needed > page_count {
                page_count = needed;
            }
        }

        // Build a speculative freelist snapshot without mutating the pager's freelist.
        // This avoids leaking freed pages into the pager if WAL commit fails below.
        // We track how many pages were actually added (free() returns false for
        // duplicates) so we undo exactly the right number of pushes.
        use crate::storage::page::PAGE_HEADER_SIZE;
        let pages_needed = {
            let fl = pager.freelist_mut();
            let mut actually_freed = 0usize;
            for &page_id in &self.freed_pages {
                if fl.free(page_id) {
                    actually_freed += 1;
                }
            }
            let needed = fl.page_count_needed();
            for _ in 0..actually_freed {
                fl.undo_last_free();
            }
            needed
        };

        // Compute freelist page IDs without mutating pager state.
        // Reuse existing freelist page as the first one, then assign sequential
        // page IDs starting from page_count for any additional pages needed.
        let mut fl_page_ids = Vec::with_capacity(pages_needed);
        let existing_fl_page_id = pager.freelist_page_id();
        if existing_fl_page_id != 0 {
            fl_page_ids.push(existing_fl_page_id);
        }
        while fl_page_ids.len() < pages_needed {
            fl_page_ids.push(page_count);
            page_count += 1;
        }

        // Build speculative freelist and serialize to pages
        let fl_pages_data = {
            let fl = pager.freelist_mut();
            let mut actually_freed = 0usize;
            for &page_id in &self.freed_pages {
                if fl.free(page_id) {
                    actually_freed += 1;
                }
            }
            let data = fl.serialize_pages(&fl_page_ids);
            for _ in 0..actually_freed {
                fl.undo_last_free();
            }
            data
        };

        // Write freelist pages to WAL
        let mut fl_disk_pages = Vec::with_capacity(fl_pages_data.len());
        for (pid, data_area) in &fl_pages_data {
            let mut fl_page = Page::new(*pid);
            fl_page.data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + data_area.len()]
                .copy_from_slice(data_area);
            wal.append(&WalRecord::PagePut {
                txid: self.txid,
                page_id: *pid,
                data: fl_page.data.to_vec(),
            })?;
            fl_disk_pages.push(fl_page);
        }

        // The first page in the chain is the freelist root
        let freelist_page_id = fl_page_ids[0];

        // Write MetaUpdate so recovery can restore catalog_root, page_count, and freelist_page_id
        wal.append(&WalRecord::MetaUpdate {
            txid: self.txid,
            catalog_root,
            page_count,
            freelist_page_id,
        })?;

        // Write Commit record
        let commit_lsn = wal.current_lsn();
        wal.append(&WalRecord::Commit {
            txid: self.txid,
            lsn: commit_lsn,
        })?;

        // Fsync the WAL — this is the commit point.
        // Only after this succeeds do we apply freed pages to the in-memory freelist.
        wal.sync()?;

        // WAL commit succeeded: now apply freed pages to the pager's freelist
        for &page_id in &self.freed_pages {
            pager.freelist_mut().free(page_id);
        }

        // Post-sync data flush — errors become CommitInDoubt since WAL is durable
        let flush_result: Result<()> = (|| {
            for page in self.dirty_pages.values() {
                pager.write_page(page)?;
            }
            for fl_page in &fl_disk_pages {
                pager.write_page(fl_page)?;
            }
            pager.set_catalog_root(catalog_root);
            pager.set_page_count(page_count);
            pager.set_freelist_page_id(freelist_page_id);
            pager.flush_meta()?;
            Ok(())
        })();

        if let Err(e) = flush_result {
            self.state = TxState::Committed; // WAL is durable
            self.dirty_pages.clear();
            self.freed_pages.clear();
            return Err(MuroError::CommitInDoubt(format!("{}", e)));
        }

        self.state = TxState::Committed;
        self.dirty_pages.clear();
        self.freed_pages.clear();

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
        self.freed_pages.clear();
        self.state = TxState::Aborted;
        Ok(())
    }

    /// Number of dirty pages.
    pub fn dirty_page_count(&self) -> usize {
        self.dirty_pages.len()
    }

    /// Iterator over dirty pages (for WAL-less commit).
    pub fn dirty_pages(&self) -> impl Iterator<Item = &Page> {
        self.dirty_pages.values()
    }

    /// Rollback without WAL: just discard dirty pages.
    pub(crate) fn rollback_no_wal(&mut self) {
        self.dirty_pages.clear();
        self.freed_pages.clear();
        self.state = TxState::Aborted;
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
        let lsn = tx.commit(&mut pager, &mut wal, 0).unwrap();
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

    /// Regression test: freelist must not be mutated if WAL commit fails.
    /// Before the fix, `pager.freelist_mut().free()` was called before WAL sync,
    /// so a WAL failure would leave freed pages in the pager's in-memory freelist.
    #[test]
    fn test_freelist_not_leaked_on_wal_failure() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // First transaction: allocate pages and commit successfully
        let mut tx1 = Transaction::begin(1, 0);
        let page_a = tx1.allocate_page(&mut pager).unwrap();
        let page_b = tx1.allocate_page(&mut pager).unwrap();
        let page_a_id = page_a.page_id();
        let page_b_id = page_b.page_id();
        tx1.write_page(page_a);
        tx1.write_page(page_b);
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        let freelist_len_before = pager.freelist_mut().len();

        // Second transaction: free a page then attempt commit with injected write failure
        let mut tx2 = Transaction::begin(2, wal.current_lsn());
        tx2.free_page(page_a_id);
        let mut page_b_dirty = pager.read_page(page_b_id).unwrap();
        page_b_dirty.insert_cell(b"modified").unwrap();
        tx2.write_page(page_b_dirty);

        // Inject a write failure so wal.append() fails mid-commit
        wal.set_inject_write_failure(Some(std::io::ErrorKind::Other));
        let result = tx2.commit(&mut pager, &mut wal, 0);
        assert!(result.is_err(), "commit must fail when WAL write fails");

        // Freelist must be unchanged — the freed page must NOT have leaked
        assert_eq!(
            pager.freelist_mut().len(),
            freelist_len_before,
            "freelist must not change when WAL commit fails"
        );

        // Clear the injection and verify a subsequent commit works correctly
        wal.set_inject_write_failure(None);
        let mut tx3 = Transaction::begin(3, wal.current_lsn());
        tx3.free_page(page_a_id);
        let page_b_read = pager.read_page(page_b_id).unwrap();
        tx3.write_page(page_b_read);
        tx3.commit(&mut pager, &mut wal, 0).unwrap();
        assert_eq!(
            pager.freelist_mut().len(),
            freelist_len_before + 1,
            "freelist should grow by 1 after successful commit with free_page"
        );
    }

    /// Test that freelist is not leaked when WAL sync (fsync) fails.
    #[test]
    fn test_freelist_not_leaked_on_wal_sync_failure() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // First transaction: allocate and commit
        let mut tx1 = Transaction::begin(1, 0);
        let page_a = tx1.allocate_page(&mut pager).unwrap();
        let page_a_id = page_a.page_id();
        tx1.write_page(page_a);
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        let freelist_len_before = pager.freelist_mut().len();

        // Second transaction: free a page, inject sync failure
        let mut tx2 = Transaction::begin(2, wal.current_lsn());
        tx2.free_page(page_a_id);
        let page_a_read = pager.read_page(page_a_id).unwrap();
        tx2.write_page(page_a_read);

        wal.set_inject_sync_failure(Some(std::io::ErrorKind::Other));
        let result = tx2.commit(&mut pager, &mut wal, 0);
        assert!(result.is_err(), "commit must fail when WAL sync fails");

        assert_eq!(
            pager.freelist_mut().len(),
            freelist_len_before,
            "freelist must not change when WAL sync fails"
        );

        wal.set_inject_sync_failure(None);
    }

    /// Test that large freelists spanning multiple pages work correctly.
    #[test]
    fn test_large_freelist_multi_page() {
        use crate::storage::freelist::ENTRIES_PER_FREELIST_PAGE;

        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut wal = WalWriter::create(&wal_path, &test_key()).unwrap();

        // First, allocate many pages
        let num_pages = ENTRIES_PER_FREELIST_PAGE + 10; // More than fits in one page
        let mut tx1 = Transaction::begin(1, 0);
        let mut page_ids = Vec::new();
        for _ in 0..num_pages {
            let page = tx1.allocate_page(&mut pager).unwrap();
            let pid = page.page_id();
            tx1.write_page(page);
            page_ids.push(pid);
        }
        tx1.commit(&mut pager, &mut wal, 0).unwrap();

        // Free all those pages in a second transaction
        let mut tx2 = Transaction::begin(2, wal.current_lsn());
        for &pid in &page_ids {
            tx2.free_page(pid);
        }
        // Need at least one dirty page for the commit
        let dummy_page = tx2.allocate_page(&mut pager).unwrap();
        tx2.write_page(dummy_page);
        tx2.commit(&mut pager, &mut wal, 0).unwrap();

        // Verify freelist size is correct
        assert!(
            pager.freelist_mut().len() >= num_pages,
            "freelist should contain at least {} entries, got {}",
            num_pages,
            pager.freelist_mut().len()
        );
    }
}
