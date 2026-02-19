use crate::error::Result;
use crate::storage::page::{Page, PageId};
use crate::storage::page_store::PageStore;
use crate::storage::pager::Pager;
use crate::tx::transaction::Transaction;

/// A `PageStore` backed by a `Transaction` dirty-page buffer.
///
/// Reads check the dirty buffer first, writes go to the dirty buffer,
/// and allocations go through the pager but are tracked.
pub struct TxPageStore<'a> {
    tx: Transaction,
    pager: &'a mut Pager,
}

impl<'a> TxPageStore<'a> {
    pub fn new(tx: Transaction, pager: &'a mut Pager) -> Self {
        TxPageStore { tx, pager }
    }

    /// Consume this store and return the `Transaction` (for put-back into Session).
    pub fn into_tx(self) -> Transaction {
        self.tx
    }
}

impl PageStore for TxPageStore<'_> {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        self.tx.read_page(self.pager, page_id)
    }

    fn write_page(&mut self, page: &Page) -> Result<()> {
        self.tx.write_page(page.clone());
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<Page> {
        self.tx.allocate_page(self.pager)
    }

    fn free_page(&mut self, page_id: PageId) {
        self.pager.free_page(page_id);
    }
}
