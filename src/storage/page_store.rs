use crate::error::Result;
use crate::storage::page::{Page, PageId};

/// Abstraction over page I/O.
///
/// Implemented by `Pager` (direct disk I/O) and `TxPageStore` (transaction dirty-buffer).
pub trait PageStore {
    fn read_page(&mut self, page_id: PageId) -> Result<Page>;
    fn write_page(&mut self, page: &Page) -> Result<()>;
    fn allocate_page(&mut self) -> Result<Page>;
    fn free_page(&mut self, page_id: PageId);
}
