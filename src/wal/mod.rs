use crate::storage::page::PAGE_SIZE;

pub mod reader;
pub mod record;
pub mod recovery;
pub mod writer;

/// Upper bound for one encrypted WAL frame payload size.
/// PagePut with one full page is the largest record currently emitted.
pub const MAX_WAL_FRAME_LEN: usize = PAGE_SIZE + 1024;
