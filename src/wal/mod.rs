use crate::storage::page::PAGE_SIZE;

pub mod reader;
pub mod record;
pub mod recovery;
pub mod writer;

/// Upper bound for one encrypted WAL frame payload size.
/// PagePut with one full page is the largest record currently emitted.
pub const MAX_WAL_FRAME_LEN: usize = PAGE_SIZE + 1024;

/// WAL file magic bytes: "MUROWAL1" (8 bytes).
pub const WAL_MAGIC: &[u8; 8] = b"MUROWAL1";

/// WAL header size: magic (8) + version (4) = 12 bytes.
pub const WAL_HEADER_SIZE: usize = 12;

/// WAL format version.
pub const WAL_VERSION: u32 = 1;
