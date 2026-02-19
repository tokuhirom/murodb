/// Slotted page implementation (4096 bytes).
///
/// Layout:
///   [PageHeader (12 bytes)] [Cell Pointer Array ...] [Free Space ...] [Cell Data ...]
///
/// PageHeader:
///   page_id:       u64 (8 bytes)
///   cell_count:    u16 (2 bytes)
///   free_start:    u16 (offset where cell pointer array ends / free space begins)
///   free_end:      u16 (offset where cell data begins, grows downward)
///
/// Cell Pointer: u16 (offset to cell data within page)
/// Cell Data: [u16 len][payload bytes]

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 14;
pub const CELL_POINTER_SIZE: usize = 2;
pub const CELL_HEADER_SIZE: usize = 2; // u16 length prefix

pub type PageId = u64;

#[derive(Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        let mut page = Page {
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_cell_count(0);
        page.set_free_start(PAGE_HEADER_SIZE as u16);
        page.set_free_end(PAGE_SIZE as u16);
        page
    }

    // --- Header accessors ---

    pub fn page_id(&self) -> PageId {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }

    pub fn set_page_id(&mut self, id: PageId) {
        self.data[0..8].copy_from_slice(&id.to_le_bytes());
    }

    pub fn cell_count(&self) -> u16 {
        u16::from_le_bytes(self.data[8..10].try_into().unwrap())
    }

    pub fn set_cell_count(&mut self, count: u16) {
        self.data[8..10].copy_from_slice(&count.to_le_bytes());
    }

    pub fn free_start(&self) -> u16 {
        u16::from_le_bytes(self.data[10..12].try_into().unwrap())
    }

    pub fn set_free_start(&mut self, offset: u16) {
        self.data[10..12].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn free_end(&self) -> u16 {
        u16::from_le_bytes(self.data[12..14].try_into().unwrap())
    }

    pub fn set_free_end(&mut self, offset: u16) {
        self.data[12..14].copy_from_slice(&offset.to_le_bytes());
    }

    /// Available free space for new cells.
    pub fn free_space(&self) -> usize {
        let start = self.free_start() as usize;
        let end = self.free_end() as usize;
        if end > start + CELL_POINTER_SIZE + CELL_HEADER_SIZE {
            end - start - CELL_POINTER_SIZE - CELL_HEADER_SIZE
        } else {
            0
        }
    }

    /// Insert a cell payload into the page. Returns the cell index.
    pub fn insert_cell(&mut self, payload: &[u8]) -> crate::error::Result<u16> {
        let total_cell_size = CELL_HEADER_SIZE + payload.len();
        let needed = CELL_POINTER_SIZE + total_cell_size;

        let free_start = self.free_start() as usize;
        let free_end = self.free_end() as usize;

        if free_end < free_start + needed {
            return Err(crate::error::MuroError::PageOverflow);
        }

        // Write cell data at the end (growing downward)
        let cell_offset = free_end - total_cell_size;
        let len = payload.len() as u16;
        self.data[cell_offset..cell_offset + 2].copy_from_slice(&len.to_le_bytes());
        self.data[cell_offset + 2..cell_offset + 2 + payload.len()]
            .copy_from_slice(payload);

        // Write cell pointer
        let cell_idx = self.cell_count();
        let ptr_offset = free_start;
        self.data[ptr_offset..ptr_offset + 2]
            .copy_from_slice(&(cell_offset as u16).to_le_bytes());

        self.set_cell_count(cell_idx + 1);
        self.set_free_start((free_start + CELL_POINTER_SIZE) as u16);
        self.set_free_end(cell_offset as u16);

        Ok(cell_idx)
    }

    /// Get cell payload by index.
    pub fn cell(&self, index: u16) -> Option<&[u8]> {
        if index >= self.cell_count() {
            return None;
        }
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * CELL_POINTER_SIZE;
        let cell_offset =
            u16::from_le_bytes(self.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(
            self.data[cell_offset..cell_offset + 2].try_into().unwrap(),
        ) as usize;
        Some(&self.data[cell_offset + 2..cell_offset + 2 + len])
    }

    /// Get a mutable reference to cell payload by index.
    pub fn cell_offset_and_len(&self, index: u16) -> Option<(usize, usize)> {
        if index >= self.cell_count() {
            return None;
        }
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * CELL_POINTER_SIZE;
        let cell_offset =
            u16::from_le_bytes(self.data[ptr_offset..ptr_offset + 2].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(
            self.data[cell_offset..cell_offset + 2].try_into().unwrap(),
        ) as usize;
        Some((cell_offset + 2, len))
    }

    /// Remove cell at the given index by swapping pointers with the last cell.
    /// Note: This does NOT reclaim the cell data space (would need compaction).
    pub fn remove_cell(&mut self, index: u16) {
        let count = self.cell_count();
        if index >= count {
            return;
        }

        // Shift cell pointers left to fill the gap
        let start = PAGE_HEADER_SIZE + (index as usize) * CELL_POINTER_SIZE;
        let end = PAGE_HEADER_SIZE + (count as usize) * CELL_POINTER_SIZE;
        if start + CELL_POINTER_SIZE < end {
            self.data.copy_within(start + CELL_POINTER_SIZE..end, start);
        }

        self.set_cell_count(count - 1);
        self.set_free_start(self.free_start() - CELL_POINTER_SIZE as u16);
    }

    /// Set cell pointer at index to point to a specific offset.
    pub fn set_cell_pointer(&mut self, index: u16, offset: u16) {
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * CELL_POINTER_SIZE;
        self.data[ptr_offset..ptr_offset + 2].copy_from_slice(&offset.to_le_bytes());
    }

    /// Get the raw cell pointer (offset) for a given index.
    pub fn cell_pointer(&self, index: u16) -> Option<u16> {
        if index >= self.cell_count() {
            return None;
        }
        let ptr_offset = PAGE_HEADER_SIZE + (index as usize) * CELL_POINTER_SIZE;
        Some(u16::from_le_bytes(
            self.data[ptr_offset..ptr_offset + 2].try_into().unwrap(),
        ))
    }

    /// Get the raw page bytes.
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    /// Create a page from raw bytes.
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        Page { data }
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("page_id", &self.page_id())
            .field("cell_count", &self.cell_count())
            .field("free_start", &self.free_start())
            .field("free_end", &self.free_end())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(42);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.cell_count(), 0);
        assert_eq!(page.free_start(), PAGE_HEADER_SIZE as u16);
        assert_eq!(page.free_end(), PAGE_SIZE as u16);
    }

    #[test]
    fn test_insert_and_read_cells() {
        let mut page = Page::new(1);
        let data1 = b"hello world";
        let data2 = b"foo bar baz";

        let idx0 = page.insert_cell(data1).unwrap();
        let idx1 = page.insert_cell(data2).unwrap();

        assert_eq!(idx0, 0);
        assert_eq!(idx1, 1);
        assert_eq!(page.cell_count(), 2);
        assert_eq!(page.cell(0), Some(data1.as_slice()));
        assert_eq!(page.cell(1), Some(data2.as_slice()));
    }

    #[test]
    fn test_remove_cell() {
        let mut page = Page::new(1);
        page.insert_cell(b"aaa").unwrap();
        page.insert_cell(b"bbb").unwrap();
        page.insert_cell(b"ccc").unwrap();

        page.remove_cell(1); // remove "bbb"
        assert_eq!(page.cell_count(), 2);
        assert_eq!(page.cell(0), Some(b"aaa".as_slice()));
        assert_eq!(page.cell(1), Some(b"ccc".as_slice()));
    }

    #[test]
    fn test_page_overflow() {
        let mut page = Page::new(1);
        let big_data = vec![0xFFu8; PAGE_SIZE]; // too large
        assert!(page.insert_cell(&big_data).is_err());
    }

    #[test]
    fn test_fill_page() {
        let mut page = Page::new(1);
        let cell_data = vec![0u8; 32];
        let mut count = 0u16;
        loop {
            match page.insert_cell(&cell_data) {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        assert!(count > 50); // should fit many 32-byte cells
        assert_eq!(page.cell_count(), count);
    }
}
