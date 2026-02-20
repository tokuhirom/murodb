use crate::storage::page::{PageId, PAGE_HEADER_SIZE, PAGE_SIZE};

/// Magic bytes at the start of each multi-page freelist page data area.
/// "FLMP" = FreeList Multi-Page. Used to reliably distinguish from the legacy
/// single-page format where the first 8 bytes are a count field.
pub const FREELIST_MULTI_PAGE_MAGIC: [u8; 4] = *b"FLMP";

/// Maximum number of freelist entries per page.
/// Data area = PAGE_SIZE - PAGE_HEADER_SIZE = 4082 bytes.
/// Per-page header = 20 bytes (magic: 4 + next_page_id: u64 + count: u64).
/// Entries = (4082 - 20) / 8 = 507.
pub const ENTRIES_PER_FREELIST_PAGE: usize = (PAGE_SIZE - PAGE_HEADER_SIZE - 20) / 8;

/// Simple freelist tracking free pages.
/// Free page IDs are stored in-memory and serialized to special page(s) on checkpoint.
#[derive(Default)]
pub struct FreeList {
    free_pages: Vec<PageId>,
}

impl FreeList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocate a free page. Returns None if no free pages available.
    pub fn allocate(&mut self) -> Option<PageId> {
        self.free_pages.pop()
    }

    /// Return a page to the free list.
    /// Panics in debug mode if the page is already free (double-free).
    /// In release mode, silently ignores the duplicate to prevent data corruption.
    pub fn free(&mut self, page_id: PageId) {
        if self.free_pages.contains(&page_id) {
            debug_assert!(
                false,
                "double-free detected: page {} is already in freelist",
                page_id
            );
            return;
        }
        self.free_pages.push(page_id);
    }

    /// Undo the most recent `free()` call. Used to speculatively compute
    /// a freelist snapshot without permanently mutating state.
    pub fn undo_last_free(&mut self) {
        self.free_pages.pop();
    }

    /// Number of free pages.
    pub fn len(&self) -> usize {
        self.free_pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.free_pages.is_empty()
    }

    /// Serialize freelist to bytes for persistence.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.free_pages.len() * 8);
        buf.extend_from_slice(&(self.free_pages.len() as u64).to_le_bytes());
        for &page_id in &self.free_pages {
            buf.extend_from_slice(&page_id.to_le_bytes());
        }
        buf
    }

    /// Number of pages needed to store this freelist in multi-page format.
    pub fn page_count_needed(&self) -> usize {
        if self.free_pages.is_empty() {
            1 // Always need at least one page for the freelist
        } else {
            self.free_pages.len().div_ceil(ENTRIES_PER_FREELIST_PAGE)
        }
    }

    /// Serialize freelist into multiple page data buffers (multi-page chain format).
    ///
    /// Each page's data area (after PAGE_HEADER_SIZE) contains:
    ///   [next_freelist_page_id: u64] [count_in_this_page: u64] [page_id entries: u64...]
    ///
    /// `page_ids` provides the allocated page IDs for each page in the chain.
    /// Returns Vec of (page_id, page_data_bytes) pairs.
    pub fn serialize_pages(&self, page_ids: &[PageId]) -> Vec<(PageId, Vec<u8>)> {
        let chunks: Vec<&[PageId]> = if self.free_pages.is_empty() {
            vec![&[]]
        } else {
            self.free_pages.chunks(ENTRIES_PER_FREELIST_PAGE).collect()
        };
        assert_eq!(
            chunks.len(),
            page_ids.len(),
            "must provide exactly enough page IDs"
        );

        let mut result = Vec::with_capacity(chunks.len());
        for (i, chunk) in chunks.iter().enumerate() {
            let next_page_id: u64 = if i + 1 < page_ids.len() {
                page_ids[i + 1]
            } else {
                0 // terminal
            };
            // Build data area content: [magic: 4][next_page_id: 8][count: 8][entries...]
            let mut data = Vec::with_capacity(20 + chunk.len() * 8);
            data.extend_from_slice(&FREELIST_MULTI_PAGE_MAGIC);
            data.extend_from_slice(&next_page_id.to_le_bytes());
            data.extend_from_slice(&(chunk.len() as u64).to_le_bytes());
            for &pid in *chunk {
                data.extend_from_slice(&pid.to_le_bytes());
            }
            result.push((page_ids[i], data));
        }
        result
    }

    /// Deserialize freelist from multiple page data buffers (multi-page chain format).
    ///
    /// Each `data` slice is the data area content (after PAGE_HEADER_SIZE) of a freelist page.
    /// Format per page: [magic: 4][next_page_id: 8][count: 8][entries: 8*N]
    pub fn deserialize_pages(pages_data: &[&[u8]]) -> Self {
        let mut free_pages = Vec::new();
        for data in pages_data {
            if data.len() < 20 {
                continue;
            }
            // Skip magic (4 bytes) + next_page_id (8 bytes), read count
            let count = u64::from_le_bytes(data[12..20].try_into().unwrap()) as usize;
            for i in 0..count {
                let offset = 20 + i * 8;
                if offset + 8 > data.len() {
                    break;
                }
                let page_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                free_pages.push(page_id);
            }
        }
        FreeList { free_pages }
    }

    /// Detect whether a data area uses the multi-page chain format.
    /// Multi-page format starts with the 4-byte magic "FLMP", while legacy format
    /// starts with a u64 count field directly. This is a reliable check regardless
    /// of the data area size (which is always page-sized, zero-padded).
    pub fn is_multi_page_format(data: &[u8]) -> bool {
        data.len() >= 4 && data[0..4] == FREELIST_MULTI_PAGE_MAGIC
    }

    /// Validate that all freelist entries are within the given page_count.
    pub fn validate(&self, page_count: u64) -> std::result::Result<(), String> {
        for &pid in &self.free_pages {
            if pid >= page_count {
                return Err(format!(
                    "freelist entry {} is beyond page_count {}",
                    pid, page_count
                ));
            }
        }
        // Check for duplicates
        let mut seen = std::collections::HashSet::new();
        for &pid in &self.free_pages {
            if !seen.insert(pid) {
                return Err(format!("duplicate freelist entry: page {}", pid));
            }
        }
        Ok(())
    }

    /// Sanitize freelist by removing out-of-range and duplicate entries.
    /// After crash recovery, the freelist may contain stale entries.
    pub fn sanitize(&mut self, page_count: u64) {
        let mut seen = std::collections::HashSet::new();
        self.free_pages
            .retain(|&pid| pid < page_count && seen.insert(pid));
    }

    /// Deserialize freelist from bytes.
    pub fn deserialize(data: &[u8]) -> Self {
        if data.len() < 8 {
            return FreeList::new();
        }
        let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut free_pages = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 8 + i * 8;
            if offset + 8 > data.len() {
                break;
            }
            let page_id = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            free_pages.push(page_id);
        }
        FreeList { free_pages }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_and_free() {
        let mut fl = FreeList::new();
        assert!(fl.allocate().is_none());

        fl.free(10);
        fl.free(20);
        assert_eq!(fl.len(), 2);

        assert_eq!(fl.allocate(), Some(20));
        assert_eq!(fl.allocate(), Some(10));
        assert!(fl.allocate().is_none());
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut fl = FreeList::new();
        fl.free(5);
        fl.free(10);
        fl.free(15);

        let data = fl.serialize();
        let fl2 = FreeList::deserialize(&data);
        assert_eq!(fl2.len(), 3);
    }

    #[test]
    fn test_serialize_pages_single() {
        let mut fl = FreeList::new();
        fl.free(100);
        fl.free(200);

        let page_ids = vec![42];
        let pages = fl.serialize_pages(&page_ids);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].0, 42);

        // Deserialize back
        let refs: Vec<&[u8]> = pages.iter().map(|(_, d)| d.as_slice()).collect();
        let fl2 = FreeList::deserialize_pages(&refs);
        assert_eq!(fl2.len(), 2);
    }

    #[test]
    fn test_serialize_pages_multi() {
        let mut fl = FreeList::new();
        // Fill more than one page
        for i in 0..(ENTRIES_PER_FREELIST_PAGE + 5) {
            fl.free(i as u64 + 1000);
        }

        assert_eq!(fl.page_count_needed(), 2);

        let page_ids = vec![10, 11];
        let pages = fl.serialize_pages(&page_ids);
        assert_eq!(pages.len(), 2);

        // Both pages should start with magic
        assert_eq!(&pages[0].1[0..4], &FREELIST_MULTI_PAGE_MAGIC);
        assert_eq!(&pages[1].1[0..4], &FREELIST_MULTI_PAGE_MAGIC);
        // First page should have next_page_id = 11 (after 4-byte magic)
        let next_ptr = u64::from_le_bytes(pages[0].1[4..12].try_into().unwrap());
        assert_eq!(next_ptr, 11);
        // Last page should have next_page_id = 0
        let last_next = u64::from_le_bytes(pages[1].1[4..12].try_into().unwrap());
        assert_eq!(last_next, 0);

        // Roundtrip
        let refs: Vec<&[u8]> = pages.iter().map(|(_, d)| d.as_slice()).collect();
        let fl2 = FreeList::deserialize_pages(&refs);
        assert_eq!(fl2.len(), ENTRIES_PER_FREELIST_PAGE + 5);
    }

    #[test]
    fn test_is_multi_page_format_detection() {
        // Legacy format: [count=2][page1][page2]
        let mut fl = FreeList::new();
        fl.free(100);
        fl.free(200);
        let legacy = fl.serialize();
        assert!(!FreeList::is_multi_page_format(&legacy));

        // Multi-page format: [magic][next=0][count=2][page1][page2]
        let pages = fl.serialize_pages(&[42]);
        assert!(FreeList::is_multi_page_format(&pages[0].1));

        // Zero-padded page-sized data with legacy format should NOT be detected as multi-page
        let mut padded = vec![0u8; 4082]; // typical data area size
        let legacy_data = fl.serialize();
        padded[..legacy_data.len()].copy_from_slice(&legacy_data);
        assert!(!FreeList::is_multi_page_format(&padded));
    }
}
