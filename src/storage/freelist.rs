use crate::storage::page::PageId;

/// Simple freelist tracking free pages.
/// Free page IDs are stored in-memory and serialized to a special page on checkpoint.
pub struct FreeList {
    free_pages: Vec<PageId>,
}

impl FreeList {
    pub fn new() -> Self {
        FreeList {
            free_pages: Vec::new(),
        }
    }

    /// Allocate a free page. Returns None if no free pages available.
    pub fn allocate(&mut self) -> Option<PageId> {
        self.free_pages.pop()
    }

    /// Return a page to the free list.
    pub fn free(&mut self, page_id: PageId) {
        self.free_pages.push(page_id);
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
}
