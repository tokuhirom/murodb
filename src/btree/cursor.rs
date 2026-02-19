/// BTreeCursor: iterate through B-tree entries in sorted order.
///
/// The cursor collects entries from the current scan position.
/// For MVP, this uses the scan method internally.
use crate::btree::ops::BTree;
use crate::error::Result;
use crate::storage::page_store::PageStore;

pub struct BTreeCursor {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    position: usize,
}

impl BTreeCursor {
    /// Create a cursor that iterates all entries.
    pub fn new(btree: &BTree, pager: &mut impl PageStore) -> Result<Self> {
        let mut entries = Vec::new();
        btree.scan(pager, |k, v| {
            entries.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })?;
        Ok(BTreeCursor {
            entries,
            position: 0,
        })
    }

    /// Create a cursor starting from a given key.
    pub fn from_key(btree: &BTree, pager: &mut impl PageStore, start_key: &[u8]) -> Result<Self> {
        let mut entries = Vec::new();
        btree.scan_from(pager, start_key, |k, v| {
            entries.push((k.to_vec(), v.to_vec()));
            Ok(true)
        })?;
        Ok(BTreeCursor {
            entries,
            position: 0,
        })
    }

    /// Get the next entry.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if self.position < self.entries.len() {
            let (ref k, ref v) = self.entries[self.position];
            self.position += 1;
            Some((k.as_slice(), v.as_slice()))
        } else {
            None
        }
    }

    /// Check if there are more entries.
    pub fn has_next(&self) -> bool {
        self.position < self.entries.len()
    }

    /// Reset to the beginning.
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Number of entries in the cursor.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::storage::pager::Pager;
    use tempfile::NamedTempFile;

    #[test]
    fn test_cursor_iteration() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);
        std::fs::remove_file(&path).ok();

        let key = MasterKey::new([0x42u8; 32]);
        let mut pager = Pager::create(&path, &key).unwrap();
        let mut btree = BTree::create(&mut pager).unwrap();

        btree.insert(&mut pager, b"c", b"3").unwrap();
        btree.insert(&mut pager, b"a", b"1").unwrap();
        btree.insert(&mut pager, b"b", b"2").unwrap();

        let mut cursor = BTreeCursor::new(&btree, &mut pager).unwrap();
        assert_eq!(cursor.len(), 3);

        let (k, v) = cursor.next().unwrap();
        assert_eq!(k, b"a");
        assert_eq!(v, b"1");

        let (k, v) = cursor.next().unwrap();
        assert_eq!(k, b"b");
        assert_eq!(v, b"2");

        let (k, v) = cursor.next().unwrap();
        assert_eq!(k, b"c");
        assert_eq!(v, b"3");

        assert!(cursor.next().is_none());

        std::fs::remove_file(&path).ok();
    }
}
