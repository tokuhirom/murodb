/// B-tree node layout on slotted pages.
///
/// Each page is either a Leaf or Internal node.
/// The node type is stored in the first byte of a special "node header" cell (cell 0).
///
/// Node header cell (cell 0):
///   [node_type: u8] [right_child: u64 (internal only)]
///
/// Leaf cell layout:
///   [key_len: u16] [key bytes] [value bytes]
///
/// Internal cell layout:
///   [left_child: u64] [key_len: u16] [key bytes]
///
/// For internal nodes, the right-most child pointer is stored in the node header.
use crate::storage::page::{Page, PageId};

const NODE_TYPE_LEAF: u8 = 1;
const NODE_TYPE_INTERNAL: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Leaf,
    Internal,
}

/// Initialize a page as a B-tree leaf node.
/// Panics only if the page has insufficient space for a 1-byte header,
/// which cannot happen with the current PAGE_SIZE (4096).
pub fn init_leaf(page: &mut Page) {
    let header = [NODE_TYPE_LEAF];
    page.insert_cell(&header)
        .expect("BUG: page too small for leaf header");
}

/// Initialize a page as a B-tree internal node with a rightmost child.
/// Panics only if the page has insufficient space for a 9-byte header.
pub fn init_internal(page: &mut Page, right_child: PageId) {
    let mut header = [0u8; 9];
    header[0] = NODE_TYPE_INTERNAL;
    header[1..9].copy_from_slice(&right_child.to_le_bytes());
    page.insert_cell(&header)
        .expect("BUG: page too small for internal header");
}

/// Get the node type from a page.
pub fn node_type(page: &Page) -> Option<NodeType> {
    let header = page.cell(0)?;
    match header[0] {
        NODE_TYPE_LEAF => Some(NodeType::Leaf),
        NODE_TYPE_INTERNAL => Some(NodeType::Internal),
        _ => None,
    }
}

/// Get the right child pointer (internal nodes only).
pub fn right_child(page: &Page) -> Option<PageId> {
    let header = page.cell(0)?;
    if header[0] != NODE_TYPE_INTERNAL || header.len() < 9 {
        return None;
    }
    Some(u64::from_le_bytes(header[1..9].try_into().unwrap()))
}

/// Set the right child pointer (internal nodes only).
pub fn set_right_child(page: &mut Page, child: PageId) {
    // We need to rebuild the header cell. Since cell 0 can't be mutated in place easily,
    // we'll modify the page data directly at the cell offset.
    if let Some((offset, _len)) = page.cell_offset_and_len(0) {
        page.data[offset] = NODE_TYPE_INTERNAL;
        page.data[offset + 1..offset + 9].copy_from_slice(&child.to_le_bytes());
    }
}

/// Number of key-value entries (excluding the header cell at index 0).
pub fn num_entries(page: &Page) -> u16 {
    let count = page.cell_count();
    if count == 0 {
        0
    } else {
        count - 1
    }
}

// --- Leaf node operations ---

/// Encode a leaf cell: [key_len: u16][key][value]
pub fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut buf = Vec::with_capacity(2 + key.len() + value.len());
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

/// Decode a leaf cell into (key, value).
pub fn decode_leaf_cell(cell: &[u8]) -> (&[u8], &[u8]) {
    let key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap()) as usize;
    let key = &cell[2..2 + key_len];
    let value = &cell[2 + key_len..];
    (key, value)
}

/// Get the key of the i-th entry in a leaf node (0-based, entries start at cell index 1).
pub fn leaf_key(page: &Page, entry_idx: u16) -> Option<&[u8]> {
    let cell = page.cell(entry_idx + 1)?;
    let (key, _) = decode_leaf_cell(cell);
    Some(key)
}

/// Get the value of the i-th entry in a leaf node.
pub fn leaf_value(page: &Page, entry_idx: u16) -> Option<&[u8]> {
    let cell = page.cell(entry_idx + 1)?;
    let (_, value) = decode_leaf_cell(cell);
    Some(value)
}

/// Get key and value of the i-th entry in a leaf node.
pub fn leaf_entry(page: &Page, entry_idx: u16) -> Option<(&[u8], &[u8])> {
    let cell = page.cell(entry_idx + 1)?;
    Some(decode_leaf_cell(cell))
}

// --- Internal node operations ---

/// Encode an internal cell: [left_child: u64][key_len: u16][key]
pub fn encode_internal_cell(left_child: PageId, key: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut buf = Vec::with_capacity(8 + 2 + key.len());
    buf.extend_from_slice(&left_child.to_le_bytes());
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf
}

/// Decode an internal cell into (left_child, key).
pub fn decode_internal_cell(cell: &[u8]) -> (PageId, &[u8]) {
    let left_child = u64::from_le_bytes(cell[0..8].try_into().unwrap());
    let key_len = u16::from_le_bytes(cell[8..10].try_into().unwrap()) as usize;
    let key = &cell[10..10 + key_len];
    (left_child, key)
}

/// Get the key of the i-th entry in an internal node.
pub fn internal_key(page: &Page, entry_idx: u16) -> Option<&[u8]> {
    let cell = page.cell(entry_idx + 1)?;
    let (_, key) = decode_internal_cell(cell);
    Some(key)
}

/// Get the left child of the i-th entry in an internal node.
pub fn internal_left_child(page: &Page, entry_idx: u16) -> Option<PageId> {
    let cell = page.cell(entry_idx + 1)?;
    let (left_child, _) = decode_internal_cell(cell);
    Some(left_child)
}

/// Find the child page to follow for a given key in an internal node.
/// Returns the child page_id.
pub fn find_child(page: &Page, key: &[u8]) -> Option<PageId> {
    let n = num_entries(page);
    for i in 0..n {
        let entry_key = internal_key(page, i)?;
        if key < entry_key {
            return internal_left_child(page, i);
        }
    }
    // Key >= all entries, go to right child
    right_child(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node() {
        let mut page = Page::new(1);
        init_leaf(&mut page);

        assert_eq!(node_type(&page), Some(NodeType::Leaf));
        assert_eq!(num_entries(&page), 0);

        let cell = encode_leaf_cell(b"key1", b"value1");
        page.insert_cell(&cell).unwrap();
        assert_eq!(num_entries(&page), 1);
        assert_eq!(leaf_key(&page, 0), Some(b"key1".as_slice()));
        assert_eq!(leaf_value(&page, 0), Some(b"value1".as_slice()));
    }

    #[test]
    fn test_internal_node() {
        let mut page = Page::new(2);
        init_internal(&mut page, 100);

        assert_eq!(node_type(&page), Some(NodeType::Internal));
        assert_eq!(right_child(&page), Some(100));

        let cell = encode_internal_cell(10, b"midkey");
        page.insert_cell(&cell).unwrap();

        assert_eq!(num_entries(&page), 1);
        assert_eq!(internal_key(&page, 0), Some(b"midkey".as_slice()));
        assert_eq!(internal_left_child(&page, 0), Some(10));
    }

    #[test]
    fn test_find_child() {
        let mut page = Page::new(3);
        init_internal(&mut page, 99); // right child

        // Add entries: left_child=10, key="m"; left_child=20, key="t"
        page.insert_cell(&encode_internal_cell(10, b"m")).unwrap();
        page.insert_cell(&encode_internal_cell(20, b"t")).unwrap();

        // key < "m" -> left child of first entry (10)
        assert_eq!(find_child(&page, b"a"), Some(10));
        // "m" <= key < "t" -> left child of second entry (20)
        assert_eq!(find_child(&page, b"m"), Some(20));
        assert_eq!(find_child(&page, b"s"), Some(20));
        // key >= "t" -> right child (99)
        assert_eq!(find_child(&page, b"t"), Some(99));
        assert_eq!(find_child(&page, b"z"), Some(99));
    }
}
