/// B-tree node layout on slotted pages.
///
/// Each page is either a Leaf or Internal node.
/// The node type is stored in the first byte of a special "node header" cell (cell 0).
///
/// Node header cell (cell 0):
///   [node_type: u8] [right_child: u64 (internal only)]
///
/// Leaf cell layout (normal):
///   [key_len: u16] [key bytes] [value bytes]
///
/// Leaf cell layout (overflow, key_len high bit set):
///   [key_len|0x8000: u16] [key bytes] [total_value_len: u32] [first_overflow_page: u64]
///   All value data is stored in overflow pages; no inline prefix.
///
/// Internal cell layout:
///   [left_child: u64] [key_len: u16] [key bytes]
///
/// For internal nodes, the right-most child pointer is stored in the node header.
use crate::storage::page::{
    Page, PageId, CELL_HEADER_SIZE, CELL_POINTER_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE,
};

const NODE_TYPE_LEAF: u8 = 1;
const NODE_TYPE_INTERNAL: u8 = 2;

/// High bit of key_len signals an overflow cell.
pub const OVERFLOW_FLAG: u16 = 0x8000;

/// Overhead of overflow metadata in a leaf cell: total_value_len(4) + first_overflow_page(8) = 12.
const OVERFLOW_META_SIZE: usize = 4 + 8;

/// Maximum cell payload that fits in a fresh leaf page (with header cell already inserted).
/// = PAGE_SIZE - PAGE_HEADER_SIZE - (header cell: pointer + header + 1 byte payload) - (this cell: pointer + header)
/// = 4096 - 14 - (2 + 2 + 1) - (2 + 2) = 4073
const MAX_LEAF_CELL_PAYLOAD: usize = PAGE_SIZE
    - PAGE_HEADER_SIZE
    - (CELL_POINTER_SIZE + CELL_HEADER_SIZE + 1)
    - (CELL_POINTER_SIZE + CELL_HEADER_SIZE);

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

/// Check whether a key+value pair requires overflow storage.
pub fn needs_overflow(key: &[u8], value: &[u8]) -> bool {
    // Normal cell payload: 2 (key_len) + key + value
    let normal_size = 2 + key.len() + value.len();
    normal_size > MAX_LEAF_CELL_PAYLOAD
}

/// Encode a leaf cell: [key_len: u16][key][value]
pub fn encode_leaf_cell(key: &[u8], value: &[u8]) -> Vec<u8> {
    let key_len = key.len() as u16;
    let mut buf = Vec::with_capacity(2 + key.len() + value.len());
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

/// Encode an overflow leaf cell. Returns cell_bytes.
///
/// Cell layout: [key_len|0x8000: u16][key][total_value_len: u32][first_overflow_page: u64]
/// The first_overflow_page is set to a placeholder; call `set_overflow_page_id` after writing
/// the overflow chain.
///
/// All value data goes to overflow pages (no inline prefix).
pub fn encode_overflow_leaf_cell(key: &[u8], total_value_len: u32) -> Vec<u8> {
    let key_len_with_flag = (key.len() as u16) | OVERFLOW_FLAG;

    // Cell: 2 (key_len) + key + 4 (total_value_len) + 8 (first_overflow_page)
    let cell_size = 2 + key.len() + OVERFLOW_META_SIZE;
    let mut cell = Vec::with_capacity(cell_size);
    cell.extend_from_slice(&key_len_with_flag.to_le_bytes());
    cell.extend_from_slice(key);
    cell.extend_from_slice(&total_value_len.to_le_bytes());
    cell.extend_from_slice(&0u64.to_le_bytes()); // placeholder for first_overflow_page
    cell
}

/// Set the first overflow page ID in an overflow cell.
/// The page ID is at offset: 2 + key_len + 4 (after total_value_len).
pub fn set_overflow_page_id(cell: &mut [u8], page_id: PageId) {
    let raw_key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap());
    let key_len = (raw_key_len & !OVERFLOW_FLAG) as usize;
    let page_id_offset = 2 + key_len + 4; // skip key_len field + key + total_value_len
    cell[page_id_offset..page_id_offset + 8].copy_from_slice(&page_id.to_le_bytes());
}

/// Check if a raw cell is an overflow cell (high bit of key_len is set).
pub fn is_overflow_cell(cell: &[u8]) -> bool {
    if cell.len() < 2 {
        return false;
    }
    let raw_key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap());
    raw_key_len & OVERFLOW_FLAG != 0
}

/// Decode overflow metadata from a cell: (total_value_len, first_overflow_page).
/// Returns None if not an overflow cell.
pub fn decode_overflow_metadata(cell: &[u8]) -> Option<(u32, PageId)> {
    if !is_overflow_cell(cell) {
        return None;
    }
    let raw_key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap());
    let key_len = (raw_key_len & !OVERFLOW_FLAG) as usize;
    let meta_start = 2 + key_len;
    let total_value_len = u32::from_le_bytes(cell[meta_start..meta_start + 4].try_into().unwrap());
    let first_page = u64::from_le_bytes(cell[meta_start + 4..meta_start + 12].try_into().unwrap());
    Some((total_value_len, first_page))
}

/// Decode a leaf cell into (key, value).
/// For overflow cells, the "value" returned is empty.
/// Callers should check `is_overflow_cell` to determine if full value reconstruction is needed.
pub fn decode_leaf_cell(cell: &[u8]) -> (&[u8], &[u8]) {
    let raw_key_len = u16::from_le_bytes(cell[0..2].try_into().unwrap());
    if raw_key_len & OVERFLOW_FLAG != 0 {
        // Overflow cell: return key and empty value
        let key_len = (raw_key_len & !OVERFLOW_FLAG) as usize;
        let key = &cell[2..2 + key_len];
        (key, &[])
    } else {
        let key_len = raw_key_len as usize;
        let key = &cell[2..2 + key_len];
        let value = &cell[2 + key_len..];
        (key, value)
    }
}

/// Get the key of the i-th entry in a leaf node (0-based, entries start at cell index 1).
pub fn leaf_key(page: &Page, entry_idx: u16) -> Option<&[u8]> {
    let cell = page.cell(entry_idx + 1)?;
    let (key, _) = decode_leaf_cell(cell);
    Some(key)
}

/// Get the value of the i-th entry in a leaf node.
/// For overflow cells, returns empty slice.
pub fn leaf_value(page: &Page, entry_idx: u16) -> Option<&[u8]> {
    let cell = page.cell(entry_idx + 1)?;
    let (_, value) = decode_leaf_cell(cell);
    Some(value)
}

/// Get key and value of the i-th entry in a leaf node.
/// For overflow cells, the value is empty.
pub fn leaf_entry(page: &Page, entry_idx: u16) -> Option<(&[u8], &[u8])> {
    let cell = page.cell(entry_idx + 1)?;
    Some(decode_leaf_cell(cell))
}

/// Check if the i-th entry in a leaf node is an overflow cell.
pub fn leaf_is_overflow(page: &Page, entry_idx: u16) -> bool {
    page.cell(entry_idx + 1).is_some_and(is_overflow_cell)
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

    #[test]
    fn test_overflow_cell_encode_decode() {
        let key = b"testkey";
        let value_len = 5000u32;

        assert!(needs_overflow(key, &vec![0xABu8; value_len as usize]));

        let mut cell = encode_overflow_leaf_cell(key, value_len);
        assert!(is_overflow_cell(&cell));

        // Set overflow page ID
        set_overflow_page_id(&mut cell, 42);

        // Decode metadata
        let (total_len, first_page) = decode_overflow_metadata(&cell).unwrap();
        assert_eq!(total_len, 5000);
        assert_eq!(first_page, 42);

        // decode_leaf_cell should return key + empty value
        let (decoded_key, decoded_value) = decode_leaf_cell(&cell);
        assert_eq!(decoded_key, key);
        assert!(decoded_value.is_empty());

        // Cell should be compact: 2 + 7 + 12 = 21 bytes
        assert_eq!(cell.len(), 2 + 7 + 12);
    }

    #[test]
    fn test_needs_overflow_small_values() {
        assert!(!needs_overflow(b"key", b"value"));
        assert!(!needs_overflow(b"k", &vec![0u8; 4000]));
    }
}
