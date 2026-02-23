/// B-tree operations: search, insert (with split), delete (with merge).
///
/// The B-tree uses a pager for page I/O. Operations are performed on
/// in-memory pages obtained from the pager.
use crate::btree::key_encoding::compare_keys;
use crate::btree::node::*;
use crate::error::{MuroError, Result};
use crate::storage::overflow;
use crate::storage::page::{Page, PageId};
use crate::storage::page_store::PageStore;

/// Minimum number of entries before considering merge/rebalance.
const MIN_ENTRIES: u16 = 2;

/// Maximum B-tree depth to prevent stack overflow on corrupted trees.
/// A 4096-byte page B-tree with 2 entries per internal node reaches depth 64
/// at 2^64 pages, which is far beyond practical limits.
const MAX_BTREE_DEPTH: usize = 64;

/// B-tree handle. Tracks the root page.
pub struct BTree {
    root_page_id: PageId,
}

impl BTree {
    /// Create a new B-tree with a fresh root leaf page.
    pub fn create(pager: &mut impl PageStore) -> Result<Self> {
        let mut root = pager.allocate_page()?;
        let root_id = root.page_id();
        init_leaf(&mut root);
        pager.write_page(&root)?;
        Ok(BTree {
            root_page_id: root_id,
        })
    }

    /// Open an existing B-tree given the root page id.
    pub fn open(root_page_id: PageId) -> Self {
        BTree { root_page_id }
    }

    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Search for a key. Returns the value if found.
    pub fn search(&self, pager: &mut impl PageStore, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.search_in_page(pager, self.root_page_id, key, 0)
    }

    fn search_in_page(
        &self,
        pager: &mut impl PageStore,
        page_id: PageId,
        key: &[u8],
        depth: usize,
    ) -> Result<Option<Vec<u8>>> {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        let page = pager.read_page(page_id)?;
        match node_type(&page) {
            Some(NodeType::Leaf) => {
                let n = num_entries(&page);
                for i in 0..n {
                    if let Some(k) = leaf_key(&page, i) {
                        match compare_keys(key, k) {
                            std::cmp::Ordering::Equal => {
                                // Check for overflow
                                let cell = page.cell(i + 1).ok_or(MuroError::InvalidPage)?;
                                if is_overflow_cell(cell) {
                                    let (total_len, first_page) = decode_overflow_metadata(cell)
                                        .ok_or_else(|| {
                                            MuroError::Corruption(
                                                "invalid overflow metadata in leaf cell".into(),
                                            )
                                        })?;
                                    let value = overflow::read_overflow_chain(
                                        pager, first_page, total_len,
                                    )?;
                                    return Ok(Some(value));
                                }
                                let (_, v) = decode_leaf_cell(cell).ok_or_else(|| {
                                    MuroError::Corruption("invalid leaf cell encoding".into())
                                })?;
                                return Ok(Some(v.to_vec()));
                            }
                            std::cmp::Ordering::Less => return Ok(None),
                            std::cmp::Ordering::Greater => continue,
                        }
                    }
                }
                Ok(None)
            }
            Some(NodeType::Internal) => {
                let child_id = find_child(&page, key).ok_or(MuroError::InvalidPage)?;
                self.search_in_page(pager, child_id, key, depth + 1)
            }
            None => Err(MuroError::InvalidPage),
        }
    }

    /// Insert a key-value pair. If key exists, update the value.
    pub fn insert(&mut self, pager: &mut impl PageStore, key: &[u8], value: &[u8]) -> Result<()> {
        let result = self.insert_into_page(pager, self.root_page_id, key, value, 0)?;

        if let Some(split) = result {
            // Root was split; create a new root
            let mut new_root = pager.allocate_page()?;
            let new_root_id = new_root.page_id();
            init_internal(&mut new_root, split.right_page_id);

            let cell = encode_internal_cell(self.root_page_id, &split.median_key);
            new_root
                .insert_cell(&cell)
                .map_err(|_| MuroError::PageOverflow)?;
            pager.write_page(&new_root)?;
            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    /// Result of inserting into a node that caused a split.
    fn insert_into_page(
        &mut self,
        pager: &mut impl PageStore,
        page_id: PageId,
        key: &[u8],
        value: &[u8],
        depth: usize,
    ) -> Result<Option<SplitResult>> {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        let page = pager.read_page(page_id)?;

        match node_type(&page) {
            Some(NodeType::Leaf) => self.insert_into_leaf(pager, page, key, value),
            Some(NodeType::Internal) => self.insert_into_internal(pager, page, key, value, depth),
            None => Err(MuroError::InvalidPage),
        }
    }

    fn insert_into_leaf(
        &self,
        pager: &mut impl PageStore,
        page: Page,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<SplitResult>> {
        let page_id = page.page_id();
        let n = num_entries(&page);

        // Check for existing key (update in place)
        for i in 0..n {
            if let Some(k) = leaf_key(&page, i) {
                if compare_keys(key, k) == std::cmp::Ordering::Equal {
                    // Free old overflow chain if the existing cell is overflow
                    let old_cell = page.cell(i + 1).ok_or(MuroError::InvalidPage)?;
                    if is_overflow_cell(old_cell) {
                        if let Some((_, first_page)) = decode_overflow_metadata(old_cell) {
                            overflow::free_overflow_chain(pager, first_page)?;
                        }
                    }

                    // Encode new cell (possibly with overflow)
                    let new_cell_bytes = self.encode_cell_with_overflow(pager, key, value)?;

                    // Rebuild the page with updated value
                    let mut new_page = Page::new(page_id);
                    init_leaf(&mut new_page);
                    for j in 0..n {
                        if j == i {
                            new_page
                                .insert_cell(&new_cell_bytes)
                                .map_err(|_| MuroError::PageOverflow)?;
                        } else if let Some(cell_data) = page.cell(j + 1) {
                            new_page
                                .insert_cell(cell_data)
                                .map_err(|_| MuroError::PageOverflow)?;
                        }
                    }
                    pager.write_page(&new_page)?;
                    return Ok(None);
                }
            }
        }

        // Find insertion position (maintain sorted order)
        let mut pos = n;
        for i in 0..n {
            if let Some(k) = leaf_key(&page, i) {
                if compare_keys(key, k) == std::cmp::Ordering::Less {
                    pos = i;
                    break;
                }
            }
        }

        // Encode cell (possibly with overflow)
        let cell = self.encode_cell_with_overflow(pager, key, value)?;

        // Rebuild page with the new entry at the correct position
        let mut new_page = Page::new(page_id);
        init_leaf(&mut new_page);

        let mut inserted = false;
        for i in 0..n {
            if i == pos && !inserted {
                if new_page.insert_cell(&cell).is_err() {
                    // Need to split — work with raw cells to preserve overflow pointers
                    return self.split_leaf_raw(pager, &page, &cell, pos);
                }
                inserted = true;
            }
            if let Some(cell_data) = page.cell(i + 1) {
                if new_page.insert_cell(cell_data).is_err() {
                    return self.split_leaf_raw(pager, &page, &cell, pos);
                }
            }
        }
        if !inserted && new_page.insert_cell(&cell).is_err() {
            return self.split_leaf_raw(pager, &page, &cell, pos);
        }

        pager.write_page(&new_page)?;
        Ok(None)
    }

    /// Encode a key+value as a leaf cell, using overflow if needed.
    fn encode_cell_with_overflow(
        &self,
        pager: &mut impl PageStore,
        key: &[u8],
        value: &[u8],
    ) -> Result<Vec<u8>> {
        if needs_overflow(key, value) {
            let total_value_len = u32::try_from(value.len()).map_err(|_| {
                MuroError::Execution(format!(
                    "value too large: {} bytes exceeds maximum of {} bytes",
                    value.len(),
                    u32::MAX
                ))
            })?;
            let mut cell = encode_overflow_leaf_cell(key, total_value_len);
            let first_page = overflow::write_overflow_chain(pager, value)?;
            set_overflow_page_id(&mut cell, first_page);
            Ok(cell)
        } else {
            Ok(encode_leaf_cell(key, value))
        }
    }

    /// Split a leaf node, working with raw cell bytes to preserve overflow pointers.
    fn split_leaf_raw(
        &self,
        pager: &mut impl PageStore,
        old_page: &Page,
        new_cell: &[u8],
        insert_pos: u16,
    ) -> Result<Option<SplitResult>> {
        let old_id = old_page.page_id();
        let n = num_entries(old_page);

        // Collect all raw cells including the new one
        let mut cells: Vec<Vec<u8>> = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            if i == insert_pos {
                cells.push(new_cell.to_vec());
            }
            if let Some(cell_data) = old_page.cell(i + 1) {
                cells.push(cell_data.to_vec());
            }
        }
        if insert_pos == n {
            cells.push(new_cell.to_vec());
        }

        let mid = cells.len() / 2;
        let (median_key, _) = decode_leaf_cell(&cells[mid])
            .ok_or_else(|| MuroError::Corruption("invalid leaf cell encoding".into()))?;
        let median_key = median_key.to_vec();

        // Left page (reuse old page id)
        let mut left = Page::new(old_id);
        init_leaf(&mut left);
        for cell in &cells[..mid] {
            left.insert_cell(cell)
                .map_err(|_| MuroError::PageOverflow)?;
        }

        // Right page (new page)
        let mut right = pager.allocate_page()?;
        let right_id = right.page_id();
        init_leaf(&mut right);
        for cell in &cells[mid..] {
            right
                .insert_cell(cell)
                .map_err(|_| MuroError::PageOverflow)?;
        }

        pager.write_page(&left)?;
        pager.write_page(&right)?;

        Ok(Some(SplitResult {
            median_key,
            right_page_id: right_id,
        }))
    }

    fn insert_into_internal(
        &mut self,
        pager: &mut impl PageStore,
        page: Page,
        key: &[u8],
        value: &[u8],
        depth: usize,
    ) -> Result<Option<SplitResult>> {
        let page_id = page.page_id();

        // Find child to recurse into
        let n = num_entries(&page);
        let mut child_idx: Option<u16> = None;
        let mut child_page_id = right_child(&page).ok_or(MuroError::InvalidPage)?;

        for i in 0..n {
            if let Some(k) = internal_key(&page, i) {
                if compare_keys(key, k) == std::cmp::Ordering::Less {
                    child_page_id = internal_left_child(&page, i).ok_or(MuroError::InvalidPage)?;
                    child_idx = Some(i);
                    break;
                }
            }
        }

        let split = self.insert_into_page(pager, child_page_id, key, value, depth + 1)?;

        if let Some(split) = split {
            // Child was split. Insert median key + right child into this internal node.
            let page = pager.read_page(page_id)?;

            // Find insertion position in internal node
            let n = num_entries(&page);
            let pos = child_idx.unwrap_or(n);

            // Rebuild page with new entry
            let new_cell = encode_internal_cell(child_page_id, &split.median_key);

            // Collect all entries
            let mut entries: Vec<Vec<u8>> = Vec::with_capacity(n as usize + 1);
            for i in 0..n {
                if let Some(cell_data) = page.cell(i + 1) {
                    entries.push(cell_data.to_vec());
                }
            }

            let old_right = right_child(&page).ok_or(MuroError::InvalidPage)?;

            entries.insert(pos as usize, new_cell);

            if (pos as usize + 1) < entries.len() {
                let old_entry = &entries[pos as usize + 1];
                let (_, old_key) = decode_internal_cell(old_entry).ok_or_else(|| {
                    MuroError::Corruption("invalid internal cell encoding".into())
                })?;
                let new_entry = encode_internal_cell(split.right_page_id, old_key);
                entries[pos as usize + 1] = new_entry;
            }

            let new_right = if child_idx.is_none() {
                split.right_page_id
            } else {
                old_right
            };

            // Try to rebuild the page
            let mut new_page = Page::new(page_id);
            init_internal(&mut new_page, new_right);
            let mut overflow = false;
            for entry in &entries {
                if new_page.insert_cell(entry).is_err() {
                    overflow = true;
                    break;
                }
            }

            if overflow {
                // Split this internal node
                return self.split_internal(pager, page_id, &entries, new_right);
            }

            pager.write_page(&new_page)?;
            return Ok(None);
        }

        Ok(None)
    }

    fn split_internal(
        &self,
        pager: &mut impl PageStore,
        old_id: PageId,
        entries: &[Vec<u8>],
        current_right: PageId,
    ) -> Result<Option<SplitResult>> {
        let mid = entries.len() / 2;

        // The median entry's key goes up to the parent
        let (median_left_child, median_key_bytes) = decode_internal_cell(&entries[mid])
            .ok_or_else(|| MuroError::Corruption("invalid internal cell encoding".into()))?;
        let median_key = median_key_bytes.to_vec();

        // Left page: entries[0..mid], right child = median_left_child
        let mut left = Page::new(old_id);
        init_internal(&mut left, median_left_child);
        for entry in &entries[..mid] {
            left.insert_cell(entry)
                .map_err(|_| MuroError::PageOverflow)?;
        }

        // Right page: entries[mid+1..], right child = current_right
        let mut right = pager.allocate_page()?;
        let right_id = right.page_id();
        init_internal(&mut right, current_right);
        for entry in &entries[mid + 1..] {
            right
                .insert_cell(entry)
                .map_err(|_| MuroError::PageOverflow)?;
        }

        pager.write_page(&left)?;
        pager.write_page(&right)?;

        Ok(Some(SplitResult {
            median_key,
            right_page_id: right_id,
        }))
    }

    /// Delete a key. Returns true if the key was found and deleted.
    pub fn delete(&mut self, pager: &mut impl PageStore, key: &[u8]) -> Result<bool> {
        let (deleted, _) = self.delete_from_page(pager, self.root_page_id, key, 0)?;

        if deleted {
            // Check if root is an internal node with 0 entries
            let root = pager.read_page(self.root_page_id)?;
            if node_type(&root) == Some(NodeType::Internal) && num_entries(&root) == 0 {
                // Collapse root: the single child becomes the new root
                if let Some(child) = right_child(&root) {
                    let old_root = self.root_page_id;
                    self.root_page_id = child;
                    pager.free_page(old_root);
                }
            }
        }

        Ok(deleted)
    }

    /// Returns (was_deleted, is_underfull).
    fn delete_from_page(
        &mut self,
        pager: &mut impl PageStore,
        page_id: PageId,
        key: &[u8],
        depth: usize,
    ) -> Result<(bool, bool)> {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        let page = pager.read_page(page_id)?;

        match node_type(&page) {
            Some(NodeType::Leaf) => {
                let n = num_entries(&page);
                let mut found_idx = None;

                for i in 0..n {
                    if let Some(k) = leaf_key(&page, i) {
                        if compare_keys(key, k) == std::cmp::Ordering::Equal {
                            found_idx = Some(i);
                            break;
                        }
                    }
                }

                if let Some(idx) = found_idx {
                    // Free overflow chain if this is an overflow cell
                    let cell = page.cell(idx + 1).ok_or(MuroError::InvalidPage)?;
                    if is_overflow_cell(cell) {
                        if let Some((_, first_page)) = decode_overflow_metadata(cell) {
                            if first_page != overflow::NO_OVERFLOW_PAGE {
                                overflow::free_overflow_chain(pager, first_page)?;
                            }
                        }
                    }

                    let mut new_page = Page::new(page_id);
                    init_leaf(&mut new_page);
                    for i in 0..n {
                        if i != idx {
                            if let Some(cell_data) = page.cell(i + 1) {
                                new_page
                                    .insert_cell(cell_data)
                                    .map_err(|_| MuroError::PageOverflow)?;
                            }
                        }
                    }
                    let underfull = num_entries(&new_page) < MIN_ENTRIES;
                    pager.write_page(&new_page)?;
                    Ok((true, underfull))
                } else {
                    Ok((false, false))
                }
            }
            Some(NodeType::Internal) => {
                // Find which child to recurse into
                let n = num_entries(&page);
                let mut child_idx: Option<u16> = None;
                let mut child_page_id = right_child(&page).ok_or(MuroError::InvalidPage)?;

                for i in 0..n {
                    if let Some(k) = internal_key(&page, i) {
                        if compare_keys(key, k) == std::cmp::Ordering::Less {
                            child_page_id =
                                internal_left_child(&page, i).ok_or(MuroError::InvalidPage)?;
                            child_idx = Some(i);
                            break;
                        }
                    }
                }

                let (deleted, underfull) =
                    self.delete_from_page(pager, child_page_id, key, depth + 1)?;

                if deleted && underfull {
                    self.try_rebalance(pager, page_id, child_idx)?;
                }

                // Check if this internal node itself is underfull
                let page = pager.read_page(page_id)?;
                let underfull = num_entries(&page) < MIN_ENTRIES;
                Ok((deleted, underfull))
            }
            None => Err(MuroError::InvalidPage),
        }
    }

    /// Iterate over all key-value pairs in sorted order.
    /// Calls the callback with (key, value) for each entry.
    /// For overflow cells, the full value is reconstructed before calling the callback.
    pub fn scan<F>(&self, pager: &mut impl PageStore, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.scan_page(pager, self.root_page_id, &mut callback, 0)
    }

    fn scan_page<F>(
        &self,
        pager: &mut impl PageStore,
        page_id: PageId,
        callback: &mut F,
        depth: usize,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        let page = pager.read_page(page_id)?;

        match node_type(&page) {
            Some(NodeType::Leaf) => {
                let n = num_entries(&page);
                for i in 0..n {
                    let cell = page.cell(i + 1).ok_or(MuroError::InvalidPage)?;
                    let (k, v) = decode_leaf_cell(cell).ok_or_else(|| {
                        MuroError::Corruption("invalid leaf cell encoding".into())
                    })?;
                    if is_overflow_cell(cell) {
                        let (total_len, first_page) =
                            decode_overflow_metadata(cell).ok_or_else(|| {
                                MuroError::Corruption(
                                    "invalid overflow metadata in leaf cell".into(),
                                )
                            })?;
                        let full_value =
                            overflow::read_overflow_chain(pager, first_page, total_len)?;
                        if !callback(k, &full_value)? {
                            return Ok(());
                        }
                    } else if !callback(k, v)? {
                        return Ok(());
                    }
                }
                Ok(())
            }
            Some(NodeType::Internal) => {
                let n = num_entries(&page);
                for i in 0..n {
                    let left = internal_left_child(&page, i).ok_or(MuroError::InvalidPage)?;
                    self.scan_page(pager, left, callback, depth + 1)?;
                }
                let right = right_child(&page).ok_or(MuroError::InvalidPage)?;
                self.scan_page(pager, right, callback, depth + 1)?;
                Ok(())
            }
            None => Err(MuroError::InvalidPage),
        }
    }

    /// Range scan: iterate over entries where key >= start_key.
    pub fn scan_from<F>(
        &self,
        pager: &mut impl PageStore,
        start_key: &[u8],
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        self.scan_from_page(pager, self.root_page_id, start_key, &mut callback, 0)
    }

    fn scan_from_page<F>(
        &self,
        pager: &mut impl PageStore,
        page_id: PageId,
        start_key: &[u8],
        callback: &mut F,
        depth: usize,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        let page = pager.read_page(page_id)?;

        match node_type(&page) {
            Some(NodeType::Leaf) => {
                let n = num_entries(&page);
                for i in 0..n {
                    let cell = page.cell(i + 1).ok_or(MuroError::InvalidPage)?;
                    let (k, v) = decode_leaf_cell(cell).ok_or_else(|| {
                        MuroError::Corruption("invalid leaf cell encoding".into())
                    })?;
                    if compare_keys(k, start_key) != std::cmp::Ordering::Less {
                        if is_overflow_cell(cell) {
                            let (total_len, first_page) = decode_overflow_metadata(cell)
                                .ok_or_else(|| {
                                    MuroError::Corruption(
                                        "invalid overflow metadata in leaf cell".into(),
                                    )
                                })?;
                            let full_value =
                                overflow::read_overflow_chain(pager, first_page, total_len)?;
                            if !callback(k, &full_value)? {
                                return Ok(());
                            }
                        } else if !callback(k, v)? {
                            return Ok(());
                        }
                    }
                }
                Ok(())
            }
            Some(NodeType::Internal) => {
                let n = num_entries(&page);
                let mut started = false;
                for i in 0..n {
                    let entry_key = internal_key(&page, i).ok_or(MuroError::InvalidPage)?;
                    if !started && compare_keys(start_key, entry_key) == std::cmp::Ordering::Less {
                        let left = internal_left_child(&page, i).ok_or(MuroError::InvalidPage)?;
                        self.scan_from_page(pager, left, start_key, callback, depth + 1)?;
                        started = true;
                    } else if started {
                        let left = internal_left_child(&page, i).ok_or(MuroError::InvalidPage)?;
                        self.scan_page(pager, left, callback, depth + 1)?;
                    }
                }
                if !started {
                    // start_key >= all keys, scan from rightmost child
                }
                let right = right_child(&page).ok_or(MuroError::InvalidPage)?;
                if started {
                    self.scan_page(pager, right, callback, depth + 1)?;
                } else {
                    self.scan_from_page(pager, right, start_key, callback, depth + 1)?;
                }
                Ok(())
            }
            None => Err(MuroError::InvalidPage),
        }
    }

    /// Try to rebalance an underfull child by merging with a sibling.
    /// `child_idx` is Some(i) if the child was found via entry i's left_child,
    /// or None if the child is the rightmost child.
    fn try_rebalance(
        &mut self,
        pager: &mut impl PageStore,
        parent_page_id: PageId,
        child_idx: Option<u16>,
    ) -> Result<()> {
        let parent = pager.read_page(parent_page_id)?;
        let n = num_entries(&parent);
        if n == 0 {
            return Ok(()); // Single child, nothing to merge with
        }

        // Determine the child and its sibling for merging
        let (left_child_id, right_child_id, separator_idx) = match child_idx {
            Some(0) => {
                let left = internal_left_child(&parent, 0).ok_or(MuroError::InvalidPage)?;
                let right = if n > 1 {
                    internal_left_child(&parent, 1).ok_or(MuroError::InvalidPage)?
                } else {
                    right_child(&parent).ok_or(MuroError::InvalidPage)?
                };
                (left, right, 0u16)
            }
            Some(i) => {
                let left = if i == 1 {
                    internal_left_child(&parent, 0).ok_or(MuroError::InvalidPage)?
                } else {
                    internal_left_child(&parent, i - 1).ok_or(MuroError::InvalidPage)?
                };
                let right = internal_left_child(&parent, i).ok_or(MuroError::InvalidPage)?;
                (left, right, i - 1)
            }
            None => {
                let left = internal_left_child(&parent, n - 1).ok_or(MuroError::InvalidPage)?;
                let right = right_child(&parent).ok_or(MuroError::InvalidPage)?;
                (left, right, n - 1)
            }
        };

        let left_page = pager.read_page(left_child_id)?;
        let right_page = pager.read_page(right_child_id)?;

        let left_type = node_type(&left_page);
        let right_type = node_type(&right_page);

        // Only merge leaf nodes for now (simpler and most common case)
        if left_type != Some(NodeType::Leaf) || right_type != Some(NodeType::Leaf) {
            return Ok(());
        }

        let left_entries = num_entries(&left_page);
        let right_entries = num_entries(&right_page);

        // Collect all raw cells from both leaves (preserves overflow pointers)
        let mut all_cells: Vec<Vec<u8>> =
            Vec::with_capacity((left_entries + right_entries) as usize);
        for i in 0..left_entries {
            if let Some(cell_data) = left_page.cell(i + 1) {
                all_cells.push(cell_data.to_vec());
            }
        }
        for i in 0..right_entries {
            if let Some(cell_data) = right_page.cell(i + 1) {
                all_cells.push(cell_data.to_vec());
            }
        }

        // Try to fit all cells into a single page
        let mut merged = Page::new(left_child_id);
        init_leaf(&mut merged);
        let mut fits = true;
        for cell in &all_cells {
            if merged.insert_cell(cell).is_err() {
                fits = false;
                break;
            }
        }

        if fits {
            // All entries fit in one page - merge successful
            pager.write_page(&merged)?;
            pager.free_page(right_child_id);

            // Remove the separator entry from the parent and update pointers
            let parent = pager.read_page(parent_page_id)?;
            let old_right = right_child(&parent).ok_or(MuroError::InvalidPage)?;
            let mut new_parent = Page::new(parent_page_id);

            let new_right = if separator_idx == n - 1 && child_idx.is_none() {
                left_child_id
            } else {
                old_right
            };

            init_internal(&mut new_parent, new_right);
            for i in 0..n {
                if i == separator_idx {
                    continue;
                }
                if let Some(cell_data) = parent.cell(i + 1) {
                    if i == separator_idx + 1 {
                        let (_, entry_key) = decode_internal_cell(cell_data).ok_or_else(|| {
                            MuroError::Corruption("invalid internal cell encoding".into())
                        })?;
                        let new_cell = encode_internal_cell(left_child_id, entry_key);
                        new_parent
                            .insert_cell(&new_cell)
                            .map_err(|_| MuroError::PageOverflow)?;
                    } else {
                        new_parent
                            .insert_cell(cell_data)
                            .map_err(|_| MuroError::PageOverflow)?;
                    }
                }
            }

            if child_idx.is_none() {
                set_right_child(&mut new_parent, left_child_id);
            }

            pager.write_page(&new_parent)?;
        }

        Ok(())
    }

    /// Collect all page IDs in this B-tree (for freeing), including overflow pages.
    pub fn collect_all_pages(&self, pager: &mut impl PageStore) -> Result<Vec<PageId>> {
        let mut pages = Vec::new();
        let mut visited = std::collections::HashSet::new();
        self.collect_pages_recursive(pager, self.root_page_id, &mut pages, &mut visited, 0)?;
        Ok(pages)
    }

    fn collect_pages_recursive(
        &self,
        pager: &mut impl PageStore,
        page_id: PageId,
        pages: &mut Vec<PageId>,
        visited: &mut std::collections::HashSet<PageId>,
        depth: usize,
    ) -> Result<()> {
        if depth > MAX_BTREE_DEPTH {
            return Err(MuroError::Corruption(
                "B-tree depth exceeds maximum (possible cycle)".into(),
            ));
        }
        if !visited.insert(page_id) {
            return Err(MuroError::Corruption(format!(
                "B-tree cycle detected: page {} visited twice during collection",
                page_id
            )));
        }
        pages.push(page_id);
        let page = pager.read_page(page_id)?;
        match node_type(&page) {
            Some(NodeType::Leaf) => {
                // Collect overflow pages for each overflow cell
                let n = num_entries(&page);
                for i in 0..n {
                    if let Some(cell) = page.cell(i + 1) {
                        if is_overflow_cell(cell) {
                            if let Some((_, first_page)) = decode_overflow_metadata(cell) {
                                if first_page != overflow::NO_OVERFLOW_PAGE {
                                    let overflow_pages =
                                        overflow::collect_overflow_pages(pager, first_page)?;
                                    pages.extend(overflow_pages);
                                }
                            }
                        }
                    }
                }
                Ok(())
            }
            Some(NodeType::Internal) => {
                let n = num_entries(&page);
                for i in 0..n {
                    if let Some(child) = internal_left_child(&page, i) {
                        self.collect_pages_recursive(pager, child, pages, visited, depth + 1)?;
                    }
                }
                if let Some(right) = right_child(&page) {
                    self.collect_pages_recursive(pager, right, pages, visited, depth + 1)?;
                }
                Ok(())
            }
            None => Err(MuroError::InvalidPage),
        }
    }
}

struct SplitResult {
    median_key: Vec<u8>,
    right_page_id: PageId,
}

#[cfg(test)]
mod tests;
