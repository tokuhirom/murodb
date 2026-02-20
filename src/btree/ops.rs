/// B-tree operations: search, insert (with split), delete (with merge).
///
/// The B-tree uses a pager for page I/O. Operations are performed on
/// in-memory pages obtained from the pager.
use crate::btree::key_encoding::compare_keys;
use crate::btree::node::*;
use crate::error::{MuroError, Result};
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
                    if let Some((k, v)) = leaf_entry(&page, i) {
                        match compare_keys(key, k) {
                            std::cmp::Ordering::Equal => return Ok(Some(v.to_vec())),
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
                    // Key exists - rebuild the page with updated value
                    let mut new_page = Page::new(page_id);
                    init_leaf(&mut new_page);
                    for j in 0..n {
                        if j == i {
                            let cell = encode_leaf_cell(key, value);
                            new_page
                                .insert_cell(&cell)
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

        // Try to insert into the page
        let cell = encode_leaf_cell(key, value);

        // Rebuild page with the new entry at the correct position
        let mut new_page = Page::new(page_id);
        init_leaf(&mut new_page);

        let mut inserted = false;
        for i in 0..n {
            if i == pos && !inserted {
                if new_page.insert_cell(&cell).is_err() {
                    // Need to split
                    return self.split_leaf(pager, &page, key, value, pos);
                }
                inserted = true;
            }
            if let Some(cell_data) = page.cell(i + 1) {
                if new_page.insert_cell(cell_data).is_err() {
                    return self.split_leaf(pager, &page, key, value, pos);
                }
            }
        }
        if !inserted && new_page.insert_cell(&cell).is_err() {
            return self.split_leaf(pager, &page, key, value, pos);
        }

        pager.write_page(&new_page)?;
        Ok(None)
    }

    fn split_leaf(
        &self,
        pager: &mut impl PageStore,
        old_page: &Page,
        new_key: &[u8],
        new_value: &[u8],
        insert_pos: u16,
    ) -> Result<Option<SplitResult>> {
        let old_id = old_page.page_id();
        let n = num_entries(old_page);

        // Collect all entries including the new one
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n as usize + 1);
        for i in 0..n {
            if i == insert_pos {
                entries.push((new_key.to_vec(), new_value.to_vec()));
            }
            if let Some((k, v)) = leaf_entry(old_page, i) {
                entries.push((k.to_vec(), v.to_vec()));
            }
        }
        if insert_pos == n {
            entries.push((new_key.to_vec(), new_value.to_vec()));
        }

        let mid = entries.len() / 2;
        let median_key = entries[mid].0.clone();

        // Left page (reuse old page id)
        let mut left = Page::new(old_id);
        init_leaf(&mut left);
        for (k, v) in &entries[..mid] {
            let cell = encode_leaf_cell(k, v);
            left.insert_cell(&cell)
                .map_err(|_| MuroError::PageOverflow)?;
        }

        // Right page (new page)
        let mut right = pager.allocate_page()?;
        let right_id = right.page_id();
        init_leaf(&mut right);
        for (k, v) in &entries[mid..] {
            let cell = encode_leaf_cell(k, v);
            right
                .insert_cell(&cell)
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

            // If the split child was the right-most child, we need to update the right child
            // and insert the new key pointing to the old right child
            let old_right = right_child(&page).ok_or(MuroError::InvalidPage)?;

            entries.insert(pos as usize, new_cell);

            // Update the right-child pointers:
            // The entry at `pos` has left_child = child_page_id (the left half after split)
            // The entry at `pos+1` (if it was previously at `pos`) should have its left child updated
            // Actually, we need to fix the child after the newly inserted key to point to split.right_page_id
            // The entry after pos should have left_child = split.right_page_id

            // Re-encode: for the entry at pos, left_child = child_page_id, key = split.median_key
            // For entry at pos+1, its left_child should be split.right_page_id

            // Actually let's think more carefully:
            // Before split, the child at pos was child_page_id.
            // After split, child_page_id is the left half, split.right_page_id is the right half.
            // We insert median_key between them.

            // In the internal node:
            // Entry at pos: left_child=child_page_id (left half), key=split.median_key
            // The next child pointer is either the left_child of entry pos+1, or right_child if pos is last.
            // That next child pointer should be split.right_page_id.

            // But wait, our entries list now has the new entry at pos with left_child=child_page_id.
            // The entry that was previously at pos has not changed. If child_idx was Some(i),
            // then the old entry at i had left_child = child_page_id (the original).
            // After split, the old entry at i should now have left_child = split.right_page_id.

            // Fix: update the left_child of the entry after the new one
            if (pos as usize + 1) < entries.len() {
                let old_entry = &entries[pos as usize + 1];
                let (_, old_key) = decode_internal_cell(old_entry);
                let new_entry = encode_internal_cell(split.right_page_id, old_key);
                entries[pos as usize + 1] = new_entry;
            }

            let new_right = if child_idx.is_none() {
                // The split child was the rightmost child
                // Update: the new entry at pos has left_child = child_page_id (old right child = left half)
                // The new right child of this internal node = split.right_page_id
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
        let (median_left_child, median_key_bytes) = decode_internal_cell(&entries[mid]);
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
                    // Try to rebalance: merge or redistribute with a sibling
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
    pub fn scan<F>(&self, pager: &mut impl PageStore, mut callback: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>, // return false to stop
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
                    if let Some((k, v)) = leaf_entry(&page, i) {
                        if !callback(k, v)? {
                            return Ok(());
                        }
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
                    if let Some((k, v)) = leaf_entry(&page, i) {
                        if compare_keys(k, start_key) != std::cmp::Ordering::Less
                            && !callback(k, v)?
                        {
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
        // We'll try to merge the child with its left sibling if possible, or right sibling.
        let (left_child_id, right_child_id, separator_idx) = match child_idx {
            Some(0) => {
                // Child is leftmost; merge with right sibling
                let left = internal_left_child(&parent, 0).ok_or(MuroError::InvalidPage)?;
                let right = if n > 1 {
                    internal_left_child(&parent, 1).ok_or(MuroError::InvalidPage)?
                } else {
                    right_child(&parent).ok_or(MuroError::InvalidPage)?
                };
                (left, right, 0u16)
            }
            Some(i) => {
                // Merge with left sibling
                let left = if i == 1 {
                    internal_left_child(&parent, 0).ok_or(MuroError::InvalidPage)?
                } else {
                    internal_left_child(&parent, i - 1).ok_or(MuroError::InvalidPage)?
                };
                let right = internal_left_child(&parent, i).ok_or(MuroError::InvalidPage)?;
                (left, right, i - 1)
            }
            None => {
                // Child is rightmost; merge with its left sibling
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

        // Collect all entries from both leaves
        let mut all_entries: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity((left_entries + right_entries) as usize);
        for i in 0..left_entries {
            if let Some((k, v)) = leaf_entry(&left_page, i) {
                all_entries.push((k.to_vec(), v.to_vec()));
            }
        }
        for i in 0..right_entries {
            if let Some((k, v)) = leaf_entry(&right_page, i) {
                all_entries.push((k.to_vec(), v.to_vec()));
            }
        }

        // Try to fit all entries into a single page
        let mut merged = Page::new(left_child_id);
        init_leaf(&mut merged);
        let mut fits = true;
        for (k, v) in &all_entries {
            let cell = encode_leaf_cell(k, v);
            if merged.insert_cell(&cell).is_err() {
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

            // Determine new right child: if we removed the last separator,
            // the merged node becomes the right child
            let new_right = if separator_idx == n - 1 && child_idx.is_none() {
                left_child_id
            } else {
                old_right
            };

            init_internal(&mut new_parent, new_right);
            for i in 0..n {
                if i == separator_idx {
                    // Skip the separator entry
                    // But if the entry after the separator pointed to right_child_id,
                    // update its left_child to left_child_id
                    continue;
                }
                if let Some(cell_data) = parent.cell(i + 1) {
                    if i == separator_idx + 1 {
                        // Update this entry's left_child to point to the merged node
                        let (_, entry_key) = decode_internal_cell(cell_data);
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

            // Handle the case where the right child was the merged right node
            if child_idx.is_none() {
                // The rightmost child was merged into the left - update right_child
                set_right_child(&mut new_parent, left_child_id);
            }

            pager.write_page(&new_parent)?;
        }

        Ok(())
    }

    /// Collect all page IDs in this B-tree (for freeing).
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
            Some(NodeType::Leaf) => Ok(()),
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
mod tests {
    use super::*;
    use crate::btree::key_encoding::encode_i64;
    use crate::crypto::aead::MasterKey;
    use crate::storage::pager::Pager;
    use tempfile::NamedTempFile;

    fn setup() -> (Pager, std::path::PathBuf) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        drop(tmp);
        std::fs::remove_file(&path).ok();
        let key = MasterKey::new([0x42u8; 32]);
        let pager = Pager::create(&path, &key).unwrap();
        (pager, path)
    }

    #[test]
    fn test_basic_insert_and_search() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        btree.insert(&mut pager, b"key1", b"value1").unwrap();
        btree.insert(&mut pager, b"key2", b"value2").unwrap();
        btree.insert(&mut pager, b"key3", b"value3").unwrap();

        assert_eq!(
            btree.search(&mut pager, b"key1").unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            btree.search(&mut pager, b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            btree.search(&mut pager, b"key3").unwrap(),
            Some(b"value3".to_vec())
        );
        assert_eq!(btree.search(&mut pager, b"key4").unwrap(), None);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_update_existing_key() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        btree.insert(&mut pager, b"key1", b"old").unwrap();
        assert_eq!(
            btree.search(&mut pager, b"key1").unwrap(),
            Some(b"old".to_vec())
        );

        btree.insert(&mut pager, b"key1", b"new").unwrap();
        assert_eq!(
            btree.search(&mut pager, b"key1").unwrap(),
            Some(b"new".to_vec())
        );

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_delete() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        btree.insert(&mut pager, b"a", b"1").unwrap();
        btree.insert(&mut pager, b"b", b"2").unwrap();
        btree.insert(&mut pager, b"c", b"3").unwrap();

        assert!(btree.delete(&mut pager, b"b").unwrap());
        assert_eq!(btree.search(&mut pager, b"b").unwrap(), None);
        assert_eq!(btree.search(&mut pager, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(btree.search(&mut pager, b"c").unwrap(), Some(b"3".to_vec()));

        assert!(!btree.delete(&mut pager, b"nonexistent").unwrap());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_scan() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        btree.insert(&mut pager, b"c", b"3").unwrap();
        btree.insert(&mut pager, b"a", b"1").unwrap();
        btree.insert(&mut pager, b"b", b"2").unwrap();

        let mut results = Vec::new();
        btree
            .scan(&mut pager, |k, v| {
                results.push((k.to_vec(), v.to_vec()));
                Ok(true)
            })
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[2], (b"c".to_vec(), b"3".to_vec()));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_many_inserts_with_splits() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        // Insert enough entries to force multiple splits
        let count = 500;
        for i in 0..count {
            let key = encode_i64(i);
            let value = format!("value_{}", i);
            btree.insert(&mut pager, &key, value.as_bytes()).unwrap();
        }

        // Verify all entries can be found
        for i in 0..count {
            let key = encode_i64(i);
            let expected = format!("value_{}", i);
            let result = btree.search(&mut pager, &key).unwrap();
            assert_eq!(
                result,
                Some(expected.into_bytes()),
                "Failed to find key {}",
                i
            );
        }

        // Verify scan returns all entries in order
        let mut scanned = Vec::new();
        btree
            .scan(&mut pager, |k, _v| {
                scanned.push(k.to_vec());
                Ok(true)
            })
            .unwrap();
        assert_eq!(scanned.len(), count as usize);
        for i in 0..scanned.len() - 1 {
            assert!(scanned[i] < scanned[i + 1], "Entries not in order at {}", i);
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_collect_all_pages_no_duplicates() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        // Insert enough entries with large values to force splits
        for i in 0..200 {
            let key = encode_i64(i);
            let value = vec![0xABu8; 100];
            btree.insert(&mut pager, &key, &value).unwrap();
        }

        let pages = btree.collect_all_pages(&mut pager).unwrap();
        assert!(pages.len() > 1, "tree should span multiple pages");

        // Verify no duplicates
        let mut seen = std::collections::HashSet::new();
        for &pid in &pages {
            assert!(
                seen.insert(pid),
                "duplicate page ID {} in collect_all_pages",
                pid
            );
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_collect_all_pages_detects_cycle() {
        use crate::btree::node::init_internal;

        let (mut pager, path) = setup();

        // Create an internal node whose right_child points back to itself (cycle)
        let root = pager.allocate_page().unwrap();
        let root_id = root.page_id();
        let mut root_page = Page::new(root_id);
        init_internal(&mut root_page, root_id); // right_child = self → cycle
        pager.write_page(&root_page).unwrap();

        let btree = BTree::open(root_id);
        let result = btree.collect_all_pages(&mut pager);

        match result {
            Err(MuroError::Corruption(msg)) => {
                assert!(msg.contains("cycle"), "expected cycle error, got: {}", msg);
            }
            other => panic!("expected Corruption error, got: {:?}", other),
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_collect_all_pages_detects_shared_child() {
        use crate::btree::node::{init_internal, init_leaf};

        let (mut pager, path) = setup();

        // Create a leaf page
        let leaf = pager.allocate_page().unwrap();
        let leaf_id = leaf.page_id();
        let mut leaf_page = Page::new(leaf_id);
        init_leaf(&mut leaf_page);
        pager.write_page(&leaf_page).unwrap();

        // Create an internal node with right_child = leaf and also an entry
        // whose left_child = leaf (same page referenced twice → duplicate)
        let root = pager.allocate_page().unwrap();
        let root_id = root.page_id();
        let mut root_page = Page::new(root_id);
        init_internal(&mut root_page, leaf_id); // right_child = leaf

        // Add an internal entry with left_child = leaf_id (same page!)
        let mut entry = Vec::new();
        entry.extend_from_slice(&leaf_id.to_le_bytes()); // left child pointer
        let key = b"key";
        let val = b"val";
        entry.extend_from_slice(&(key.len() as u16).to_le_bytes());
        entry.extend_from_slice(key);
        entry.extend_from_slice(&(val.len() as u16).to_le_bytes());
        entry.extend_from_slice(val);
        root_page.insert_cell(&entry).unwrap();

        pager.write_page(&root_page).unwrap();

        let btree = BTree::open(root_id);
        let result = btree.collect_all_pages(&mut pager);

        match result {
            Err(MuroError::Corruption(msg)) => {
                assert!(
                    msg.contains("cycle") || msg.contains("visited twice"),
                    "expected cycle/duplicate error, got: {}",
                    msg
                );
            }
            other => panic!("expected Corruption error, got: {:?}", other),
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_insert_delete_many() {
        let (mut pager, path) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();

        let count = 200;
        for i in 0..count {
            let key = encode_i64(i);
            btree.insert(&mut pager, &key, b"data").unwrap();
        }

        // Delete even keys
        for i in (0..count).step_by(2) {
            let key = encode_i64(i);
            assert!(btree.delete(&mut pager, &key).unwrap());
        }

        // Verify only odd keys remain
        for i in 0..count {
            let key = encode_i64(i);
            let result = btree.search(&mut pager, &key).unwrap();
            if i % 2 == 0 {
                assert_eq!(result, None, "Key {} should have been deleted", i);
            } else {
                assert_eq!(result, Some(b"data".to_vec()), "Key {} should exist", i);
            }
        }

        std::fs::remove_file(&path).ok();
    }
}
