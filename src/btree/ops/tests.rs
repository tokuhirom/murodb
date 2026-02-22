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
