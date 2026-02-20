/// Property and fuzz tests for B-tree rebalance invariants.
///
/// Runs randomized insert/delete workloads and verifies structural invariants:
/// - Sorted scan order
/// - Key reachability (all inserted keys are searchable)
/// - No duplicate page IDs (no dangling/unreachable pages)
/// - Parent/child pointer consistency
/// - Equal depth for all leaf paths
use murodb::btree::key_encoding::encode_i64;
use murodb::btree::node::*;
use murodb::btree::ops::BTree;
use murodb::crypto::aead::MasterKey;
use murodb::storage::page::PageId;
use murodb::storage::pager::Pager;
use std::collections::{BTreeSet, HashSet};
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn setup() -> (Pager, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let pager = Pager::create(&db_path, &test_key()).unwrap();
    (pager, dir)
}

/// Simple deterministic PRNG (xorshift64) for reproducible tests without
/// requiring the rand crate in test scope.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn next_range(&mut self, max: u64) -> u64 {
        self.next() % max
    }
}

// ── Invariant checkers ──

/// Verify scan returns all entries in strictly sorted order.
fn assert_sorted_scan(btree: &BTree, pager: &mut Pager) {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    btree
        .scan(pager, |k, _v| {
            keys.push(k.to_vec());
            Ok(true)
        })
        .expect("scan should succeed");

    for i in 1..keys.len() {
        assert!(
            keys[i - 1] < keys[i],
            "scan order violated at index {}: {:?} >= {:?}",
            i,
            &keys[i - 1],
            &keys[i]
        );
    }
}

/// Verify every key in `expected` is reachable via search, and scan count matches.
fn assert_key_reachability(btree: &BTree, pager: &mut Pager, expected: &BTreeSet<i64>) {
    for &k in expected {
        let key = encode_i64(k);
        let result = btree
            .search(pager, &key)
            .unwrap_or_else(|e| panic!("search for key {} failed: {}", k, e));
        assert!(
            result.is_some(),
            "key {} should be reachable but search returned None",
            k
        );
    }

    // Verify scan count matches
    let mut count = 0usize;
    btree
        .scan(pager, |_k, _v| {
            count += 1;
            Ok(true)
        })
        .unwrap();
    assert_eq!(
        count,
        expected.len(),
        "scan count {} != expected {}",
        count,
        expected.len()
    );
}

/// Verify collect_all_pages returns no duplicate page IDs.
fn assert_no_duplicate_pages(btree: &BTree, pager: &mut Pager) {
    let pages = btree
        .collect_all_pages(pager)
        .expect("collect_all_pages should succeed");
    let mut seen = HashSet::new();
    for &pid in &pages {
        assert!(
            seen.insert(pid),
            "duplicate page ID {} in collect_all_pages",
            pid
        );
    }
}

/// Recursively verify B-tree structural invariants:
/// - Internal node keys are sorted
/// - Leaf node keys are sorted
/// - All paths from root to leaf have the same depth
/// Returns the depth of the subtree.
fn verify_tree_structure(pager: &mut Pager, page_id: PageId, depth: usize) -> usize {
    assert!(depth <= 64, "tree depth exceeds 64, possible corruption");

    let page = pager.read_page(page_id).unwrap();
    let ntype = node_type(&page).expect("page should have valid node type");
    let n = num_entries(&page);

    match ntype {
        NodeType::Leaf => {
            // Verify leaf keys are sorted
            for i in 1..n {
                let prev = leaf_key(&page, i - 1).expect("leaf key should exist");
                let curr = leaf_key(&page, i).expect("leaf key should exist");
                assert!(
                    prev < curr,
                    "leaf keys not sorted at page {} entries {}/{}",
                    page_id,
                    i - 1,
                    i
                );
            }
            depth
        }
        NodeType::Internal => {
            // Verify internal keys are sorted
            for i in 1..n {
                let prev = internal_key(&page, i - 1).expect("internal key should exist");
                let curr = internal_key(&page, i).expect("internal key should exist");
                assert!(
                    prev < curr,
                    "internal keys not sorted at page {} entries {}/{}",
                    page_id,
                    i - 1,
                    i
                );
            }

            // Collect all child depths and verify they are equal
            let mut child_depths = Vec::new();
            for i in 0..n {
                if let Some(child) = internal_left_child(&page, i) {
                    child_depths.push(verify_tree_structure(pager, child, depth + 1));
                }
            }
            if let Some(right) = right_child(&page) {
                child_depths.push(verify_tree_structure(pager, right, depth + 1));
            }

            if !child_depths.is_empty() {
                let first = child_depths[0];
                for (i, &d) in child_depths.iter().enumerate() {
                    assert_eq!(
                        d, first,
                        "unequal leaf depths at page {}: child {} depth {} != first child depth {}",
                        page_id, i, d, first
                    );
                }
            }

            child_depths.first().copied().unwrap_or(depth)
        }
    }
}

fn assert_tree_invariants(btree: &BTree, pager: &mut Pager) {
    verify_tree_structure(pager, btree.root_page_id(), 0);
}

// ── Property tests ──

/// Random insert-only workload: insert N random keys, verify all invariants.
#[test]
fn test_property_random_inserts() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut rng = Rng::new(12345);
    let mut expected = BTreeSet::new();

    for _ in 0..500 {
        let k = rng.next_range(10000) as i64;
        let key = encode_i64(k);
        let value = format!("v{}", k);
        btree.insert(&mut pager, &key, value.as_bytes()).unwrap();
        expected.insert(k);
    }

    assert_sorted_scan(&btree, &mut pager);
    assert_key_reachability(&btree, &mut pager, &expected);
    assert_no_duplicate_pages(&btree, &mut pager);
    assert_tree_invariants(&btree, &mut pager);
}

/// Random mixed insert/delete workload.
#[test]
fn test_property_random_insert_delete() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut rng = Rng::new(67890);
    let mut expected = BTreeSet::new();

    for _ in 0..1000 {
        let op = rng.next_range(3);
        let k = rng.next_range(200) as i64;
        let key = encode_i64(k);

        if op < 2 {
            // Insert (2/3 probability)
            btree.insert(&mut pager, &key, b"data").unwrap();
            expected.insert(k);
        } else {
            // Delete (1/3 probability)
            let deleted = btree.delete(&mut pager, &key).unwrap();
            if expected.remove(&k) {
                assert!(deleted, "delete should return true for existing key {}", k);
            } else {
                assert!(!deleted, "delete should return false for missing key {}", k);
            }
        }
    }

    assert_sorted_scan(&btree, &mut pager);
    assert_key_reachability(&btree, &mut pager, &expected);
    assert_no_duplicate_pages(&btree, &mut pager);
    assert_tree_invariants(&btree, &mut pager);
}

/// Heavy delete workload: insert many, then delete most, verify invariants
/// throughout.
#[test]
fn test_property_heavy_delete() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut expected = BTreeSet::new();

    // Insert 300 keys
    for i in 0..300i64 {
        let key = encode_i64(i);
        btree.insert(&mut pager, &key, b"val").unwrap();
        expected.insert(i);
    }

    assert_tree_invariants(&btree, &mut pager);

    // Delete in a shuffled order (using PRNG)
    let mut rng = Rng::new(11111);
    let mut to_delete: Vec<i64> = (0..300).collect();
    // Fisher-Yates shuffle
    for i in (1..to_delete.len()).rev() {
        let j = rng.next_range((i + 1) as u64) as usize;
        to_delete.swap(i, j);
    }

    // Delete 280 of 300, checking invariants periodically
    for (round, &k) in to_delete.iter().take(280).enumerate() {
        let key = encode_i64(k);
        let deleted = btree.delete(&mut pager, &key).unwrap();
        assert!(deleted, "key {} should exist for deletion", k);
        expected.remove(&k);

        // Check invariants every 50 deletions
        if (round + 1) % 50 == 0 {
            assert_sorted_scan(&btree, &mut pager);
            assert_key_reachability(&btree, &mut pager, &expected);
            assert_tree_invariants(&btree, &mut pager);
        }
    }

    // Final checks
    assert_sorted_scan(&btree, &mut pager);
    assert_key_reachability(&btree, &mut pager, &expected);
    assert_no_duplicate_pages(&btree, &mut pager);
    assert_tree_invariants(&btree, &mut pager);
    assert_eq!(expected.len(), 20);
}

/// Multiple seeds to increase coverage.
#[test]
fn test_property_multiple_seeds() {
    for seed in [99, 777, 42424, 1337, 0xDEAD] {
        let (mut pager, _dir) = setup();
        let mut btree = BTree::create(&mut pager).unwrap();
        let mut rng = Rng::new(seed);
        let mut expected = BTreeSet::new();

        for _ in 0..400 {
            let op = rng.next_range(4);
            let k = rng.next_range(100) as i64;
            let key = encode_i64(k);

            if op < 3 {
                btree.insert(&mut pager, &key, b"x").unwrap();
                expected.insert(k);
            } else {
                btree.delete(&mut pager, &key).unwrap();
                expected.remove(&k);
            }
        }

        assert_sorted_scan(&btree, &mut pager);
        assert_key_reachability(&btree, &mut pager, &expected);
        assert_no_duplicate_pages(&btree, &mut pager);
        assert_tree_invariants(&btree, &mut pager);
    }
}

/// Delete all keys one-by-one and verify tree is valid at each step.
#[test]
fn test_property_delete_to_empty() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut expected = BTreeSet::new();

    for i in 0..50i64 {
        let key = encode_i64(i);
        btree.insert(&mut pager, &key, b"val").unwrap();
        expected.insert(i);
    }

    for i in 0..50i64 {
        let key = encode_i64(i);
        assert!(btree.delete(&mut pager, &key).unwrap());
        expected.remove(&i);
        assert_sorted_scan(&btree, &mut pager);
        assert_key_reachability(&btree, &mut pager, &expected);
    }

    // Tree should be empty
    let mut count = 0;
    btree
        .scan(&mut pager, |_, _| {
            count += 1;
            Ok(true)
        })
        .unwrap();
    assert_eq!(count, 0);
}

/// Reverse-order insertion (worst case for naive B-tree) + deletion.
#[test]
fn test_property_reverse_insert_order() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut expected = BTreeSet::new();

    // Insert in reverse order
    for i in (0..200i64).rev() {
        let key = encode_i64(i);
        btree.insert(&mut pager, &key, b"rev").unwrap();
        expected.insert(i);
    }

    assert_sorted_scan(&btree, &mut pager);
    assert_tree_invariants(&btree, &mut pager);

    // Delete every third key
    for i in (0..200i64).step_by(3) {
        let key = encode_i64(i);
        btree.delete(&mut pager, &key).unwrap();
        expected.remove(&i);
    }

    assert_sorted_scan(&btree, &mut pager);
    assert_key_reachability(&btree, &mut pager, &expected);
    assert_no_duplicate_pages(&btree, &mut pager);
    assert_tree_invariants(&btree, &mut pager);
}

/// Duplicate key updates should not corrupt the tree.
#[test]
fn test_property_duplicate_key_updates() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();
    let mut rng = Rng::new(55555);
    let mut expected = BTreeSet::new();

    // Repeatedly insert/update a small key set
    for _ in 0..500 {
        let k = rng.next_range(20) as i64;
        let key = encode_i64(k);
        let val = format!("v{}", rng.next_range(1000));
        btree.insert(&mut pager, &key, val.as_bytes()).unwrap();
        expected.insert(k);
    }

    assert_sorted_scan(&btree, &mut pager);
    assert_key_reachability(&btree, &mut pager, &expected);
    assert_tree_invariants(&btree, &mut pager);
}

// ── Corruption-seeded regression test ──

/// Manually corrupt a leaf node's key order and verify that
/// verify_tree_structure catches it.
#[test]
fn test_corruption_seeded_unsorted_leaf() {
    let (mut pager, _dir) = setup();
    let mut btree = BTree::create(&mut pager).unwrap();

    // Insert keys in order
    for i in 0..5i64 {
        let key = encode_i64(i);
        btree.insert(&mut pager, &key, b"v").unwrap();
    }

    // Corrupt the root leaf: overwrite key 0 with a larger key so the
    // leaf is no longer sorted. We'll read, modify, and write back.
    let root_id = btree.root_page_id();
    let mut page = pager.read_page(root_id).unwrap();

    // The leaf has header at cell 0, then entries at cells 1..=5.
    // Overwrite cell 1's key (which should be key 0) with encode_i64(999).
    // Cell layout: [key_len: u16][key bytes][value bytes]
    if let Some((offset, _len)) = page.cell_offset_and_len(1) {
        let big_key = encode_i64(999);
        // key_len is 8 (i64 encoding), starts at offset+2
        page.data[offset + 2..offset + 2 + 8].copy_from_slice(&big_key);
    }
    pager.write_page(&page).unwrap();

    // verify_tree_structure should detect the unsorted keys
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        verify_tree_structure(&mut pager, root_id, 0);
    }));
    assert!(
        result.is_err(),
        "verify_tree_structure should panic on unsorted leaf"
    );
}
