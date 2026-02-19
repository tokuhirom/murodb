use murodb::btree::key_encoding::{decode_i64, encode_i64};
use murodb::btree::ops::BTree;
use murodb::crypto::aead::MasterKey;
use murodb::storage::pager::Pager;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

#[test]
fn test_btree_large_dataset() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut btree = BTree::create(&mut pager).unwrap();

    let count = 1000;

    // Insert 1000 entries in reverse order (worst case for splits)
    for i in (0..count).rev() {
        let key = encode_i64(i);
        let value = format!("value_{:04}", i);
        btree.insert(&mut pager, &key, value.as_bytes()).unwrap();
    }

    // Verify all entries
    for i in 0..count {
        let key = encode_i64(i);
        let expected = format!("value_{:04}", i);
        let result = btree.search(&mut pager, &key).unwrap();
        assert_eq!(result, Some(expected.into_bytes()), "Failed at key {}", i);
    }

    // Verify sort order via scan
    let mut last_key: Option<Vec<u8>> = None;
    let mut scan_count = 0;
    btree.scan(&mut pager, |k, _v| {
        if let Some(ref lk) = last_key {
            assert!(k > lk.as_slice(), "Keys not in order");
        }
        last_key = Some(k.to_vec());
        scan_count += 1;
        Ok(true)
    }).unwrap();
    assert_eq!(scan_count, count);
}

#[test]
fn test_btree_string_keys() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let mut btree = BTree::create(&mut pager).unwrap();

    let words = vec!["zebra", "apple", "mango", "banana", "cherry"];
    for word in &words {
        btree.insert(&mut pager, word.as_bytes(), b"data").unwrap();
    }

    // Verify sorted scan
    let mut results = Vec::new();
    btree.scan(&mut pager, |k, _| {
        results.push(String::from_utf8(k.to_vec()).unwrap());
        Ok(true)
    }).unwrap();

    assert_eq!(results, vec!["apple", "banana", "cherry", "mango", "zebra"]);
}

#[test]
fn test_i64_key_encoding_comprehensive() {
    let test_values = vec![
        i64::MIN,
        i64::MIN + 1,
        -1_000_000,
        -1,
        0,
        1,
        1_000_000,
        i64::MAX - 1,
        i64::MAX,
    ];

    for &val in &test_values {
        let encoded = encode_i64(val);
        let decoded = decode_i64(&encoded);
        assert_eq!(val, decoded, "Roundtrip failed for {}", val);
    }

    // Verify ordering
    for pair in test_values.windows(2) {
        let enc_a = encode_i64(pair[0]);
        let enc_b = encode_i64(pair[1]);
        assert!(enc_a < enc_b, "{} should sort before {}", pair[0], pair[1]);
    }
}
