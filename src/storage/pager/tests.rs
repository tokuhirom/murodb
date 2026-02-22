use super::*;
use tempfile::NamedTempFile;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

#[test]
fn test_create_and_reopen() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp); // delete so create_new works
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        assert_eq!(pager.page_count(), 0);
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        assert_eq!(pager.page_count(), 0);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_write_and_read_pages() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();

        // Allocate and write a data page
        let mut page = pager.allocate_page().unwrap();
        page.insert_cell(b"hello world").unwrap();
        page.insert_cell(b"second cell").unwrap();
        pager.write_page(&page).unwrap();

        let mut page2 = pager.allocate_page().unwrap();
        page2.insert_cell(b"page two data").unwrap();
        pager.write_page(&page2).unwrap();

        pager.flush_meta().unwrap();
    }

    {
        let mut pager = Pager::open(&path, &test_key()).unwrap();
        assert_eq!(pager.page_count(), 2);

        let page = pager.read_page(0).unwrap();
        assert_eq!(page.cell_count(), 2);
        assert_eq!(page.cell(0), Some(b"hello world".as_slice()));
        assert_eq!(page.cell(1), Some(b"second cell".as_slice()));

        let page2 = pager.read_page(1).unwrap();
        assert_eq!(page2.cell(0), Some(b"page two data".as_slice()));
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_wrong_key_cannot_read() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        // Write at least one page so open can verify decryption
        let page = pager.allocate_page().unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let wrong_key = MasterKey::new([0x99u8; 32]);
        let result = Pager::open(&path, &wrong_key);
        assert!(result.is_err()); // decryption should fail
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    let mut pager = Pager::create(&path, &test_key()).unwrap();

    let page1 = pager.allocate_page().unwrap();
    let page1_id = page1.page_id();
    pager.write_page(&page1).unwrap();

    let page2 = pager.allocate_page().unwrap();
    pager.write_page(&page2).unwrap();

    assert_eq!(pager.page_count(), 2);

    // Free page1 and reallocate - should get the same ID
    pager.free_page(page1_id);
    let page3 = pager.allocate_page().unwrap();
    assert_eq!(page3.page_id(), page1_id);
    // page_count should not increase since we reused a free page
    assert_eq!(pager.page_count(), 2);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_cache_hit() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    let mut pager = Pager::create(&path, &test_key()).unwrap();

    let mut page = pager.allocate_page().unwrap();
    page.insert_cell(b"cached data").unwrap();
    pager.write_page(&page).unwrap();

    // Read twice - second should come from cache
    let p1 = pager.read_page(0).unwrap();
    let p2 = pager.read_page(0).unwrap();
    assert_eq!(p1.cell(0), p2.cell(0));
    assert_eq!(pager.cache_hits(), 2);
    assert_eq!(pager.cache_misses(), 0);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_cache_miss_then_hit_stats() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        let mut page = pager.allocate_page().unwrap();
        page.insert_cell(b"x").unwrap();
        pager.write_page(&page).unwrap();
        pager.flush_meta().unwrap();
    }

    let mut pager = Pager::open(&path, &test_key()).unwrap();
    let _ = pager.read_page(0).unwrap(); // miss
    let _ = pager.read_page(0).unwrap(); // hit
    assert_eq!(pager.cache_hits(), 1);
    assert_eq!(pager.cache_misses(), 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_catalog_root_persistence() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        pager.set_catalog_root(42);
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 42);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_salt_persistence() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    let salt = [0xAB; 16];
    {
        let mut pager = Pager::create_with_salt(&path, &test_key(), salt).unwrap();
        pager.flush_meta().unwrap();
    }

    {
        let read_salt = Pager::read_salt_from_file(&path).unwrap();
        assert_eq!(read_salt, salt);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_header_crc32_detects_corruption() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        pager.flush_meta().unwrap();
    }

    // Corrupt a byte in the header (e.g., catalog_root field at offset 28)
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(28)).unwrap();
        file.write_all(&[0xFF; 1]).unwrap();
    }

    let result = Pager::open(&path, &test_key());
    match result {
        Err(MuroError::Wal(msg)) => assert!(msg.contains("header corrupted")),
        Err(other) => panic!("Expected Wal error, got: {:?}", other),
        Ok(_) => panic!("Expected error, got Ok"),
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_header_crc32_valid_on_normal_open() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        pager.set_catalog_root(99);
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        assert_eq!(pager.catalog_root(), 99);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist_page_id_persistence() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        // Allocate pages 0 and 1, use page 1 as freelist page
        let _p0 = pager.allocate_page().unwrap();
        pager.write_page(&_p0).unwrap();
        let fl_page = pager.allocate_page().unwrap();
        // Write freelist data into the page (after header)
        let mut fl = Page::new(fl_page.page_id());
        let freelist_data = pager.freelist_mut().serialize();
        fl.data[crate::storage::page::PAGE_HEADER_SIZE
            ..crate::storage::page::PAGE_HEADER_SIZE + freelist_data.len()]
            .copy_from_slice(&freelist_data);
        pager.write_page(&fl).unwrap();
        pager.set_freelist_page_id(fl_page.page_id());
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        assert_eq!(pager.freelist_page_id(), 1);
    }

    std::fs::remove_file(&path).ok();
}

/// Helper: create a DB with a freelist page whose next pointer is set to `next_page_id`.
/// The freelist page is written at page 1, with page 0 as a dummy data page.
fn create_db_with_corrupt_freelist_next(path: &std::path::Path, next_page_id: u64) {
    let mut pager = Pager::create(path, &test_key()).unwrap();
    // Allocate pages 0 and 1
    let p0 = pager.allocate_page().unwrap();
    pager.write_page(&p0).unwrap();
    let fl_page = pager.allocate_page().unwrap();
    let fl_pid = fl_page.page_id(); // should be 1

    // Build a multi-page format freelist page with a corrupted next pointer
    let mut fl = Page::new(fl_pid);
    let off = crate::storage::page::PAGE_HEADER_SIZE;
    fl.data[off..off + 4].copy_from_slice(b"FLMP"); // magic
    fl.data[off + 4..off + 12].copy_from_slice(&next_page_id.to_le_bytes()); // next
    fl.data[off + 12..off + 20].copy_from_slice(&0u64.to_le_bytes()); // count = 0
    pager.write_page(&fl).unwrap();

    pager.set_freelist_page_id(fl_pid);
    pager.flush_meta().unwrap();
}

#[test]
fn test_freelist_chain_self_reference_detected() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    // Freelist page 1 points to itself (next = 1)
    create_db_with_corrupt_freelist_next(&path, 1);

    let err = match Pager::open(&path, &test_key()) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    let msg = err.to_string();
    assert!(msg.contains("cycle"), "expected cycle error, got: {msg}");

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist_chain_two_node_cycle_detected() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    let mut pager = Pager::create(&path, &test_key()).unwrap();
    let p0 = pager.allocate_page().unwrap();
    pager.write_page(&p0).unwrap();
    let p1 = pager.allocate_page().unwrap();
    let p2 = pager.allocate_page().unwrap();
    let off = crate::storage::page::PAGE_HEADER_SIZE;

    // Page 1: freelist page, next → page 2
    let mut fl1 = Page::new(p1.page_id());
    fl1.data[off..off + 4].copy_from_slice(b"FLMP");
    fl1.data[off + 4..off + 12].copy_from_slice(&p2.page_id().to_le_bytes());
    fl1.data[off + 12..off + 20].copy_from_slice(&0u64.to_le_bytes());
    pager.write_page(&fl1).unwrap();

    // Page 2: freelist page, next → page 1 (cycle back)
    let mut fl2 = Page::new(p2.page_id());
    fl2.data[off..off + 4].copy_from_slice(b"FLMP");
    fl2.data[off + 4..off + 12].copy_from_slice(&p1.page_id().to_le_bytes());
    fl2.data[off + 12..off + 20].copy_from_slice(&0u64.to_le_bytes());
    pager.write_page(&fl2).unwrap();

    pager.set_freelist_page_id(p1.page_id());
    pager.flush_meta().unwrap();
    drop(pager);

    let err = match Pager::open(&path, &test_key()) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    let msg = err.to_string();
    assert!(msg.contains("cycle"), "expected cycle error, got: {msg}");

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist_chain_next_beyond_page_count_rejected() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    // Freelist page 1 points to page 9999 which is beyond page_count (2)
    create_db_with_corrupt_freelist_next(&path, 9999);

    let err = match Pager::open(&path, &test_key()) {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    let msg = err.to_string();
    assert!(
        msg.contains("beyond page_count"),
        "expected beyond page_count error, got: {msg}"
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist_sanitize_report_observable_on_open() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    // Create a DB with a freelist containing an out-of-range entry.
    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        let p0 = pager.allocate_page().unwrap();
        pager.write_page(&p0).unwrap();
        let fl_page = pager.allocate_page().unwrap();
        let fl_pid = fl_page.page_id();

        let off = crate::storage::page::PAGE_HEADER_SIZE;
        let mut fl = Page::new(fl_pid);
        fl.data[off..off + 4].copy_from_slice(b"FLMP"); // magic
        fl.data[off + 4..off + 12].copy_from_slice(&0u64.to_le_bytes()); // next = 0
        fl.data[off + 12..off + 20].copy_from_slice(&2u64.to_le_bytes()); // count = 2
        fl.data[off + 20..off + 28].copy_from_slice(&0u64.to_le_bytes()); // page 0 (valid)
        fl.data[off + 28..off + 36].copy_from_slice(&9999u64.to_le_bytes()); // page 9999 (out-of-range)
        pager.write_page(&fl).unwrap();
        pager.set_freelist_page_id(fl_pid);
        pager.flush_meta().unwrap();
    }

    // Re-open: sanitize should remove page 9999 and report it.
    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        let report = pager
            .freelist_sanitize_report()
            .expect("expected sanitize report");
        assert_eq!(report.out_of_range, vec![9999]);
        assert!(report.duplicates.is_empty());
        assert_eq!(report.total_removed(), 1);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn test_freelist_sanitize_report_none_when_clean() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();
    drop(tmp);
    std::fs::remove_file(&path).ok();

    {
        let mut pager = Pager::create(&path, &test_key()).unwrap();
        let p0 = pager.allocate_page().unwrap();
        pager.write_page(&p0).unwrap();
        let fl_page = pager.allocate_page().unwrap();
        let fl_pid = fl_page.page_id();

        let off = crate::storage::page::PAGE_HEADER_SIZE;
        let mut fl = Page::new(fl_pid);
        fl.data[off..off + 4].copy_from_slice(b"FLMP"); // magic
        fl.data[off + 4..off + 12].copy_from_slice(&0u64.to_le_bytes()); // next = 0
        fl.data[off + 12..off + 20].copy_from_slice(&1u64.to_le_bytes()); // count = 1
        fl.data[off + 20..off + 28].copy_from_slice(&0u64.to_le_bytes()); // page 0 (valid)
        pager.write_page(&fl).unwrap();
        pager.set_freelist_page_id(fl_pid);
        pager.flush_meta().unwrap();
    }

    {
        let pager = Pager::open(&path, &test_key()).unwrap();
        assert!(
            pager.freelist_sanitize_report().is_none(),
            "expected no sanitize report for clean freelist"
        );
    }

    std::fs::remove_file(&path).ok();
}
