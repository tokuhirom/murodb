use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use lru::LruCache;
use std::num::NonZeroUsize;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::storage::freelist::FreeList;
use crate::storage::page::{Page, PageId, PAGE_SIZE};

/// On-disk encrypted page size = nonce(12) + ciphertext(4096) + tag(16) = 4124
const ENCRYPTED_PAGE_SIZE: usize = PAGE_SIZE + PageCrypto::overhead();

/// DB file header stored in page 0 (first 64 bytes of plaintext page 0).
/// Magic(8) + version(4) + page_count(8) + freelist_page(8) + epoch(8) + salt(16) + reserved
const MAGIC: &[u8; 8] = b"MURODB01";
const DB_HEADER_SIZE: usize = 64;

/// Default LRU cache capacity.
const DEFAULT_CACHE_CAPACITY: usize = 256;

pub struct Pager {
    file: File,
    crypto: PageCrypto,
    page_count: u64,
    epoch: u64,
    freelist: FreeList,
    cache: LruCache<PageId, Page>,
}

impl Pager {
    /// Create a new database file.
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        let crypto = PageCrypto::new(master_key);
        let cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());

        let mut pager = Pager {
            file,
            crypto,
            page_count: 1, // page 0 is the meta page
            epoch: 0,
            freelist: FreeList::new(),
            cache,
        };

        // Write page 0 (meta/header page)
        let mut meta_page = Page::new(0);
        pager.write_db_header(&mut meta_page);
        pager.write_page_to_disk(&meta_page)?;

        Ok(pager)
    }

    /// Open an existing database file.
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let crypto = PageCrypto::new(master_key);
        let cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());

        let mut pager = Pager {
            file,
            crypto,
            page_count: 0,
            epoch: 0,
            freelist: FreeList::new(),
            cache,
        };

        // Read meta page to get DB header
        let meta_page = pager.read_page_from_disk(0)?;
        pager.read_db_header(&meta_page)?;

        Ok(pager)
    }

    /// Write DB header into page 0's data area.
    fn write_db_header(&self, page: &mut Page) {
        let mut header = [0u8; DB_HEADER_SIZE];
        header[0..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&1u32.to_le_bytes()); // version
        header[12..20].copy_from_slice(&self.page_count.to_le_bytes());
        header[20..28].copy_from_slice(&self.epoch.to_le_bytes());
        // Bytes 28..64 reserved

        // Store header as a cell in page 0
        // Clear the page first and write header as first cell
        *page = Page::new(0);
        page.insert_cell(&header).expect("header fits in page");
    }

    /// Read DB header from page 0.
    fn read_db_header(&mut self, page: &Page) -> Result<()> {
        let header = page.cell(0).ok_or_else(|| {
            MuroError::InvalidPage
        })?;

        if header.len() < DB_HEADER_SIZE {
            return Err(MuroError::InvalidPage);
        }

        if &header[0..8] != MAGIC {
            return Err(MuroError::InvalidPage);
        }

        let _version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        self.page_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
        self.epoch = u64::from_le_bytes(header[20..28].try_into().unwrap());

        Ok(())
    }

    /// Allocate a new page. Returns a fresh Page with the assigned page_id.
    pub fn allocate_page(&mut self) -> Result<Page> {
        let page_id = if let Some(free_id) = self.freelist.allocate() {
            free_id
        } else {
            let id = self.page_count;
            self.page_count += 1;
            id
        };

        let page = Page::new(page_id);
        Ok(page)
    }

    /// Free a page, returning it to the freelist.
    pub fn free_page(&mut self, page_id: PageId) {
        self.cache.pop(&page_id);
        self.freelist.free(page_id);
    }

    /// Read a page (from cache or disk).
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if let Some(page) = self.cache.get(&page_id) {
            return Ok(page.clone());
        }

        let page = self.read_page_from_disk(page_id)?;
        self.cache.put(page_id, page.clone());
        Ok(page)
    }

    /// Write a page (to cache and disk).
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        self.write_page_to_disk(page)?;
        self.cache.put(page.page_id(), page.clone());
        Ok(())
    }

    /// Read an encrypted page from disk and decrypt it.
    fn read_page_from_disk(&mut self, page_id: PageId) -> Result<Page> {
        let offset = page_id as u64 * ENCRYPTED_PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut encrypted = vec![0u8; ENCRYPTED_PAGE_SIZE];
        self.file.read_exact(&mut encrypted)?;

        let plaintext = self.crypto.decrypt(page_id, self.epoch, &encrypted)?;

        if plaintext.len() != PAGE_SIZE {
            return Err(MuroError::InvalidPage);
        }

        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(&plaintext);
        Ok(Page::from_bytes(data))
    }

    /// Encrypt a page and write it to disk.
    fn write_page_to_disk(&mut self, page: &Page) -> Result<()> {
        let page_id = page.page_id();
        let encrypted = self.crypto.encrypt(page_id, self.epoch, page.as_bytes())?;

        let offset = page_id as u64 * ENCRYPTED_PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&encrypted)?;
        Ok(())
    }

    /// Flush the meta page (page 0) with current state.
    pub fn flush_meta(&mut self) -> Result<()> {
        let mut meta_page = Page::new(0);
        self.write_db_header(&mut meta_page);
        self.write_page_to_disk(&meta_page)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Get current page count.
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    /// Get current epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Sync file to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
            let pager = Pager::create(&path, &test_key()).unwrap();
            assert_eq!(pager.page_count(), 1); // meta page
        }

        {
            let pager = Pager::open(&path, &test_key()).unwrap();
            assert_eq!(pager.page_count(), 1);
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
            assert_eq!(pager.page_count(), 3); // meta + 2 data pages

            let page = pager.read_page(1).unwrap();
            assert_eq!(page.cell_count(), 2);
            assert_eq!(page.cell(0), Some(b"hello world".as_slice()));
            assert_eq!(page.cell(1), Some(b"second cell".as_slice()));

            let page2 = pager.read_page(2).unwrap();
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
            let _pager = Pager::create(&path, &test_key()).unwrap();
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

        assert_eq!(pager.page_count(), 3);

        // Free page1 and reallocate - should get the same ID
        pager.free_page(page1_id);
        let page3 = pager.allocate_page().unwrap();
        assert_eq!(page3.page_id(), page1_id);
        // page_count should not increase since we reused a free page
        assert_eq!(pager.page_count(), 3);

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
        let p1 = pager.read_page(1).unwrap();
        let p2 = pager.read_page(1).unwrap();
        assert_eq!(p1.cell(0), p2.cell(0));

        std::fs::remove_file(&path).ok();
    }
}
