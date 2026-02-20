use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use lru::LruCache;
use std::num::NonZeroUsize;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::storage::freelist::FreeList;
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::wal::record::crc32;

/// On-disk encrypted page size = nonce(12) + ciphertext(4096) + tag(16) = 4124
const ENCRYPTED_PAGE_SIZE: usize = PAGE_SIZE + PageCrypto::overhead();

/// Plaintext file header size (written before any encrypted pages).
/// Layout:
///   0..8    Magic "MURODB01"
///   8..12   Format version (u32 LE) — currently 2
///   12..28  Salt (16 bytes, for Argon2 KDF)
///   28..36  Catalog root page ID (u64 LE)
///   36..44  Page count (u64 LE)
///   44..52  Epoch (u64 LE)
///   52..60  Freelist page ID (u64 LE, 0 = no freelist page)
///   60..64  Header CRC32 (u32 LE, over bytes 0..60)
const PLAINTEXT_HEADER_SIZE: u64 = 64;
const MAGIC: &[u8; 8] = b"MURODB01";
const FORMAT_VERSION: u32 = 2;

/// Default LRU cache capacity.
const DEFAULT_CACHE_CAPACITY: usize = 256;

pub struct Pager {
    file: File,
    crypto: PageCrypto,
    page_count: u64,
    epoch: u64,
    catalog_root: u64,
    salt: [u8; 16],
    freelist: FreeList,
    freelist_page_id: u64,
    cache: LruCache<PageId, Page>,
}

impl Pager {
    /// Create a new database file with the given salt.
    pub fn create_with_salt(path: &Path, master_key: &MasterKey, salt: [u8; 16]) -> Result<Self> {
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
            page_count: 0,
            epoch: 0,
            catalog_root: 0,
            salt,
            freelist: FreeList::new(),
            freelist_page_id: 0,
            cache,
        };

        // Write the plaintext header
        pager.write_plaintext_header()?;

        Ok(pager)
    }

    /// Create a new database file (legacy API, generates a zero salt).
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        Self::create_with_salt(path, master_key, [0u8; 16])
    }

    /// Open an existing database file.
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        let crypto = PageCrypto::new(master_key);
        let cache = LruCache::new(NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap());

        let mut pager = Pager {
            file,
            crypto,
            page_count: 0,
            epoch: 0,
            catalog_root: 0,
            salt: [0u8; 16],
            freelist: FreeList::new(),
            freelist_page_id: 0,
            cache,
        };

        pager.read_plaintext_header()?;

        // Verify that decryption works by reading page 0 if there are pages
        if pager.page_count > 0 {
            let _page0 = pager.read_page_from_disk(0)?;
        }

        // Load persisted freelist if present
        if pager.freelist_page_id != 0 {
            let fl_page = pager.read_page_from_disk(pager.freelist_page_id)?;
            pager.freelist = FreeList::deserialize(
                &fl_page.as_bytes()[crate::storage::page::PAGE_HEADER_SIZE..],
            );
        }

        Ok(pager)
    }

    /// Read the plaintext header from the file to verify magic and extract salt.
    /// This does NOT require a master key and can be called on a raw file.
    pub fn read_salt_from_file(path: &Path) -> Result<[u8; 16]> {
        let mut file = File::open(path)?;
        let mut header = [0u8; PLAINTEXT_HEADER_SIZE as usize];
        file.read_exact(&mut header)?;

        if &header[0..8] != MAGIC {
            return Err(MuroError::InvalidPage);
        }

        let mut salt = [0u8; 16];
        salt.copy_from_slice(&header[12..28]);
        Ok(salt)
    }

    /// Write the plaintext file header.
    fn write_plaintext_header(&mut self) -> Result<()> {
        let mut header = [0u8; PLAINTEXT_HEADER_SIZE as usize];
        header[0..8].copy_from_slice(MAGIC);
        header[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        header[12..28].copy_from_slice(&self.salt);
        header[28..36].copy_from_slice(&self.catalog_root.to_le_bytes());
        header[36..44].copy_from_slice(&self.page_count.to_le_bytes());
        header[44..52].copy_from_slice(&self.epoch.to_le_bytes());
        header[52..60].copy_from_slice(&self.freelist_page_id.to_le_bytes());
        // CRC32 over bytes 0..60
        let checksum = crc32(&header[0..60]);
        header[60..64].copy_from_slice(&checksum.to_le_bytes());

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        Ok(())
    }

    /// Read the plaintext file header.
    fn read_plaintext_header(&mut self) -> Result<()> {
        let mut header = [0u8; PLAINTEXT_HEADER_SIZE as usize];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut header)?;

        if &header[0..8] != MAGIC {
            return Err(MuroError::InvalidPage);
        }

        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        self.salt.copy_from_slice(&header[12..28]);
        self.catalog_root = u64::from_le_bytes(header[28..36].try_into().unwrap());
        self.page_count = u64::from_le_bytes(header[36..44].try_into().unwrap());
        self.epoch = u64::from_le_bytes(header[44..52].try_into().unwrap());

        if version >= 2 {
            self.freelist_page_id = u64::from_le_bytes(header[52..60].try_into().unwrap());
            let stored_crc = u32::from_le_bytes(header[60..64].try_into().unwrap());
            let computed_crc = crc32(&header[0..60]);
            if stored_crc != computed_crc {
                return Err(MuroError::Wal("header corrupted".into()));
            }
        }

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
        let offset = PLAINTEXT_HEADER_SIZE + page_id * ENCRYPTED_PAGE_SIZE as u64;
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

        let offset = PLAINTEXT_HEADER_SIZE + page_id * ENCRYPTED_PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&encrypted)?;
        Ok(())
    }

    /// Flush the plaintext header with current state.
    pub fn flush_meta(&mut self) -> Result<()> {
        self.write_plaintext_header()?;
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

    /// Get catalog root page ID.
    pub fn catalog_root(&self) -> u64 {
        self.catalog_root
    }

    /// Set catalog root page ID.
    pub fn set_catalog_root(&mut self, root: u64) {
        self.catalog_root = root;
    }

    /// Set page count (used by WAL recovery to restore metadata).
    pub fn set_page_count(&mut self, count: u64) {
        self.page_count = count;
    }

    /// Get salt.
    pub fn salt(&self) -> &[u8; 16] {
        &self.salt
    }

    /// Get freelist page ID (0 = no persisted freelist).
    pub fn freelist_page_id(&self) -> u64 {
        self.freelist_page_id
    }

    /// Set freelist page ID.
    pub fn set_freelist_page_id(&mut self, page_id: u64) {
        self.freelist_page_id = page_id;
    }

    /// Get mutable reference to the in-memory freelist.
    pub fn freelist_mut(&mut self) -> &mut FreeList {
        &mut self.freelist
    }

    /// Sync file to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

impl crate::storage::page_store::PageStore for Pager {
    fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        Pager::read_page(self, page_id)
    }

    fn write_page(&mut self, page: &Page) -> Result<()> {
        Pager::write_page(self, page)
    }

    fn allocate_page(&mut self) -> Result<Page> {
        Pager::allocate_page(self)
    }

    fn free_page(&mut self, page_id: PageId) {
        Pager::free_page(self, page_id)
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
}
