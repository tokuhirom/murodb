use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use lru::LruCache;
use std::num::NonZeroUsize;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::storage::freelist::{FreeList, SanitizeReport};
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::wal::record::crc32;

/// On-disk encrypted page size = nonce(12) + ciphertext(4096) + tag(16) = 4124
const ENCRYPTED_PAGE_SIZE: usize = PAGE_SIZE + PageCrypto::overhead();

/// Plaintext file header size (written before any encrypted pages).
/// Layout:
///   0..8    Magic "MURODB01"
///   8..12   Format version (u32 LE) — currently 3
///   12..28  Salt (16 bytes, for Argon2 KDF)
///   28..36  Catalog root page ID (u64 LE)
///   36..44  Page count (u64 LE)
///   44..52  Epoch (u64 LE)
///   52..60  Freelist page ID (u64 LE, 0 = no freelist page)
///   60..68  Next TxId (u64 LE)
///   68..72  Header CRC32 (u32 LE, over bytes 0..68)
const PLAINTEXT_HEADER_SIZE: u64 = 72;
const MAGIC: &[u8; 8] = b"MURODB01";
const FORMAT_VERSION: u32 = 3;

/// Default LRU cache capacity.
const DEFAULT_CACHE_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeaderSnapshot {
    version: u32,
    salt: [u8; 16],
    catalog_root: u64,
    page_count: u64,
    epoch: u64,
    freelist_page_id: u64,
    next_txid: u64,
}

pub struct Pager {
    file: File,
    crypto: PageCrypto,
    page_count: u64,
    epoch: u64,
    catalog_root: u64,
    salt: [u8; 16],
    freelist: FreeList,
    freelist_page_id: u64,
    next_txid: u64,
    cache: LruCache<PageId, Page>,
    cache_hits: u64,
    cache_misses: u64,
    /// Diagnostics from freelist sanitization during open.
    freelist_sanitize_report: Option<SanitizeReport>,
    #[cfg(any(test, feature = "test-utils"))]
    inject_write_page_failure: Option<std::io::ErrorKind>,
    #[cfg(any(test, feature = "test-utils"))]
    inject_flush_meta_failure: Option<std::io::ErrorKind>,
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
            next_txid: 1,
            cache,
            cache_hits: 0,
            cache_misses: 0,
            freelist_sanitize_report: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_write_page_failure: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_flush_meta_failure: None,
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
            next_txid: 1,
            cache,
            cache_hits: 0,
            cache_misses: 0,
            freelist_sanitize_report: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_write_page_failure: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_flush_meta_failure: None,
        };

        pager.read_plaintext_header()?;

        // Verify that decryption works by reading page 0 if there are pages
        if pager.page_count > 0 {
            let _page0 = pager.read_page_from_disk(0)?;
        }

        pager.reload_freelist_from_disk()?;

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
        header[60..68].copy_from_slice(&self.next_txid.to_le_bytes());
        // CRC32 over bytes 0..68
        let checksum = crc32(&header[0..68]);
        header[68..72].copy_from_slice(&checksum.to_le_bytes());

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        Ok(())
    }

    /// Read the plaintext file header.
    fn read_plaintext_header(&mut self) -> Result<()> {
        let snapshot = self.read_plaintext_header_snapshot()?;
        self.apply_header_snapshot(snapshot);

        // Auto-upgrade v1/v2 → v3: rewrite header with new format
        if snapshot.version < FORMAT_VERSION {
            self.write_plaintext_header()?;
            self.file.sync_all()?;
        }

        Ok(())
    }

    fn read_plaintext_header_snapshot(&mut self) -> Result<HeaderSnapshot> {
        // Read max possible header size; older versions have smaller headers.
        let mut header = [0u8; PLAINTEXT_HEADER_SIZE as usize];
        self.file.seek(SeekFrom::Start(0))?;
        // Read at least old header size (64 bytes), tolerating shorter files for v1
        let bytes_read = {
            let mut total = 0;
            loop {
                match self.file.read(&mut header[total..]) {
                    Ok(0) => break,
                    Ok(n) => total += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            total
        };
        if bytes_read < 64 {
            return Err(MuroError::InvalidPage);
        }
        if &header[0..8] != MAGIC {
            return Err(MuroError::InvalidPage);
        }

        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if version > FORMAT_VERSION {
            return Err(MuroError::Wal(format!(
                "unsupported database format version {}",
                version
            )));
        }

        let mut salt = [0u8; 16];
        salt.copy_from_slice(&header[12..28]);
        let catalog_root = u64::from_le_bytes(header[28..36].try_into().unwrap());
        let page_count = u64::from_le_bytes(header[36..44].try_into().unwrap());
        let epoch = u64::from_le_bytes(header[44..52].try_into().unwrap());
        let freelist_page_id = u64::from_le_bytes(header[52..60].try_into().unwrap());

        let next_txid = match version {
            1 => 1, // v1 has no header CRC/next_txid field.
            2 => {
                let stored_crc = u32::from_le_bytes(header[60..64].try_into().unwrap());
                let computed_crc = crc32(&header[0..60]);
                if stored_crc != computed_crc {
                    return Err(MuroError::Wal("header corrupted".into()));
                }
                1
            }
            _ => {
                let stored_crc = u32::from_le_bytes(header[68..72].try_into().unwrap());
                let computed_crc = crc32(&header[0..68]);
                if stored_crc != computed_crc {
                    return Err(MuroError::Wal("header corrupted".into()));
                }
                u64::from_le_bytes(header[60..68].try_into().unwrap())
            }
        };

        Ok(HeaderSnapshot {
            version,
            salt,
            catalog_root,
            page_count,
            epoch,
            freelist_page_id,
            next_txid,
        })
    }

    fn apply_header_snapshot(&mut self, snapshot: HeaderSnapshot) {
        self.salt = snapshot.salt;
        self.catalog_root = snapshot.catalog_root;
        self.page_count = snapshot.page_count;
        self.epoch = snapshot.epoch;
        self.freelist_page_id = snapshot.freelist_page_id;
        self.next_txid = snapshot.next_txid;
    }

    fn reload_freelist_from_disk(&mut self) -> Result<()> {
        self.freelist = FreeList::new();
        self.freelist_sanitize_report = None;

        if self.freelist_page_id == 0 {
            return Ok(());
        }

        let first_page = self.read_page_from_disk(self.freelist_page_id)?;
        let data_area = &first_page.as_bytes()[crate::storage::page::PAGE_HEADER_SIZE..];

        if FreeList::is_multi_page_format(data_area) {
            // Multi-page chain: walk the chain with cycle detection
            let mut visited = std::collections::HashSet::new();
            visited.insert(self.freelist_page_id);
            let mut pages_data_owned: Vec<Vec<u8>> = Vec::new();
            pages_data_owned.push(data_area.to_vec());
            // Read next pointer from first page (offset 4, after 4-byte magic)
            let mut next_page_id = u64::from_le_bytes(data_area[4..12].try_into().unwrap());
            while next_page_id != 0 {
                if !visited.insert(next_page_id) {
                    return Err(MuroError::Corruption(format!(
                        "freelist chain cycle detected at page {}",
                        next_page_id
                    )));
                }
                if next_page_id >= self.page_count {
                    return Err(MuroError::Corruption(format!(
                        "freelist chain references page {} beyond page_count {}",
                        next_page_id, self.page_count
                    )));
                }
                let next_page = self.read_page_from_disk(next_page_id)?;
                let next_data = &next_page.as_bytes()[crate::storage::page::PAGE_HEADER_SIZE..];
                next_page_id = u64::from_le_bytes(next_data[4..12].try_into().unwrap());
                pages_data_owned.push(next_data.to_vec());
            }
            let pages_refs: Vec<&[u8]> = pages_data_owned.iter().map(|v| v.as_slice()).collect();
            self.freelist = FreeList::deserialize_pages(&pages_refs);
        } else {
            // Legacy single-page format
            self.freelist = FreeList::deserialize(data_area);
        }

        // Remove out-of-range and duplicated freelist entries.
        let report = self.freelist.sanitize(self.page_count);
        if !report.is_clean() {
            self.freelist_sanitize_report = Some(report);
        }

        Ok(())
    }

    /// Refresh in-memory metadata and page cache if another process committed changes.
    ///
    /// Returns `Ok(true)` when metadata changed and local cache was invalidated.
    pub fn refresh_from_disk_if_changed(&mut self) -> Result<bool> {
        let snapshot = self.read_plaintext_header_snapshot()?;
        if snapshot.salt != self.salt {
            return Err(MuroError::Corruption(
                "database salt changed unexpectedly".into(),
            ));
        }

        let changed = snapshot.catalog_root != self.catalog_root
            || snapshot.page_count != self.page_count
            || snapshot.epoch != self.epoch
            || snapshot.freelist_page_id != self.freelist_page_id
            || snapshot.next_txid != self.next_txid;
        if !changed {
            return Ok(false);
        }

        self.apply_header_snapshot(snapshot);
        self.cache.clear();
        self.reload_freelist_from_disk()?;
        Ok(true)
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
            self.cache_hits = self.cache_hits.saturating_add(1);
            return Ok(page.clone());
        }
        self.cache_misses = self.cache_misses.saturating_add(1);

        let page = self.read_page_from_disk(page_id)?;
        self.cache.put(page_id, page.clone());
        Ok(page)
    }

    /// Write a page (to cache and disk).
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(kind) = self.inject_write_page_failure {
            return Err(MuroError::Io(std::io::Error::new(
                kind,
                "injected write_page failure",
            )));
        }
        self.write_page_to_disk(page)?;
        self.cache.put(page.page_id(), page.clone());
        Ok(())
    }

    /// Read an encrypted page from disk and decrypt it.
    fn read_page_from_disk(&mut self, page_id: PageId) -> Result<Page> {
        let offset = PLAINTEXT_HEADER_SIZE + page_id * ENCRYPTED_PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut encrypted = [0u8; ENCRYPTED_PAGE_SIZE];
        self.file.read_exact(&mut encrypted)?;

        let mut plaintext = [0u8; PAGE_SIZE];
        let plaintext_len =
            self.crypto
                .decrypt_into(page_id, self.epoch, &encrypted, &mut plaintext)?;

        if plaintext_len != PAGE_SIZE {
            return Err(MuroError::InvalidPage);
        }

        Ok(Page::from_bytes(plaintext))
    }

    /// Encrypt a page and write it to disk.
    fn write_page_to_disk(&mut self, page: &Page) -> Result<()> {
        let page_id = page.page_id();
        let mut encrypted = [0u8; ENCRYPTED_PAGE_SIZE];
        let written =
            self.crypto
                .encrypt_into(page_id, self.epoch, page.as_bytes(), &mut encrypted)?;
        if written != ENCRYPTED_PAGE_SIZE {
            return Err(MuroError::Encryption(
                "unexpected encrypted page size".to_string(),
            ));
        }

        let offset = PLAINTEXT_HEADER_SIZE + page_id * ENCRYPTED_PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&encrypted)?;
        Ok(())
    }

    /// Flush the plaintext header with current state.
    pub fn flush_meta(&mut self) -> Result<()> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(kind) = self.inject_flush_meta_failure {
            return Err(MuroError::Io(std::io::Error::new(
                kind,
                "injected flush_meta failure",
            )));
        }
        self.write_plaintext_header()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Get current page count.
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    /// Cache hit count since pager open/create.
    pub fn cache_hits(&self) -> u64 {
        self.cache_hits
    }

    /// Cache miss count since pager open/create.
    pub fn cache_misses(&self) -> u64 {
        self.cache_misses
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

    /// Returns the freelist sanitization report if entries were removed during open.
    /// `None` means no sanitization was needed (clean freelist).
    pub fn freelist_sanitize_report(&self) -> Option<&SanitizeReport> {
        self.freelist_sanitize_report.as_ref()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_inject_write_page_failure(&mut self, kind: Option<std::io::ErrorKind>) {
        self.inject_write_page_failure = kind;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_inject_flush_meta_failure(&mut self, kind: Option<std::io::ErrorKind>) {
        self.inject_flush_meta_failure = kind;
    }

    /// Get the next transaction ID.
    pub fn next_txid(&self) -> u64 {
        self.next_txid
    }

    /// Set the next transaction ID.
    pub fn set_next_txid(&mut self, txid: u64) {
        self.next_txid = txid;
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
}
