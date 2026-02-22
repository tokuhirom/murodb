use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::crypto::aead::MasterKey;
use crate::crypto::suite::{EncryptionSuite, PageCipher};
use crate::error::{MuroError, Result};
use crate::wal::record::{crc32, Lsn, WalRecord};
use crate::wal::{MAX_WAL_FRAME_LEN, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};
/// WAL writer: append-only log with encryption.
///
/// Framing on disk:
///   [frame_len: u32 (of encrypted payload)] [encrypted payload]
///
/// Encrypted payload contains:
///   [record bytes] [crc32: u4]
pub struct WalWriter {
    file: File,
    path: PathBuf,
    crypto: PageCipher,
    current_lsn: Lsn,
    #[cfg(test)]
    inject_write_failure: Option<std::io::ErrorKind>,
    #[cfg(test)]
    inject_sync_failure: Option<std::io::ErrorKind>,
    #[cfg(any(test, feature = "test-utils"))]
    inject_checkpoint_truncate_failure: Option<std::io::ErrorKind>,
}

impl WalWriter {
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        Self::create_with_suite(path, EncryptionSuite::Aes256GcmSiv, Some(master_key))
    }

    pub fn create_plaintext(path: &Path) -> Result<Self> {
        Self::create_with_suite(path, EncryptionSuite::Plaintext, None)
    }

    pub fn create_with_suite(
        path: &Path,
        suite: EncryptionSuite,
        master_key: Option<&MasterKey>,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // Write WAL header
        Self::write_wal_header(&mut file)?;

        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            crypto: PageCipher::new(suite, master_key)?,
            current_lsn: 0,
            #[cfg(test)]
            inject_write_failure: None,
            #[cfg(test)]
            inject_sync_failure: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_checkpoint_truncate_failure: None,
        })
    }

    pub fn open(path: &Path, master_key: &MasterKey, start_lsn: Lsn) -> Result<Self> {
        Self::open_with_suite(
            path,
            EncryptionSuite::Aes256GcmSiv,
            Some(master_key),
            start_lsn,
        )
    }

    pub fn open_plaintext(path: &Path, start_lsn: Lsn) -> Result<Self> {
        Self::open_with_suite(path, EncryptionSuite::Plaintext, None, start_lsn)
    }

    pub fn open_with_suite(
        path: &Path,
        suite: EncryptionSuite,
        master_key: Option<&MasterKey>,
        start_lsn: Lsn,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let file_len = file.metadata()?.len();
        if file_len == 0 {
            // Empty file: write header
            Self::write_wal_header(&mut file)?;
        } else if file_len >= WAL_HEADER_SIZE as u64 {
            // Validate existing header
            Self::validate_wal_header(&mut file)?;
            file.seek(SeekFrom::End(0))?;
        } else {
            // Non-empty but shorter than the WAL header — the file is corrupt.
            return Err(MuroError::Wal(format!(
                "WAL file is corrupt: size {} is smaller than the required header size {}",
                file_len, WAL_HEADER_SIZE
            )));
        }

        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            crypto: PageCipher::new(suite, master_key)?,
            current_lsn: start_lsn,
            #[cfg(test)]
            inject_write_failure: None,
            #[cfg(test)]
            inject_sync_failure: None,
            #[cfg(any(test, feature = "test-utils"))]
            inject_checkpoint_truncate_failure: None,
        })
    }

    fn write_wal_header(file: &mut File) -> Result<()> {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..8].copy_from_slice(WAL_MAGIC);
        header[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
        file.write_all(&header)?;
        Ok(())
    }

    fn validate_wal_header(file: &mut File) -> Result<()> {
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;
        if &header[0..8] != WAL_MAGIC {
            return Err(MuroError::Wal(
                "WAL file magic mismatch: not a valid MuroDB WAL file".into(),
            ));
        }
        let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if version > WAL_VERSION {
            return Err(MuroError::Wal(format!(
                "unsupported WAL format version {}",
                version
            )));
        }
        Ok(())
    }

    /// Append a WAL record. Returns the LSN assigned.
    pub fn append(&mut self, record: &WalRecord) -> Result<Lsn> {
        let lsn = self.current_lsn;

        let record_bytes = record.serialize();
        let crc = crc32(&record_bytes);

        let mut payload = record_bytes;
        payload.extend_from_slice(&crc.to_le_bytes());

        // Encrypt with LSN as "page_id" and 0 as epoch
        let encrypted = self.crypto.encrypt(lsn, 0, &payload)?;
        if encrypted.len() > MAX_WAL_FRAME_LEN {
            return Err(MuroError::Wal(format!(
                "WAL frame length {} exceeds max {}",
                encrypted.len(),
                MAX_WAL_FRAME_LEN
            )));
        }

        #[cfg(test)]
        if let Some(kind) = self.inject_write_failure {
            return Err(MuroError::Io(std::io::Error::new(
                kind,
                "injected write failure",
            )));
        }

        let frame_len = encrypted.len() as u32;
        self.file.write_all(&frame_len.to_le_bytes())?;
        self.file.write_all(&encrypted)?;

        self.current_lsn += 1;
        Ok(lsn)
    }

    /// Sync the WAL file to disk (fsync).
    pub fn sync(&mut self) -> Result<()> {
        #[cfg(test)]
        if let Some(kind) = self.inject_sync_failure {
            return Err(MuroError::Io(std::io::Error::new(
                kind,
                "injected sync failure",
            )));
        }
        self.file.sync_all()?;
        Ok(())
    }

    /// Truncate WAL to just the header and reset LSN stream.
    ///
    /// Safe to call after a successful commit because data pages and metadata
    /// have already been flushed to the main database file.
    ///
    /// ## Durability
    ///
    /// After `set_len()`, `sync_all()` is called to ensure the truncated state
    /// reaches stable storage. A best-effort directory fsync follows to harden
    /// the metadata change. If the process crashes before `sync_all()` completes,
    /// the old WAL may still be present and will be replayed idempotently on
    /// next open.
    pub fn checkpoint_truncate(&mut self) -> Result<()> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(kind) = self.inject_checkpoint_truncate_failure {
            return Err(MuroError::Io(std::io::Error::new(
                kind,
                "injected checkpoint_truncate failure",
            )));
        }
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        self.file.sync_all()?;
        // Best-effort parent directory fsync to harden metadata persistence.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        self.current_lsn = 0;
        Ok(())
    }

    /// Current WAL file size in bytes.
    pub fn file_size_bytes(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn wal_path(&self) -> &Path {
        &self.path
    }

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    #[cfg(test)]
    pub fn set_inject_write_failure(&mut self, kind: Option<std::io::ErrorKind>) {
        self.inject_write_failure = kind;
    }

    #[cfg(test)]
    pub fn set_inject_sync_failure(&mut self, kind: Option<std::io::ErrorKind>) {
        self.inject_sync_failure = kind;
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_inject_checkpoint_truncate_failure(&mut self, kind: Option<std::io::ErrorKind>) {
        self.inject_checkpoint_truncate_failure = kind;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MuroError;
    use crate::storage::page::PAGE_SIZE;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_write() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let key = MasterKey::new([0x42u8; 32]);
        let mut writer = WalWriter::create(&path, &key).unwrap();

        let lsn0 = writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        assert_eq!(lsn0, 0);

        let lsn1 = writer
            .append(&WalRecord::PagePut {
                txid: 1,
                page_id: 5,
                data: vec![0xFF; 50],
            })
            .unwrap();
        assert_eq!(lsn1, 1);

        let lsn2 = writer
            .append(&WalRecord::Commit { txid: 1, lsn: 2 })
            .unwrap();
        assert_eq!(lsn2, 2);

        writer.sync().unwrap();
    }

    #[test]
    fn test_checkpoint_truncate_resets_wal_and_lsn() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let key = MasterKey::new([0x42u8; 32]);
        let mut writer = WalWriter::create(&path, &key).unwrap();
        writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        writer.sync().unwrap();
        assert!(writer.file_size_bytes().unwrap() > 0);

        writer.checkpoint_truncate().unwrap();
        assert_eq!(writer.file_size_bytes().unwrap(), WAL_HEADER_SIZE as u64);
        assert_eq!(writer.current_lsn(), 0);

        let lsn = writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_append_rejects_oversized_frame_without_advancing_lsn() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let key = MasterKey::new([0x42u8; 32]);
        let mut writer = WalWriter::create(&path, &key).unwrap();
        let res = writer.append(&WalRecord::PagePut {
            txid: 1,
            page_id: 0,
            data: vec![0xAB; PAGE_SIZE * 2],
        });

        assert!(matches!(res, Err(MuroError::Wal(_))));
        assert_eq!(writer.current_lsn(), 0);
        assert_eq!(writer.file_size_bytes().unwrap(), WAL_HEADER_SIZE as u64);
    }

    #[test]
    fn test_open_rejects_truncated_header() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write a few bytes — less than WAL_HEADER_SIZE (12 bytes)
        std::fs::write(&path, &[0xAA; 5]).unwrap();

        let key = MasterKey::new([0x42u8; 32]);
        let res = WalWriter::open(&path, &key, 0);
        assert!(matches!(res, Err(MuroError::Wal(ref msg)) if msg.contains("corrupt")));
    }
}
