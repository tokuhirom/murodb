use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::wal::record::{crc32, Lsn, WalRecord};
use crate::wal::MAX_WAL_FRAME_LEN;
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
    crypto: PageCrypto,
    current_lsn: Lsn,
}

impl WalWriter {
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            crypto: PageCrypto::new(master_key),
            current_lsn: 0,
        })
    }

    pub fn open(path: &Path, master_key: &MasterKey, start_lsn: Lsn) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(WalWriter {
            file,
            path: path.to_path_buf(),
            crypto: PageCrypto::new(master_key),
            current_lsn: start_lsn,
        })
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

        let frame_len = encrypted.len() as u32;
        self.file.write_all(&frame_len.to_le_bytes())?;
        self.file.write_all(&encrypted)?;

        self.current_lsn += 1;
        Ok(lsn)
    }

    /// Sync the WAL file to disk (fsync).
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Truncate WAL to empty and reset LSN stream.
    ///
    /// Safe to call after a successful commit because data pages and metadata
    /// have already been flushed to the main database file.
    pub fn checkpoint_truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
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

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
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
        assert_eq!(writer.file_size_bytes().unwrap(), 0);
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
        assert_eq!(writer.file_size_bytes().unwrap(), 0);
    }
}
