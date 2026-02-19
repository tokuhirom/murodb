use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::Result;
use crate::wal::record::{crc32, Lsn, WalRecord};

/// WAL writer: append-only log with encryption.
///
/// Framing on disk:
///   [frame_len: u32 (of encrypted payload)] [encrypted payload]
///
/// Encrypted payload contains:
///   [record bytes] [crc32: u4]
pub struct WalWriter {
    file: File,
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
            crypto: PageCrypto::new(master_key),
            current_lsn: 0,
        })
    }

    pub fn open(path: &Path, master_key: &MasterKey, start_lsn: Lsn) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(WalWriter {
            file,
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

    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_write() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let key = MasterKey::new([0x42u8; 32]);
        let mut writer = WalWriter::create(&path, &key).unwrap();

        let lsn0 = writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
        assert_eq!(lsn0, 0);

        let lsn1 = writer.append(&WalRecord::PagePut {
            txid: 1,
            page_id: 5,
            data: vec![0xFF; 50],
        }).unwrap();
        assert_eq!(lsn1, 1);

        let lsn2 = writer.append(&WalRecord::Commit { txid: 1, lsn: 2 }).unwrap();
        assert_eq!(lsn2, 2);

        writer.sync().unwrap();
    }
}
