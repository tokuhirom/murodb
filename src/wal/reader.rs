use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::wal::record::{crc32, Lsn, WalRecord};

/// WAL reader: iterate through WAL records for recovery/snapshot.
pub struct WalReader {
    file: File,
    crypto: PageCrypto,
    current_lsn: Lsn,
}

impl WalReader {
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let file = File::open(path)?;
        Ok(WalReader {
            file,
            crypto: PageCrypto::new(master_key),
            current_lsn: 0,
        })
    }

    /// Read the next WAL record. Returns None at end-of-file.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(Lsn, WalRecord)>> {
        // Read frame length
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let frame_len = u32::from_le_bytes(len_buf) as usize;

        let mut encrypted = vec![0u8; frame_len];
        self.file.read_exact(&mut encrypted)?;

        let lsn = self.current_lsn;
        let payload = self
            .crypto
            .decrypt(lsn, 0, &encrypted)
            .map_err(|_| MuroError::Wal(format!("Failed to decrypt WAL record at LSN {}", lsn)))?;

        if payload.len() < 4 {
            return Err(MuroError::Wal("WAL record too short".into()));
        }

        let record_bytes = &payload[..payload.len() - 4];
        let stored_crc = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());

        if crc32(record_bytes) != stored_crc {
            return Err(MuroError::Wal(format!("CRC mismatch at LSN {}", lsn)));
        }

        let record = WalRecord::deserialize(record_bytes)
            .ok_or_else(|| MuroError::Wal(format!("Invalid record at LSN {}", lsn)))?;

        self.current_lsn += 1;
        Ok(Some((lsn, record)))
    }

    /// Read all records into a vector.
    pub fn read_all(&mut self) -> Result<Vec<(Lsn, WalRecord)>> {
        self.file.seek(SeekFrom::Start(0))?;
        self.current_lsn = 0;

        let mut records = Vec::new();
        while let Some(record) = self.next()? {
            records.push(record);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_and_read_back() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let key = MasterKey::new([0x42u8; 32]);

        {
            let mut writer = WalWriter::create(&path, &key).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 10,
                    data: vec![0xAA; 32],
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 2 })
                .unwrap();
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::open(&path, &key).unwrap();
            let records = reader.read_all().unwrap();
            assert_eq!(records.len(), 3);

            assert!(matches!(&records[0].1, WalRecord::Begin { txid: 1 }));
            assert!(matches!(
                &records[1].1,
                WalRecord::PagePut {
                    txid: 1,
                    page_id: 10,
                    ..
                }
            ));
            assert!(matches!(
                &records[2].1,
                WalRecord::Commit { txid: 1, lsn: 2 }
            ));
        }
    }
}
