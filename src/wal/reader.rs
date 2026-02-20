use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::crypto::aead::{MasterKey, PageCrypto};
use crate::error::{MuroError, Result};
use crate::wal::record::{crc32, Lsn, WalRecord};
use crate::wal::{MAX_WAL_FRAME_LEN, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

/// WAL reader: iterate through WAL records for recovery/snapshot.
pub struct WalReader {
    file: File,
    crypto: PageCrypto,
    current_lsn: Lsn,
    file_len: u64,
}

impl WalReader {
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        // Validate and skip WAL header if present
        if file_len >= WAL_HEADER_SIZE as u64 {
            let mut header = [0u8; WAL_HEADER_SIZE];
            file.read_exact(&mut header)?;
            if &header[0..8] == WAL_MAGIC {
                // Check version — reject WAL files from a future format
                let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
                if version > WAL_VERSION {
                    return Err(MuroError::Wal(format!(
                        "unsupported WAL format version {}",
                        version
                    )));
                }
                // Valid header - file position is now past the header
            } else {
                // Legacy WAL without header - seek back to start
                file.seek(SeekFrom::Start(0))?;
            }
        }
        // If file is smaller than header size, it's either empty or legacy

        Ok(WalReader {
            file,
            crypto: PageCrypto::new(master_key),
            current_lsn: 0,
            file_len,
        })
    }

    /// Check whether the current file position is at or near the end of the WAL.
    /// "At tail" means there are no more complete frames after the current position.
    ///
    /// This peeks at the next frame's length header (if present) and checks whether
    /// the claimed payload actually fits in the remaining file space. This catches
    /// cases where 4+ bytes remain but they don't form a complete frame.
    fn is_at_tail(&mut self) -> bool {
        let pos = self.file.stream_position().unwrap_or(self.file_len);
        let remaining = self.file_len.saturating_sub(pos);

        // Not even room for a frame length header.
        if remaining < 4 {
            return true;
        }

        // Peek at the next frame's length header to see if its payload fits.
        let mut len_buf = [0u8; 4];
        if self.file.read_exact(&mut len_buf).is_err() {
            return true;
        }
        // Seek back so we don't consume the header.
        if self.file.seek(SeekFrom::Start(pos)).is_err() {
            return true; // Seek failed - treat as tail to avoid reading from wrong position
        }

        let next_frame_len = u32::from_le_bytes(len_buf) as u64;
        // If the claimed payload doesn't fit in the remaining space, we're at tail.
        remaining < 4 + next_frame_len
    }

    /// Read the next WAL record. Returns None at end-of-file.
    ///
    /// Tolerates partial/corrupt frames only at the WAL tail (last frame position).
    /// Mid-log corruption is returned as a hard error to avoid silently dropping
    /// committed records that follow.
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
        let payload_pos = self.file.stream_position()?;
        let remaining_payload_bytes = self.file_len.saturating_sub(payload_pos);

        // Truncated tail frame: header is present but payload isn't fully written.
        if frame_len as u64 > remaining_payload_bytes {
            return Ok(None);
        }
        if frame_len == 0 {
            return Err(MuroError::Wal("WAL frame length is zero".into()));
        }
        if frame_len > MAX_WAL_FRAME_LEN {
            // If the oversized frame occupies the exact tail, tolerate it as a torn/corrupt tail.
            if frame_len as u64 == remaining_payload_bytes {
                return Ok(None);
            }
            return Err(MuroError::Wal(format!(
                "WAL frame length {} exceeds max {} at LSN {}",
                frame_len, MAX_WAL_FRAME_LEN, self.current_lsn
            )));
        }

        let mut encrypted = vec![0u8; frame_len];
        match self.file.read_exact(&mut encrypted) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Payload was truncated — this can only happen at the WAL tail
                // (crash during frame write). Safe to treat as end-of-log.
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }

        let lsn = self.current_lsn;
        let at_tail = self.is_at_tail();

        let payload = match self.crypto.decrypt(lsn, 0, &encrypted) {
            Ok(p) => p,
            Err(_) if at_tail => {
                // Corrupt frame at WAL tail: partial write before crash.
                return Ok(None);
            }
            Err(_) => {
                return Err(MuroError::Wal(format!(
                    "Failed to decrypt WAL record at LSN {} (mid-log corruption)",
                    lsn
                )));
            }
        };

        if payload.len() < 4 {
            if at_tail {
                return Ok(None);
            }
            return Err(MuroError::Wal("WAL record too short".into()));
        }

        let record_bytes = &payload[..payload.len() - 4];
        let stored_crc = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());

        if crc32(record_bytes) != stored_crc {
            if at_tail {
                return Ok(None);
            }
            return Err(MuroError::Wal(format!(
                "CRC mismatch at LSN {} (mid-log corruption)",
                lsn
            )));
        }

        let record = match WalRecord::deserialize(record_bytes) {
            Some(r) => r,
            None => {
                if at_tail {
                    return Ok(None);
                }
                return Err(MuroError::Wal(format!(
                    "Invalid record at LSN {} (mid-log corruption)",
                    lsn
                )));
            }
        };

        self.current_lsn += 1;
        Ok(Some((lsn, record)))
    }

    /// Read all records into a vector.
    pub fn read_all(&mut self) -> Result<Vec<(Lsn, WalRecord)>> {
        // Seek to start and skip header if present
        self.file.seek(SeekFrom::Start(0))?;
        self.current_lsn = 0;

        if self.file_len >= WAL_HEADER_SIZE as u64 {
            let mut header = [0u8; WAL_HEADER_SIZE];
            if self.file.read_exact(&mut header).is_ok() && &header[0..8] == WAL_MAGIC {
                // Valid header - continue reading from after header
            } else {
                // Legacy format or read failure - seek back to start
                self.file.seek(SeekFrom::Start(0))?;
            }
        }

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
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

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

    #[test]
    fn test_tail_truncation_tolerated() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write one valid record
        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer.sync().unwrap();
        }

        // Append truncated garbage at tail
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&500u32.to_le_bytes()).unwrap();
            file.write_all(&[0xDE; 5]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1); // valid Begin record recovered
    }

    /// Edge case: 4+ bytes remain after the last valid frame but they claim a payload
    /// that doesn't fit. This should be treated as tail (crash during header write),
    /// not mid-log corruption.
    #[test]
    fn test_incomplete_next_frame_treated_as_tail() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write one valid record
        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer.sync().unwrap();
        }

        // Append a frame header claiming 200 bytes but only write 10 bytes of payload.
        // Total appended = 4 (header) + 10 (partial payload) = 14 bytes, which is >= 4
        // so the old `remaining < 4` check would NOT consider this tail.
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&200u32.to_le_bytes()).unwrap();
            file.write_all(&[0xAB; 10]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1); // valid Begin recovered, incomplete frame ignored
    }

    #[test]
    fn test_mid_log_corruption_is_error() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write two valid records
        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer.append(&WalRecord::Begin { txid: 2 }).unwrap();
            writer.sync().unwrap();
        }

        // Read file to find first frame boundary, then corrupt the first frame's payload
        let file_bytes = std::fs::read(&path).unwrap();
        let hdr = WAL_HEADER_SIZE; // skip WAL header
        let first_frame_len =
            u32::from_le_bytes(file_bytes[hdr..hdr + 4].try_into().unwrap()) as usize;
        // Corrupt a byte in the first frame's encrypted payload (after the header + 4-byte length)
        let mut corrupted = file_bytes.clone();
        corrupted[hdr + 4 + first_frame_len / 2] ^= 0xFF;
        std::fs::write(&path, &corrupted).unwrap();

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let result = reader.read_all();
        // Should be a hard error because there's a valid frame after the corrupt one
        assert!(result.is_err());
    }

    #[test]
    fn test_oversized_tail_frame_tolerated() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer.sync().unwrap();
        }

        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            let oversized_len = (MAX_WAL_FRAME_LEN as u32) + 1;
            file.write_all(&oversized_len.to_le_bytes()).unwrap();
            file.write_all(&vec![0xEE; oversized_len as usize]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0].1, WalRecord::Begin { txid: 1 }));
    }
}
