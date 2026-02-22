use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::crypto::aead::MasterKey;
use crate::crypto::suite::{EncryptionSuite, PageCipher};
use crate::error::{MuroError, Result};
use crate::wal::record::{crc32, Lsn, WalRecord};
use crate::wal::{MAX_WAL_FRAME_LEN, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

/// WAL reader: iterate through WAL records for recovery/snapshot.
pub struct WalReader {
    file: File,
    crypto: PageCipher,
    current_lsn: Lsn,
    file_len: u64,
}

impl WalReader {
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        Self::open_with_suite(path, EncryptionSuite::Aes256GcmSiv, Some(master_key))
    }

    pub fn open_plaintext(path: &Path) -> Result<Self> {
        Self::open_with_suite(path, EncryptionSuite::Plaintext, None)
    }

    pub fn open_with_suite(
        path: &Path,
        suite: EncryptionSuite,
        master_key: Option<&MasterKey>,
    ) -> Result<Self> {
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
            crypto: PageCipher::new(suite, master_key)?,
            current_lsn: 0,
            file_len,
        })
    }

    /// Check whether the current file position is at or near the end of the WAL.
    /// "At tail" means there are no more complete, structurally plausible frames
    /// after the current position.
    ///
    /// This peeks at the next frame's length header (if present) and checks:
    /// 1. Whether there is room for a length header at all.
    /// 2. Whether the claimed length is within valid bounds (non-zero,
    ///    ≤ MAX_WAL_FRAME_LEN).
    /// 3. Whether the claimed payload fits in the remaining file space.
    ///
    /// Garbage that encodes a zero length, an oversized length, or a length that
    /// overflows the file is classified as tail.
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
            return true;
        }

        let next_frame_len = u32::from_le_bytes(len_buf) as u64;

        // A valid frame must have a non-zero length within the protocol bound.
        if next_frame_len == 0 || next_frame_len > MAX_WAL_FRAME_LEN as u64 {
            return true;
        }

        // If the claimed payload doesn't fit in the remaining space, we're at tail.
        remaining < 4 + next_frame_len
    }

    /// Scan ahead from the current file position to check whether any valid
    /// (decryptable + CRC-correct) WAL frame exists in the remaining data.
    ///
    /// This is used as a fallback when `is_at_tail` returns false (the next
    /// chunk looks structurally plausible) but the *current* frame failed
    /// validation. If no valid frame follows, the failure is effectively at the
    /// tail and can be tolerated.
    ///
    /// The file position and `current_lsn` are restored after probing.
    fn has_valid_frame_ahead(&mut self) -> bool {
        let saved_pos = match self.file.stream_position() {
            Ok(p) => p,
            Err(_) => return false,
        };
        let saved_lsn = self.current_lsn;

        // Start probing at the *next* LSN because the current frame (which
        // triggered this probe) was encrypted at self.current_lsn. Subsequent
        // frames use incrementing LSNs as their encryption nonce.
        let mut probe_lsn = self.current_lsn + 1;
        let found = loop {
            // Try to read a frame header
            let mut len_buf = [0u8; 4];
            if self.file.read_exact(&mut len_buf).is_err() {
                break false;
            }
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            if frame_len == 0 || frame_len > MAX_WAL_FRAME_LEN {
                break false;
            }

            let mut encrypted = vec![0u8; frame_len];
            if self.file.read_exact(&mut encrypted).is_err() {
                break false;
            }

            // Try to decrypt and validate CRC
            if let Ok(payload) = self.crypto.decrypt(probe_lsn, 0, &encrypted) {
                if payload.len() >= 4 {
                    let record_bytes = &payload[..payload.len() - 4];
                    let stored_crc =
                        u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());
                    if crc32(record_bytes) == stored_crc {
                        break true;
                    }
                }
            }

            // This frame was invalid; keep scanning
            probe_lsn += 1;
        };

        // Restore position and LSN
        let _ = self.file.seek(SeekFrom::Start(saved_pos));
        self.current_lsn = saved_lsn;
        found
    }

    /// Read the next WAL record. Returns None at end-of-file.
    ///
    /// Tolerates partial/corrupt frames at the WAL tail (no valid frames follow).
    /// Mid-log corruption (a corrupt frame followed by valid frames) is returned
    /// as a hard error to avoid silently dropping committed records.
    ///
    /// The tail heuristic uses two layers:
    /// 1. **Structural check** (`is_at_tail`): no structurally plausible next frame.
    /// 2. **Content probe** (`has_valid_frame_ahead`): even if the next chunk looks
    ///    frame-shaped, if it (and everything after) fails decryption/CRC, there
    ///    are no valid records to protect and the corruption is treated as tail.
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
            // Zero-length frame is never valid. If nothing valid follows, treat
            // as tail; otherwise it's genuine mid-log corruption.
            if !self.has_valid_frame_ahead() {
                return Ok(None);
            }
            return Err(MuroError::Wal("WAL frame length is zero".into()));
        }
        if frame_len > MAX_WAL_FRAME_LEN {
            // Oversized length — the length header itself may be corrupted, so we
            // cannot determine the real next-frame boundary. Scan-ahead would start
            // inside the payload and miss valid frames that follow.
            // Tolerate only when the claimed payload occupies the exact file tail
            // (original heuristic); otherwise report as corruption.
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
        // Two-layer tail check: structural heuristic first, then content probe
        // as fallback. The probe is only called when validation fails, so the
        // happy path pays no extra I/O cost.
        let effectively_at_tail =
            |this: &mut Self| -> bool { this.is_at_tail() || !this.has_valid_frame_ahead() };

        let payload = match self.crypto.decrypt(lsn, 0, &encrypted) {
            Ok(p) => p,
            Err(_) if effectively_at_tail(self) => {
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
            if effectively_at_tail(self) {
                return Ok(None);
            }
            return Err(MuroError::Wal("WAL record too short".into()));
        }

        let record_bytes = &payload[..payload.len() - 4];
        let stored_crc = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());

        if crc32(record_bytes) != stored_crc {
            if effectively_at_tail(self) {
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
                if effectively_at_tail(self) {
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

    /// Regression test (issue #12 review): valid frame A, mid-log frame B whose
    /// length header is corrupted to an oversized value, then valid frame C.
    /// The oversized branch must NOT use scan-ahead (which would start inside
    /// frame B's payload and miss frame C), and must return a hard error.
    #[test]
    fn test_oversized_length_mid_log_is_error_not_false_tail() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write three large records so the file is big enough for the corrupted
        // oversized length to still fit within remaining bytes.
        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap(); // A (small)
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 0,
                    data: vec![0xAA; 4096], // B (large — ~4K encrypted)
                })
                .unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: vec![0xBB; 4096], // C (large — ~4K encrypted)
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Corrupt frame B's length header to an oversized value that still fits
        // in the remaining file space (so it doesn't hit the truncation check).
        let file_bytes = std::fs::read(&path).unwrap();
        let hdr = WAL_HEADER_SIZE;
        let frame_a_len = u32::from_le_bytes(file_bytes[hdr..hdr + 4].try_into().unwrap()) as usize;
        let frame_b_offset = hdr + 4 + frame_a_len;
        let remaining_after_b_header = file_bytes.len() - frame_b_offset - 4;
        // Pick an oversized value that exceeds MAX_WAL_FRAME_LEN but fits in file
        let oversized = (MAX_WAL_FRAME_LEN as u32) + 100;
        assert!(
            (oversized as usize) <= remaining_after_b_header,
            "test setup: oversized value {} must fit in remaining {} bytes",
            oversized,
            remaining_after_b_header
        );

        let mut corrupted = file_bytes;
        corrupted[frame_b_offset..frame_b_offset + 4].copy_from_slice(&oversized.to_le_bytes());
        std::fs::write(&path, &corrupted).unwrap();

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        // Frame A should be read successfully
        let first = reader.next().unwrap();
        assert!(first.is_some(), "frame A should be readable");
        // Frame B has corrupted oversized length — mid-log, so hard error
        let result = reader.next();
        assert!(
            result.is_err(),
            "oversized length mid-log must be a hard error, not false-tail; got {:?}",
            result
        );
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

    /// Trailing garbage that happens to encode a plausible frame length but
    /// contains an invalid (undecryptable) payload. Before the fix, this would
    /// cause a hard "mid-log corruption" error because `is_at_tail` saw a
    /// structurally plausible next frame. With the scan-ahead fallback, recovery
    /// tolerates this as tail garbage (issue #12).
    #[test]
    fn test_plausible_length_garbage_at_tail_tolerated() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write one valid record
        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer.sync().unwrap();
        }

        // Append garbage that looks like a complete frame: a plausible length
        // (50 bytes, well within MAX_WAL_FRAME_LEN) followed by 50 bytes of
        // random-looking data, then MORE garbage that also looks frame-shaped.
        // This creates a scenario where is_at_tail returns false for both frames.
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            // First fake frame: length=50, payload=50 bytes of garbage
            file.write_all(&50u32.to_le_bytes()).unwrap();
            file.write_all(&[0xCA; 50]).unwrap();
            // Second fake frame: length=30, payload=30 bytes of garbage
            file.write_all(&30u32.to_le_bytes()).unwrap();
            file.write_all(&[0xFE; 30]).unwrap();
            file.sync_all().unwrap();
        }

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(
            records.len(),
            1,
            "only the valid Begin should be recovered; frame-shaped garbage must be tolerated"
        );
        assert!(matches!(&records[0].1, WalRecord::Begin { txid: 1 }));
    }

    /// Multiple fake frames at tail, each with a plausible length that fits
    /// within the file. All are garbage. Recovery must tolerate them.
    #[test]
    fn test_multiple_plausible_garbage_frames_at_tail() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        {
            let mut writer = WalWriter::create(&path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 1 })
                .unwrap();
            writer.sync().unwrap();
        }

        // Append three fake frames that chain together structurally
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            for i in 0..3 {
                let fake_len = 40 + i * 10;
                file.write_all(&(fake_len as u32).to_le_bytes()).unwrap();
                file.write_all(&vec![0xBB ^ (i as u8); fake_len]).unwrap();
            }
            file.sync_all().unwrap();
        }

        let mut reader = WalReader::open(&path, &test_key()).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 2); // Begin + Commit
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
