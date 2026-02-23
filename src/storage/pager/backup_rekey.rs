use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::crypto::aead::MasterKey;
use crate::crypto::suite::{EncryptionSuite, PageCipher};
use crate::error::{MuroError, Result};
use crate::storage::page::PAGE_SIZE;

use super::rekey_marker::write_rekey_marker;
use super::{rekey_marker_path, Pager, PLAINTEXT_HEADER_SIZE};

impl Pager {
    /// Re-encrypt all pages with a new master key and salt.
    ///
    /// This performs a full re-encryption of every page in the database file:
    /// 1. Writes a `.rekey` marker file for crash safety
    /// 2. Reads each page with the current key/epoch, re-encrypts with new key/epoch
    /// 3. Syncs all pages to disk
    /// 4. Updates the header with new salt/epoch
    /// 5. Removes the marker file
    ///
    /// If a crash occurs mid-rekey, the marker file enables recovery on next open.
    pub fn rekey(&mut self, new_key: &MasterKey, new_salt: [u8; 16]) -> Result<()> {
        if self.encryption_suite == EncryptionSuite::Plaintext {
            return Err(MuroError::Execution(
                "rekey is not supported for plaintext databases".to_string(),
            ));
        }
        let old_master_key = self.master_key.as_ref().ok_or_else(|| {
            MuroError::Encryption("missing in-memory master key for rekey operation".to_string())
        })?;

        let new_epoch = self.epoch + 1;
        let new_crypto = PageCipher::new(self.encryption_suite, Some(new_key))?;
        let old_epoch = self.epoch;

        // Write .rekey marker file for crash recovery
        let marker_path = rekey_marker_path(&self.path);
        write_rekey_marker(&marker_path, &new_salt, new_epoch, old_master_key, new_key)?;

        // Re-encrypt all pages
        let page_count = self.page_count;
        let page_size_on_disk = self.page_size_on_disk();
        for page_id in 0..page_count {
            // Read with current crypto/epoch
            let offset = PLAINTEXT_HEADER_SIZE + page_id * page_size_on_disk as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            let mut encrypted = vec![0u8; page_size_on_disk];
            self.file.read_exact(&mut encrypted)?;

            let mut plaintext = [0u8; PAGE_SIZE];
            let plaintext_len =
                self.crypto
                    .decrypt_into(page_id, old_epoch, &encrypted, &mut plaintext)?;
            if plaintext_len != PAGE_SIZE {
                return Err(MuroError::InvalidPage);
            }

            // Re-encrypt with new crypto/epoch
            let mut new_encrypted = vec![0u8; page_size_on_disk];
            let written =
                new_crypto.encrypt_into(page_id, new_epoch, &plaintext, &mut new_encrypted)?;
            if written != page_size_on_disk {
                return Err(MuroError::Encryption(
                    "unexpected encrypted page size during rekey".to_string(),
                ));
            }

            // Write back
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&new_encrypted)?;
        }

        // Sync all page data to disk
        self.file.sync_data()?;

        // Update in-memory state
        self.crypto = new_crypto;
        self.master_key = Some(new_key.clone());
        self.salt = new_salt;
        self.epoch = new_epoch;

        // Write new header and sync
        self.write_plaintext_header()?;
        self.file.sync_all()?;

        // Remove marker file
        let _ = std::fs::remove_file(&marker_path);

        // Clear page cache since all pages changed
        self.cache.clear();

        Ok(())
    }

    /// Create a byte-level copy of the database file to `dest`.
    ///
    /// Copies the plaintext header and all encrypted pages as raw bytes
    /// (no decryption/re-encryption). The caller must ensure no concurrent
    /// writes are in progress (e.g. by holding a lock) and that the WAL
    /// has been checkpointed before calling this method.
    ///
    /// The resulting file is a valid MuroDB database that can be opened
    /// with the same key/password.
    pub fn backup_to_file(&mut self, dest: &Path) -> Result<()> {
        // Guard: reject backup to the same file (including symlinks/hardlinks).
        {
            use std::os::unix::fs::MetadataExt;
            let src_meta = self.file.metadata()?;
            if let Ok(dest_meta) = std::fs::metadata(dest) {
                if src_meta.dev() == dest_meta.dev() && src_meta.ino() == dest_meta.ino() {
                    return Err(MuroError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "backup destination is the same file as the source database",
                    )));
                }
            }
        }

        self.refresh_from_disk_if_changed()?;

        let total_bytes = PLAINTEXT_HEADER_SIZE + self.page_count * self.page_size_on_disk() as u64;

        self.file.seek(SeekFrom::Start(0))?;

        let mut dest_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)?;

        // Use a fixed-size buffer to avoid BufReader's read-ahead from
        // advancing self.file's seek position beyond total_bytes.
        let mut remaining = total_bytes;
        let mut buf = [0u8; 64 * 1024];
        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            self.file.read_exact(&mut buf[..to_read])?;
            dest_file.write_all(&buf[..to_read])?;
            remaining -= to_read as u64;
        }
        dest_file.sync_all()?;

        Ok(())
    }
}
