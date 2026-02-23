use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::crypto::aead::MasterKey;
use crate::crypto::suite::{EncryptionSuite, PageCipher};
use crate::error::{MuroError, Result};
use crate::wal::record::crc32;

/// Marker file layout:
///   0..4    Magic "REKY"
///   4..20   new_salt (16 bytes)
///  20..28   new_epoch (u64 LE)
///  28..32   flags (bit0: wrapped old key present)
///  32..36   CRC32 of bytes 0..32
///  36..96   wrapped_old_key (optional; 60 bytes)
const REKEY_MARKER_MAGIC: &[u8; 4] = b"REKY";
const REKEY_MARKER_SIZE: usize = 36;
const REKEY_MARKER_FLAG_WRAPPED_OLD_KEY: u32 = 1;
const REKEY_MARKER_WRAP_PAGE_ID: u64 = u64::MAX;
const REKEY_WRAPPED_OLD_KEY_LEN: usize = crate::crypto::aead::PageCrypto::overhead() + 32;

#[derive(Debug, Clone)]
pub struct RekeyMarker {
    pub new_salt: [u8; 16],
    pub new_epoch: u64,
    pub wrapped_old_key: Option<Vec<u8>>,
}

/// Path for the .rekey crash-safety marker file.
pub fn rekey_marker_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".rekey");
    PathBuf::from(s)
}

pub(super) fn write_rekey_marker(
    path: &Path,
    new_salt: &[u8; 16],
    new_epoch: u64,
    old_key: &MasterKey,
    new_key: &MasterKey,
) -> Result<()> {
    let wrap_cipher = PageCipher::new(EncryptionSuite::Aes256GcmSiv, Some(new_key))?;
    let wrapped_old_key =
        wrap_cipher.encrypt(REKEY_MARKER_WRAP_PAGE_ID, new_epoch, old_key.as_bytes())?;
    if wrapped_old_key.len() != REKEY_WRAPPED_OLD_KEY_LEN {
        return Err(MuroError::Encryption(
            "unexpected wrapped old key size in rekey marker".to_string(),
        ));
    }

    let mut buf = vec![0u8; REKEY_MARKER_SIZE + REKEY_WRAPPED_OLD_KEY_LEN];
    buf[0..4].copy_from_slice(REKEY_MARKER_MAGIC);
    buf[4..20].copy_from_slice(new_salt);
    buf[20..28].copy_from_slice(&new_epoch.to_le_bytes());
    buf[28..32].copy_from_slice(&REKEY_MARKER_FLAG_WRAPPED_OLD_KEY.to_le_bytes());
    let checksum = crc32(&buf[0..32]);
    buf[32..36].copy_from_slice(&checksum.to_le_bytes());
    buf[36..36 + REKEY_WRAPPED_OLD_KEY_LEN].copy_from_slice(&wrapped_old_key);

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(&buf)?;
    file.sync_all()?;
    Ok(())
}

/// Read and validate a .rekey marker file.
pub fn read_rekey_marker(path: &Path) -> Result<RekeyMarker> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    if buf.len() < REKEY_MARKER_SIZE {
        return Err(MuroError::Corruption(
            "rekey marker file is too short".to_string(),
        ));
    }

    if &buf[0..4] != REKEY_MARKER_MAGIC {
        return Err(MuroError::Corruption(
            "rekey marker file has invalid magic".to_string(),
        ));
    }
    let stored_crc = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    let computed_crc = crc32(&buf[0..32]);
    if stored_crc != computed_crc {
        return Err(MuroError::Corruption(
            "rekey marker file is corrupted".to_string(),
        ));
    }

    let mut new_salt = [0u8; 16];
    new_salt.copy_from_slice(&buf[4..20]);
    let new_epoch = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let flags = u32::from_le_bytes(buf[28..32].try_into().unwrap());

    let wrapped_old_key = if flags & REKEY_MARKER_FLAG_WRAPPED_OLD_KEY != 0 {
        if buf.len() < REKEY_MARKER_SIZE + REKEY_WRAPPED_OLD_KEY_LEN {
            return Err(MuroError::Corruption(
                "rekey marker file missing wrapped old key payload".to_string(),
            ));
        }
        Some(buf[36..36 + REKEY_WRAPPED_OLD_KEY_LEN].to_vec())
    } else {
        None
    };

    Ok(RekeyMarker {
        new_salt,
        new_epoch,
        wrapped_old_key,
    })
}

pub fn unwrap_rekey_old_key(
    new_key: &MasterKey,
    new_epoch: u64,
    wrapped_old_key: &[u8],
) -> Result<MasterKey> {
    let unwrap_cipher = PageCipher::new(EncryptionSuite::Aes256GcmSiv, Some(new_key))?;
    let old_key_bytes =
        unwrap_cipher.decrypt(REKEY_MARKER_WRAP_PAGE_ID, new_epoch, wrapped_old_key)?;
    MasterKey::from_slice(&old_key_bytes)
}
