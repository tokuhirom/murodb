use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use rand::RngCore;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{MuroError, Result};
use crate::storage::page::PageId;

/// 256-bit master key for AES-256-GCM-SIV.
/// Key material is zeroed on drop to prevent leaking secrets in memory.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct MasterKey {
    key: [u8; 32],
}

impl MasterKey {
    pub fn new(key: [u8; 32]) -> Self {
        MasterKey { key }
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() != 32 {
            return Err(MuroError::Encryption(
                "Master key must be 32 bytes".to_string(),
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(slice);
        Ok(MasterKey { key })
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.key
    }
}

/// Page-level AEAD encryption/decryption.
///
/// Uses AES-256-GCM-SIV which is nonce-misuse resistant.
/// AAD = page_id (8 bytes LE) || epoch (8 bytes LE)
pub struct PageCrypto {
    cipher: Aes256GcmSiv,
}

/// Nonce size for AES-GCM-SIV is 12 bytes.
const NONCE_SIZE: usize = 12;
/// Authentication tag is 16 bytes.
const TAG_OVERHEAD: usize = 16;

impl PageCrypto {
    pub fn new(master_key: &MasterKey) -> Self {
        let cipher = Aes256GcmSiv::new_from_slice(master_key.as_bytes()).expect("valid key size");
        PageCrypto { cipher }
    }

    /// Build AAD from page_id and epoch.
    fn build_aad(page_id: PageId, epoch: u64) -> [u8; 16] {
        let mut aad = [0u8; 16];
        aad[0..8].copy_from_slice(&page_id.to_le_bytes());
        aad[8..16].copy_from_slice(&epoch.to_le_bytes());
        aad
    }

    /// Encrypt page plaintext.
    /// Returns: nonce (12 bytes) || ciphertext+tag
    pub fn encrypt(&self, page_id: PageId, epoch: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let aad = Self::build_aad(page_id, epoch);

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = Payload {
            msg: plaintext,
            aad: &aad,
        };

        let ciphertext = self
            .cipher
            .encrypt(nonce, payload)
            .map_err(|e| MuroError::Encryption(e.to_string()))?;

        let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        Ok(result)
    }

    /// Decrypt: input = nonce (12 bytes) || ciphertext+tag
    pub fn decrypt(&self, page_id: PageId, epoch: u64, encrypted: &[u8]) -> Result<Vec<u8>> {
        if encrypted.len() < NONCE_SIZE + TAG_OVERHEAD {
            return Err(MuroError::Decryption);
        }

        let aad = Self::build_aad(page_id, epoch);
        let nonce = Nonce::from_slice(&encrypted[..NONCE_SIZE]);
        let ciphertext = &encrypted[NONCE_SIZE..];

        let payload = Payload {
            msg: ciphertext,
            aad: &aad,
        };

        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| MuroError::Decryption)
    }

    /// Overhead added by encryption (nonce + tag).
    pub const fn overhead() -> usize {
        NONCE_SIZE + TAG_OVERHEAD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let crypto = PageCrypto::new(&test_key());
        let plaintext = b"Hello, MuroDB page data!";
        let page_id = 7;
        let epoch = 1;

        let encrypted = crypto.encrypt(page_id, epoch, plaintext).unwrap();
        let decrypted = crypto.decrypt(page_id, epoch, &encrypted).unwrap();

        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_tamper_detection() {
        let crypto = PageCrypto::new(&test_key());
        let plaintext = b"Sensitive data";
        let page_id = 1;
        let epoch = 0;

        let mut encrypted = crypto.encrypt(page_id, epoch, plaintext).unwrap();

        // Flip one byte in the ciphertext portion
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0x01;

        let result = crypto.decrypt(page_id, epoch, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_page_id_fails() {
        let crypto = PageCrypto::new(&test_key());
        let plaintext = b"data";

        let encrypted = crypto.encrypt(1, 0, plaintext).unwrap();
        // Decrypt with different page_id should fail (AAD mismatch)
        let result = crypto.decrypt(2, 0, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_epoch_fails() {
        let crypto = PageCrypto::new(&test_key());
        let plaintext = b"data";

        let encrypted = crypto.encrypt(1, 0, plaintext).unwrap();
        // Decrypt with different epoch should fail
        let result = crypto.decrypt(1, 1, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_key_fails() {
        let crypto1 = PageCrypto::new(&MasterKey::new([0x01u8; 32]));
        let crypto2 = PageCrypto::new(&MasterKey::new([0x02u8; 32]));

        let encrypted = crypto1.encrypt(1, 0, b"secret").unwrap();
        let result = crypto2.decrypt(1, 0, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_page_roundtrip() {
        let crypto = PageCrypto::new(&test_key());
        let plaintext = vec![0xABu8; 4096];

        let encrypted = crypto.encrypt(0, 0, &plaintext).unwrap();
        assert_eq!(encrypted.len(), 4096 + PageCrypto::overhead());

        let decrypted = crypto.decrypt(0, 0, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
