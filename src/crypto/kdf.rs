use crate::crypto::aead::MasterKey;
use crate::error::{MuroError, Result};
use argon2::Argon2;

/// Derive a 256-bit master key from a passphrase using Argon2id.
pub fn derive_key(passphrase: &[u8], salt: &[u8]) -> Result<MasterKey> {
    let mut output = [0u8; 32];
    Argon2::default()
        .hash_password_into(passphrase, salt, &mut output)
        .map_err(|e| MuroError::Kdf(e.to_string()))?;
    Ok(MasterKey::new(output))
}

/// Generate a random 16-byte salt.
pub fn generate_salt() -> [u8; 16] {
    let mut salt = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut salt);
    salt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key_deterministic() {
        let salt = [0x01u8; 16];
        let key1 = derive_key(b"my passphrase", &salt).unwrap();
        let key2 = derive_key(b"my passphrase", &salt).unwrap();
        assert_eq!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_different_passphrase_different_key() {
        let salt = [0x01u8; 16];
        let key1 = derive_key(b"passphrase1", &salt).unwrap();
        let key2 = derive_key(b"passphrase2", &salt).unwrap();
        assert_ne!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_different_salt_different_key() {
        let key1 = derive_key(b"pass", &[0x01u8; 16]).unwrap();
        let key2 = derive_key(b"pass", &[0x02u8; 16]).unwrap();
        assert_ne!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_salt_too_short_fails() {
        let result = derive_key(b"pass", &[0x01u8; 4]);
        assert!(result.is_err());
    }
}
