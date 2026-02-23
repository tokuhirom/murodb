use crate::crypto::aead::MasterKey;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const FTS_TERM_KEY_LABEL: &[u8] = b"murodb/fts/term-key/v1";
const FTS_TERM_KEY_PLAINTEXT_LABEL: &[u8] = b"murodb/fts/term-key/plaintext/v1";

/// Compute HMAC-SHA256 for FTS term blinding.
/// term_id = HMAC-SHA256(term_key, bigram_bytes)
/// This ensures no plaintext tokens appear on disk.
pub fn hmac_term_id(term_key: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(term_key).expect("HMAC can take key of any size");
    mac.update(data);
    let result = mac.finalize();
    result.into_bytes().into()
}

/// Derive a database-scoped FTS term key from the master key and DB salt.
pub fn derive_fts_term_key(master_key: &MasterKey, salt: &[u8; 16]) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(master_key.as_bytes()).expect("HMAC can take key of any size");
    mac.update(FTS_TERM_KEY_LABEL);
    mac.update(salt);
    let result = mac.finalize();
    result.into_bytes().into()
}

/// Derive a plaintext-mode FTS term key using DB salt only.
pub fn derive_fts_term_key_plaintext(salt: &[u8; 16]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(FTS_TERM_KEY_PLAINTEXT_LABEL)
        .expect("HMAC can take key of any size");
    mac.update(salt);
    let result = mac.finalize();
    result.into_bytes().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_deterministic() {
        let key = [0x42u8; 32];
        let h1 = hmac_term_id(&key, "東京".as_bytes());
        let h2 = hmac_term_id(&key, "東京".as_bytes());
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hmac_different_input() {
        let key = [0x42u8; 32];
        let h1 = hmac_term_id(&key, "東京".as_bytes());
        let h2 = hmac_term_id(&key, "京都".as_bytes());
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hmac_different_key() {
        let h1 = hmac_term_id(&[0x01u8; 32], "test".as_bytes());
        let h2 = hmac_term_id(&[0x02u8; 32], "test".as_bytes());
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_derive_fts_term_key_is_deterministic() {
        let master = MasterKey::new([0x42u8; 32]);
        let salt = [0x11u8; 16];
        let k1 = derive_fts_term_key(&master, &salt);
        let k2 = derive_fts_term_key(&master, &salt);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_derive_fts_term_key_changes_with_salt() {
        let master = MasterKey::new([0x42u8; 32]);
        let k1 = derive_fts_term_key(&master, &[0x11u8; 16]);
        let k2 = derive_fts_term_key(&master, &[0x22u8; 16]);
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_derive_fts_term_key_changes_with_master_key() {
        let salt = [0x11u8; 16];
        let k1 = derive_fts_term_key(&MasterKey::new([0x01u8; 32]), &salt);
        let k2 = derive_fts_term_key(&MasterKey::new([0x02u8; 32]), &salt);
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_derive_fts_term_key_plaintext_changes_with_salt() {
        let k1 = derive_fts_term_key_plaintext(&[0x11u8; 16]);
        let k2 = derive_fts_term_key_plaintext(&[0x22u8; 16]);
        assert_ne!(k1, k2);
    }
}
