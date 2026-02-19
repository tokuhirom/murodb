use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Compute HMAC-SHA256 for FTS term blinding.
/// term_id = HMAC-SHA256(term_key, bigram_bytes)
/// This ensures no plaintext tokens appear on disk.
pub fn hmac_term_id(term_key: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(term_key).expect("HMAC can take key of any size");
    mac.update(data);
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
}
