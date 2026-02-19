/// Key encoding for order-preserving byte comparison.
///
/// INT64: big-endian with sign bit flipped (so negative < positive in byte order)
/// VARCHAR: raw UTF-8 bytes
/// VARBINARY: raw bytes

/// Encode i64 into 8 bytes that preserve sort order under byte comparison.
pub fn encode_i64(val: i64) -> [u8; 8] {
    // Flip the sign bit so that negative numbers sort before positive
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

/// Decode i64 from order-preserving encoding.
pub fn decode_i64(bytes: &[u8; 8]) -> i64 {
    let unsigned = u64::from_be_bytes(*bytes);
    (unsigned ^ (1u64 << 63)) as i64
}

/// Compare two encoded keys.
/// Keys are variable-length bytes: the comparison is lexicographic.
pub fn compare_keys(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i64_encoding_order() {
        let values = [i64::MIN, -1000, -1, 0, 1, 1000, i64::MAX];
        let encoded: Vec<[u8; 8]> = values.iter().map(|&v| encode_i64(v)).collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "encode({}) should be < encode({})",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_i64_roundtrip() {
        for val in [i64::MIN, -1, 0, 1, i64::MAX, 42, -42] {
            assert_eq!(decode_i64(&encode_i64(val)), val);
        }
    }

    #[test]
    fn test_varchar_byte_comparison() {
        // UTF-8 byte comparison
        assert!(compare_keys(b"abc", b"abd") == std::cmp::Ordering::Less);
        assert!(compare_keys(b"abc", b"abc") == std::cmp::Ordering::Equal);
        assert!(compare_keys(b"b", b"a") == std::cmp::Ordering::Greater);
    }
}
