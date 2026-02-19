/// Key encoding for order-preserving byte comparison.
///
/// Integer types: big-endian with sign bit flipped (so negative < positive in byte order)
/// VARCHAR/TEXT: raw UTF-8 bytes
/// VARBINARY: raw bytes
/// Encode i8 into 1 byte that preserves sort order under byte comparison.
pub fn encode_i8(val: i8) -> [u8; 1] {
    let unsigned = (val as u8) ^ (1u8 << 7);
    [unsigned]
}

/// Decode i8 from order-preserving encoding.
pub fn decode_i8(bytes: &[u8; 1]) -> i8 {
    (bytes[0] ^ (1u8 << 7)) as i8
}

/// Encode i16 into 2 bytes that preserve sort order under byte comparison.
pub fn encode_i16(val: i16) -> [u8; 2] {
    let unsigned = (val as u16) ^ (1u16 << 15);
    unsigned.to_be_bytes()
}

/// Decode i16 from order-preserving encoding.
pub fn decode_i16(bytes: &[u8; 2]) -> i16 {
    let unsigned = u16::from_be_bytes(*bytes);
    (unsigned ^ (1u16 << 15)) as i16
}

/// Encode i32 into 4 bytes that preserve sort order under byte comparison.
pub fn encode_i32(val: i32) -> [u8; 4] {
    let unsigned = (val as u32) ^ (1u32 << 31);
    unsigned.to_be_bytes()
}

/// Decode i32 from order-preserving encoding.
pub fn decode_i32(bytes: &[u8; 4]) -> i32 {
    let unsigned = u32::from_be_bytes(*bytes);
    (unsigned ^ (1u32 << 31)) as i32
}

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
    fn test_i8_encoding_order() {
        let values = [i8::MIN, -1, 0, 1, i8::MAX];
        let encoded: Vec<[u8; 1]> = values.iter().map(|&v| encode_i8(v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1]);
        }
    }

    #[test]
    fn test_i8_roundtrip() {
        for val in [i8::MIN, -1, 0, 1, i8::MAX, 42, -42] {
            assert_eq!(decode_i8(&encode_i8(val)), val);
        }
    }

    #[test]
    fn test_i16_encoding_order() {
        let values = [i16::MIN, -1000, -1, 0, 1, 1000, i16::MAX];
        let encoded: Vec<[u8; 2]> = values.iter().map(|&v| encode_i16(v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1]);
        }
    }

    #[test]
    fn test_i16_roundtrip() {
        for val in [i16::MIN, -1, 0, 1, i16::MAX, 42, -42] {
            assert_eq!(decode_i16(&encode_i16(val)), val);
        }
    }

    #[test]
    fn test_i32_encoding_order() {
        let values = [i32::MIN, -1000, -1, 0, 1, 1000, i32::MAX];
        let encoded: Vec<[u8; 4]> = values.iter().map(|&v| encode_i32(v)).collect();
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1]);
        }
    }

    #[test]
    fn test_i32_roundtrip() {
        for val in [i32::MIN, -1, 0, 1, i32::MAX, 42, -42] {
            assert_eq!(decode_i32(&encode_i32(val)), val);
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
