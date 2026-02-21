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

/// Encode f32 into 4 bytes that preserve sort order under byte comparison.
pub fn encode_f32(val: f32) -> [u8; 4] {
    let val = if val == 0.0 { 0.0 } else { val };
    let bits = val.to_bits();
    let ordered = if bits & (1u32 << 31) != 0 {
        !bits
    } else {
        bits ^ (1u32 << 31)
    };
    ordered.to_be_bytes()
}

/// Decode f32 from order-preserving encoding.
pub fn decode_f32(bytes: &[u8; 4]) -> f32 {
    let ordered = u32::from_be_bytes(*bytes);
    let bits = if ordered & (1u32 << 31) != 0 {
        ordered ^ (1u32 << 31)
    } else {
        !ordered
    };
    f32::from_bits(bits)
}

/// Encode f64 into 8 bytes that preserve sort order under byte comparison.
pub fn encode_f64(val: f64) -> [u8; 8] {
    let val = if val == 0.0 { 0.0 } else { val };
    let bits = val.to_bits();
    let ordered = if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    ordered.to_be_bytes()
}

/// Decode f64 from order-preserving encoding.
pub fn decode_f64(bytes: &[u8; 8]) -> f64 {
    let ordered = u64::from_be_bytes(*bytes);
    let bits = if ordered & (1u64 << 63) != 0 {
        ordered ^ (1u64 << 63)
    } else {
        !ordered
    };
    f64::from_bits(bits)
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

/// Encode a composite key from multiple values into a single byte sequence
/// that preserves sort order under lexicographic comparison.
///
/// Each column is encoded as:
/// - NULL: `0x00` (1 byte) — NULL sorts smallest
/// - Non-NULL: `0x01` + encoded value
///   - Fixed-length integers: sign-bit-flipped big-endian (existing encode_iN)
///   - Variable-length (VARCHAR/TEXT/VARBINARY): byte-stuffing
///     (`0x00` → `0x00 0x01`, terminated by `0x00 0x00`)
pub fn encode_composite_key(
    values: &[&crate::types::Value],
    data_types: &[&crate::types::DataType],
) -> Vec<u8> {
    use crate::types::{DataType, Value};

    let mut buf = Vec::new();
    for (val, dt) in values.iter().zip(data_types.iter()) {
        match val {
            Value::Null => {
                buf.push(0x00);
            }
            Value::Integer(n) => {
                buf.push(0x01);
                match dt {
                    DataType::TinyInt => buf.extend_from_slice(&encode_i8(*n as i8)),
                    DataType::SmallInt => buf.extend_from_slice(&encode_i16(*n as i16)),
                    DataType::Int => buf.extend_from_slice(&encode_i32(*n as i32)),
                    DataType::BigInt => buf.extend_from_slice(&encode_i64(*n)),
                    DataType::Float => buf.extend_from_slice(&encode_f32(*n as f32)),
                    DataType::Double => buf.extend_from_slice(&encode_f64(*n as f64)),
                    _ => buf.extend_from_slice(&encode_i64(*n)),
                }
            }
            Value::Float(n) => {
                buf.push(0x01);
                match dt {
                    DataType::TinyInt => {
                        if n.is_finite()
                            && n.fract() == 0.0
                            && *n >= i8::MIN as f64
                            && *n <= i8::MAX as f64
                        {
                            buf.extend_from_slice(&encode_i8(*n as i8));
                        } else {
                            buf.extend_from_slice(&[0xff; 9]);
                        }
                    }
                    DataType::SmallInt => {
                        if n.is_finite()
                            && n.fract() == 0.0
                            && *n >= i16::MIN as f64
                            && *n <= i16::MAX as f64
                        {
                            buf.extend_from_slice(&encode_i16(*n as i16));
                        } else {
                            buf.extend_from_slice(&[0xff; 9]);
                        }
                    }
                    DataType::Int => {
                        if n.is_finite()
                            && n.fract() == 0.0
                            && *n >= i32::MIN as f64
                            && *n <= i32::MAX as f64
                        {
                            buf.extend_from_slice(&encode_i32(*n as i32));
                        } else {
                            buf.extend_from_slice(&[0xff; 9]);
                        }
                    }
                    DataType::BigInt => {
                        if n.is_finite()
                            && n.fract() == 0.0
                            && *n >= i64::MIN as f64
                            && *n <= i64::MAX as f64
                        {
                            buf.extend_from_slice(&encode_i64(*n as i64));
                        } else {
                            buf.extend_from_slice(&[0xff; 9]);
                        }
                    }
                    DataType::Float => buf.extend_from_slice(&encode_f32(*n as f32)),
                    DataType::Double => buf.extend_from_slice(&encode_f64(*n)),
                    _ => buf.extend_from_slice(&encode_f64(*n)),
                }
            }
            Value::Varchar(s) => {
                buf.push(0x01);
                encode_byte_stuffed(&mut buf, s.as_bytes());
            }
            Value::Varbinary(b) => {
                buf.push(0x01);
                encode_byte_stuffed(&mut buf, b);
            }
        }
    }
    buf
}

/// Byte-stuffing encoding for variable-length data.
/// Each `0x00` byte in the input is replaced with `0x00 0x01`.
/// The sequence is terminated with `0x00 0x00`.
/// This preserves lexicographic order.
fn encode_byte_stuffed(buf: &mut Vec<u8>, data: &[u8]) {
    for &b in data {
        if b == 0x00 {
            buf.push(0x00);
            buf.push(0x01);
        } else {
            buf.push(b);
        }
    }
    buf.push(0x00);
    buf.push(0x00);
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
    fn test_float_zero_canonicalization() {
        assert_eq!(encode_f32(-0.0), encode_f32(0.0));
        assert_eq!(encode_f64(-0.0), encode_f64(0.0));
    }

    #[test]
    fn test_composite_key_integer_literal_for_float_column() {
        use crate::types::{DataType, Value};

        let vals_int = [&Value::Integer(1), &Value::Integer(7)];
        let vals_float = [&Value::Float(1.0), &Value::Integer(7)];
        let dts = [&DataType::Double, &DataType::Int];

        let k1 = encode_composite_key(&vals_int, &dts);
        let k2 = encode_composite_key(&vals_float, &dts);
        assert_eq!(k1, k2);
    }

    #[test]
    fn test_varchar_byte_comparison() {
        // UTF-8 byte comparison
        assert!(compare_keys(b"abc", b"abd") == std::cmp::Ordering::Less);
        assert!(compare_keys(b"abc", b"abc") == std::cmp::Ordering::Equal);
        assert!(compare_keys(b"b", b"a") == std::cmp::Ordering::Greater);
    }

    #[test]
    fn test_composite_key_order_int_string() {
        use crate::types::{DataType, Value};

        let v1a = Value::Integer(1);
        let v1b = Value::Varchar("abc".to_string());
        let v2a = Value::Integer(1);
        let v2b = Value::Varchar("abd".to_string());

        let dt = [&DataType::Int, &DataType::Varchar(None)];

        let k1 = encode_composite_key(&[&v1a, &v1b], &dt);
        let k2 = encode_composite_key(&[&v2a, &v2b], &dt);
        assert!(k1 < k2, "(1, 'abc') should be < (1, 'abd')");
    }

    #[test]
    fn test_composite_key_null_less_than_non_null() {
        use crate::types::{DataType, Value};

        let v1a = Value::Integer(1);
        let v1b = Value::Null;
        let v2a = Value::Integer(1);
        let v2b = Value::Varchar("a".to_string());

        let dt = [&DataType::Int, &DataType::Varchar(None)];

        let k1 = encode_composite_key(&[&v1a, &v1b], &dt);
        let k2 = encode_composite_key(&[&v2a, &v2b], &dt);
        assert!(k1 < k2, "(1, NULL) should be < (1, 'a')");
    }

    #[test]
    fn test_composite_key_with_nul_byte_in_varchar() {
        use crate::types::{DataType, Value};

        let v1 = Value::Varchar("a\0b".to_string());
        let v2 = Value::Varchar("a\0c".to_string());

        let dt = [&DataType::Varchar(None)];

        let k1 = encode_composite_key(&[&v1], &dt);
        let k2 = encode_composite_key(&[&v2], &dt);
        assert!(k1 < k2, "'a\\0b' should be < 'a\\0c'");
    }

    #[test]
    fn test_composite_key_equality() {
        use crate::types::{DataType, Value};

        let dt = [&DataType::Int, &DataType::Varchar(None)];

        let v1 = [&Value::Integer(42), &Value::Varchar("hello".to_string())];
        let v2 = [&Value::Integer(42), &Value::Varchar("hello".to_string())];

        assert_eq!(
            encode_composite_key(&v1, &dt),
            encode_composite_key(&v2, &dt)
        );
    }
}
