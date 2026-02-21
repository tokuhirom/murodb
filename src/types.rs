use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Varchar(String),
    Varbinary(Vec<u8>),
    Null,
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Varchar(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Varbinary(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
            Value::Varbinary(v) => write!(f, "<binary {} bytes>", v.len()),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Wrapper for Value that implements Eq + Hash, for use in HashMap/HashSet (GROUP BY, COUNT DISTINCT).
#[derive(Debug, Clone)]
pub struct ValueKey(pub Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                match (float_to_exact_i64_key(*a), float_to_exact_i64_key(*b)) {
                    (Some(x), Some(y)) => x == y,
                    _ => canonical_f64_bits(*a) == canonical_f64_bits(*b),
                }
            }
            (Value::Integer(a), Value::Float(b)) => int_float_equal(*a, *b),
            (Value::Float(a), Value::Integer(b)) => int_float_equal(*b, *a),
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Varbinary(a), Value::Varbinary(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ValueKey {}

impl Hash for ValueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Integer(n) => {
                if in_exact_f64_int_range(*n) {
                    // Numeric-equivalent domain for int/float (e.g. 1 and 1.0).
                    0u8.hash(state);
                    n.hash(state);
                } else {
                    1u8.hash(state);
                    n.hash(state);
                }
            }
            Value::Float(n) => {
                if let Some(i) = float_to_exact_i64_key(*n) {
                    0u8.hash(state);
                    i.hash(state);
                } else {
                    2u8.hash(state);
                    canonical_f64_bits(*n).hash(state);
                }
            }
            Value::Varchar(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Value::Varbinary(b) => {
                4u8.hash(state);
                b.hash(state);
            }
            Value::Null => {
                5u8.hash(state);
            }
        }
    }
}

const MAX_EXACT_F64_INT: i64 = 1_i64 << 53;

fn in_exact_f64_int_range(i: i64) -> bool {
    (-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&i)
}

fn canonical_f64_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else {
        n.to_bits()
    }
}

fn float_to_exact_i64_key(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let f = if f == 0.0 { 0.0 } else { f };
    if f.fract() != 0.0 {
        return None;
    }
    if !((-MAX_EXACT_F64_INT as f64)..=(MAX_EXACT_F64_INT as f64)).contains(&f) {
        return None;
    }
    Some(f as i64)
}

fn int_float_equal(i: i64, f: f64) -> bool {
    in_exact_f64_int_range(i) && float_to_exact_i64_key(f) == Some(i)
}

#[cfg(test)]
mod tests {
    use super::{Value, ValueKey};
    use std::collections::HashSet;

    #[test]
    fn test_value_key_large_int_float_no_non_transitive_equality() {
        let a = ValueKey(Value::Integer(9_007_199_254_740_992)); // 2^53
        let b = ValueKey(Value::Float(9_007_199_254_740_992.0));
        let c = ValueKey(Value::Integer(9_007_199_254_740_993)); // 2^53 + 1

        assert_eq!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn test_value_key_hash_for_numeric_equivalent_values() {
        let mut set = HashSet::new();
        set.insert(ValueKey(Value::Integer(1)));
        set.insert(ValueKey(Value::Float(1.0)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_as_i64_is_strict_integer_only() {
        assert_eq!(Value::Integer(42).as_i64(), Some(42));
        assert_eq!(Value::Float(42.0).as_i64(), None);
        assert_eq!(Value::Float(f64::NAN).as_i64(), None);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Varchar(Option<u32>),
    Varbinary(Option<u32>),
    Text,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Varchar(None) => write!(f, "VARCHAR"),
            DataType::Varchar(Some(n)) => write!(f, "VARCHAR({})", n),
            DataType::Varbinary(None) => write!(f, "VARBINARY"),
            DataType::Varbinary(Some(n)) => write!(f, "VARBINARY({})", n),
            DataType::Text => write!(f, "TEXT"),
        }
    }
}
