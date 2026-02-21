use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
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
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            Value::Integer(n) => n.hash(state),
            Value::Varchar(s) => s.hash(state),
            Value::Varbinary(b) => b.hash(state),
            Value::Null => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
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
            DataType::Varchar(None) => write!(f, "VARCHAR"),
            DataType::Varchar(Some(n)) => write!(f, "VARCHAR({})", n),
            DataType::Varbinary(None) => write!(f, "VARBINARY"),
            DataType::Varbinary(Some(n)) => write!(f, "VARBINARY({})", n),
            DataType::Text => write!(f, "TEXT"),
        }
    }
}
