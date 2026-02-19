use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int64(i64),
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
            Value::Int64(v) => Some(*v),
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
            Value::Int64(v) => write!(f, "{}", v),
            Value::Varchar(v) => write!(f, "{}", v),
            Value::Varbinary(v) => write!(f, "<binary {} bytes>", v.len()),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int64,
    Varchar,
    Varbinary,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int64 => write!(f, "INT64"),
            DataType::Varchar => write!(f, "VARCHAR"),
            DataType::Varbinary => write!(f, "VARBINARY"),
        }
    }
}
