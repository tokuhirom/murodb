use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
    pub is_hidden: bool,
    pub auto_increment: bool,
    /// Default value as a simple literal (integer or string).
    /// Stored as serialized bytes in the column definition.
    pub default_value: Option<DefaultValue>,
    /// CHECK constraint expression text (stored as string, re-parsed at runtime).
    pub check_expr: Option<String>,
}

/// Simple default values that can be serialized.
#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    Integer(i64),
    Float(f64),
    String(String),
    Null,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType) -> Self {
        ColumnDef {
            name: name.to_string(),
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_nullable: true,
            is_hidden: false,
            auto_increment: false,
            default_value: None,
            check_expr: None,
        }
    }

    pub fn primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self.is_nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    pub fn not_null(mut self) -> Self {
        self.is_nullable = false;
        self
    }

    pub fn hidden(mut self) -> Self {
        self.is_hidden = true;
        self
    }

    pub fn with_auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    pub fn with_default(mut self, default: DefaultValue) -> Self {
        self.default_value = Some(default);
        self
    }

    pub fn with_check(mut self, check: &str) -> Self {
        self.check_expr = Some(check.to_string());
        self
    }

    /// Serialize column definition to bytes.
    /// Format: [name_len(u16)][name][type_byte][flags][optional_size(u32)]
    ///         [default_tag(u8)][default_data...][check_len(u16)][check_str...]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // name length + name
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        // data type byte
        buf.push(match self.data_type {
            DataType::BigInt => 1,
            DataType::Varchar(_) => 2,
            DataType::Varbinary(_) => 3,
            DataType::TinyInt => 4,
            DataType::SmallInt => 5,
            DataType::Int => 6,
            DataType::Text => 7,
            DataType::Float => 8,
            DataType::Double => 9,
        });
        // flags
        let mut flags: u8 = 0;
        if self.is_primary_key {
            flags |= 0x01;
        }
        if self.is_unique {
            flags |= 0x02;
        }
        if self.is_nullable {
            flags |= 0x04;
        }
        if self.is_hidden {
            flags |= 0x08;
        }
        if self.auto_increment {
            flags |= 0x10;
        }
        buf.push(flags);
        // optional size for Varchar/Varbinary
        match self.data_type {
            DataType::Varchar(size) | DataType::Varbinary(size) => {
                buf.extend_from_slice(&size.unwrap_or(0).to_le_bytes());
            }
            _ => {}
        }
        // default value
        match &self.default_value {
            None => buf.push(0), // no default
            Some(DefaultValue::Null) => buf.push(1),
            Some(DefaultValue::Integer(n)) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Some(DefaultValue::Float(n)) => {
                buf.push(4);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Some(DefaultValue::String(s)) => {
                buf.push(3);
                let s_bytes = s.as_bytes();
                buf.extend_from_slice(&(s_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(s_bytes);
            }
        }
        // check expression
        match &self.check_expr {
            None => buf.extend_from_slice(&0u16.to_le_bytes()),
            Some(expr) => {
                let expr_bytes = expr.as_bytes();
                buf.extend_from_slice(&(expr_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(expr_bytes);
            }
        }
        buf
    }

    /// Deserialize column definition from bytes. Returns (ColumnDef, bytes_consumed).
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 4 {
            return None;
        }
        let name_len = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
        if data.len() < 2 + name_len + 2 {
            return None;
        }
        let name = String::from_utf8(data[2..2 + name_len].to_vec()).ok()?;
        let type_byte = data[2 + name_len];
        let flags = data[2 + name_len + 1];
        let mut consumed = 2 + name_len + 2;

        let data_type = match type_byte {
            1 => DataType::BigInt,
            2 => {
                // Varchar with optional size
                let size = if data.len() >= consumed + 4 {
                    let n = u32::from_le_bytes(data[consumed..consumed + 4].try_into().unwrap());
                    consumed += 4;
                    if n == 0 {
                        None
                    } else {
                        Some(n)
                    }
                } else {
                    None
                };
                DataType::Varchar(size)
            }
            3 => {
                // Varbinary with optional size
                let size = if data.len() >= consumed + 4 {
                    let n = u32::from_le_bytes(data[consumed..consumed + 4].try_into().unwrap());
                    consumed += 4;
                    if n == 0 {
                        None
                    } else {
                        Some(n)
                    }
                } else {
                    None
                };
                DataType::Varbinary(size)
            }
            4 => DataType::TinyInt,
            5 => DataType::SmallInt,
            6 => DataType::Int,
            7 => DataType::Text,
            8 => DataType::Float,
            9 => DataType::Double,
            _ => return None,
        };

        let auto_increment = flags & 0x10 != 0;

        // default value
        let default_value = if data.len() > consumed {
            let tag = data[consumed];
            consumed += 1;
            match tag {
                0 => None,
                1 => Some(DefaultValue::Null),
                2 => {
                    if data.len() < consumed + 8 {
                        return None;
                    }
                    let n = i64::from_le_bytes(data[consumed..consumed + 8].try_into().unwrap());
                    consumed += 8;
                    Some(DefaultValue::Integer(n))
                }
                3 => {
                    if data.len() < consumed + 2 {
                        return None;
                    }
                    let slen = u16::from_le_bytes(data[consumed..consumed + 2].try_into().unwrap())
                        as usize;
                    consumed += 2;
                    if data.len() < consumed + slen {
                        return None;
                    }
                    let s = String::from_utf8(data[consumed..consumed + slen].to_vec()).ok()?;
                    consumed += slen;
                    Some(DefaultValue::String(s))
                }
                4 => {
                    if data.len() < consumed + 8 {
                        return None;
                    }
                    let n = f64::from_le_bytes(data[consumed..consumed + 8].try_into().unwrap());
                    consumed += 8;
                    Some(DefaultValue::Float(n))
                }
                _ => return None,
            }
        } else {
            None
        };

        // check expression
        let check_expr = if data.len() >= consumed + 2 {
            let check_len =
                u16::from_le_bytes(data[consumed..consumed + 2].try_into().unwrap()) as usize;
            consumed += 2;
            if check_len > 0 {
                if data.len() < consumed + check_len {
                    return None;
                }
                let s = String::from_utf8(data[consumed..consumed + check_len].to_vec()).ok()?;
                consumed += check_len;
                Some(s)
            } else {
                None
            }
        } else {
            None
        };

        let col = ColumnDef {
            name,
            data_type,
            is_primary_key: flags & 0x01 != 0,
            is_unique: flags & 0x02 != 0,
            is_nullable: flags & 0x04 != 0,
            is_hidden: flags & 0x08 != 0,
            auto_increment,
            default_value,
            check_expr,
        };
        Some((col, consumed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_roundtrip() {
        let col = ColumnDef::new("id", DataType::BigInt).primary_key();
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.name, "id");
        assert_eq!(col2.data_type, DataType::BigInt);
        assert!(col2.is_primary_key);
        assert!(!col2.is_nullable);
    }

    #[test]
    fn test_column_roundtrip_varchar_with_size() {
        let col = ColumnDef::new("name", DataType::Varchar(Some(255)));
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.name, "name");
        assert_eq!(col2.data_type, DataType::Varchar(Some(255)));
    }

    #[test]
    fn test_column_roundtrip_all_types() {
        for dt in [
            DataType::TinyInt,
            DataType::SmallInt,
            DataType::Int,
            DataType::BigInt,
            DataType::Float,
            DataType::Double,
            DataType::Varchar(None),
            DataType::Varchar(Some(100)),
            DataType::Varbinary(None),
            DataType::Varbinary(Some(512)),
            DataType::Text,
        ] {
            let col = ColumnDef::new("test", dt);
            let bytes = col.serialize();
            let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
            assert_eq!(col2.data_type, dt, "Roundtrip failed for {:?}", dt);
        }
    }

    #[test]
    fn test_column_roundtrip_auto_increment() {
        let col = ColumnDef::new("id", DataType::BigInt)
            .primary_key()
            .with_auto_increment();
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert!(col2.auto_increment);
        assert!(col2.is_primary_key);
    }

    #[test]
    fn test_column_roundtrip_default_integer() {
        let col = ColumnDef::new("status", DataType::Int).with_default(DefaultValue::Integer(0));
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.default_value, Some(DefaultValue::Integer(0)));
    }

    #[test]
    fn test_column_roundtrip_default_string() {
        let col = ColumnDef::new("name", DataType::Varchar(None))
            .with_default(DefaultValue::String("unknown".into()));
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(
            col2.default_value,
            Some(DefaultValue::String("unknown".into()))
        );
    }

    #[test]
    fn test_column_roundtrip_default_null() {
        let col = ColumnDef::new("name", DataType::Varchar(None)).with_default(DefaultValue::Null);
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.default_value, Some(DefaultValue::Null));
    }

    #[test]
    fn test_column_roundtrip_check() {
        let col = ColumnDef::new("age", DataType::Int).with_check("age > 0");
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.check_expr, Some("age > 0".into()));
    }
}
