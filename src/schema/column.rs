use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
    pub is_hidden: bool,
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

    /// Serialize column definition to bytes.
    /// Format: [name_len(u16)][name][type_byte][flags][optional_size(u32)]
    /// optional_size is written for Varchar and Varbinary types:
    ///   0 = None, non-zero = Some(n)
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
        buf.push(flags);
        // optional size for Varchar/Varbinary
        match self.data_type {
            DataType::Varchar(size) | DataType::Varbinary(size) => {
                buf.extend_from_slice(&size.unwrap_or(0).to_le_bytes());
            }
            _ => {}
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
            _ => return None,
        };

        let col = ColumnDef {
            name,
            data_type,
            is_primary_key: flags & 0x01 != 0,
            is_unique: flags & 0x02 != 0,
            is_nullable: flags & 0x04 != 0,
            is_hidden: flags & 0x08 != 0,
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
}
