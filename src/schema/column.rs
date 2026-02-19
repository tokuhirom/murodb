use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub is_nullable: bool,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType) -> Self {
        ColumnDef {
            name: name.to_string(),
            data_type,
            is_primary_key: false,
            is_unique: false,
            is_nullable: true,
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

    /// Serialize column definition to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // name length + name
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        // data type
        buf.push(match self.data_type {
            DataType::Int64 => 1,
            DataType::Varchar => 2,
            DataType::Varbinary => 3,
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
        buf.push(flags);
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
        let data_type = match data[2 + name_len] {
            1 => DataType::Int64,
            2 => DataType::Varchar,
            3 => DataType::Varbinary,
            _ => return None,
        };
        let flags = data[2 + name_len + 1];
        let col = ColumnDef {
            name,
            data_type,
            is_primary_key: flags & 0x01 != 0,
            is_unique: flags & 0x02 != 0,
            is_nullable: flags & 0x04 != 0,
        };
        Some((col, 2 + name_len + 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_roundtrip() {
        let col = ColumnDef::new("id", DataType::Int64).primary_key();
        let bytes = col.serialize();
        let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
        assert_eq!(col2.name, "id");
        assert_eq!(col2.data_type, DataType::Int64);
        assert!(col2.is_primary_key);
        assert!(!col2.is_nullable);
    }
}
