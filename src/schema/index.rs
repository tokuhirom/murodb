use crate::storage::page::PageId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Fulltext,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub index_type: IndexType,
    pub is_unique: bool,
    pub btree_root: PageId,
    /// Last analyzed distinct key count (0 means unknown / not analyzed).
    pub stats_distinct_keys: u64,
}

impl IndexDef {
    /// Serialize index definition to bytes.
    /// Backward-compatible: first column_name is written at the legacy position,
    /// additional columns are appended after btree_root.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // name
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        // table_name
        let table_bytes = self.table_name.as_bytes();
        buf.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(table_bytes);
        // column_name (first column, legacy position)
        let first_col = self.column_names.first().map(|s| s.as_str()).unwrap_or("");
        let col_bytes = first_col.as_bytes();
        buf.extend_from_slice(&(col_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(col_bytes);
        // index_type
        buf.push(match self.index_type {
            IndexType::BTree => 1,
            IndexType::Fulltext => 2,
        });
        // is_unique
        buf.push(if self.is_unique { 1 } else { 0 });
        // btree_root
        buf.extend_from_slice(&self.btree_root.to_le_bytes());
        // additional columns (u16 count + strings) — only if > 1 column
        let extra = if self.column_names.len() > 1 {
            self.column_names.len() - 1
        } else {
            0
        };
        buf.extend_from_slice(&(extra as u16).to_le_bytes());
        for col in self.column_names.iter().skip(1) {
            let cb = col.as_bytes();
            buf.extend_from_slice(&(cb.len() as u16).to_le_bytes());
            buf.extend_from_slice(cb);
        }
        // stats_distinct_keys
        buf.extend_from_slice(&self.stats_distinct_keys.to_le_bytes());
        buf
    }

    /// Deserialize index definition from bytes.
    /// Backward-compatible: reads first column from legacy position,
    /// then reads additional columns if present after btree_root.
    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut offset = 0;

        // name
        if data.len() < offset + 2 {
            return None;
        }
        let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if data.len() < offset + name_len {
            return None;
        }
        let name = String::from_utf8(data[offset..offset + name_len].to_vec()).ok()?;
        offset += name_len;

        // table_name
        if data.len() < offset + 2 {
            return None;
        }
        let table_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if data.len() < offset + table_len {
            return None;
        }
        let table_name = String::from_utf8(data[offset..offset + table_len].to_vec()).ok()?;
        offset += table_len;

        // column_name (first column, legacy position)
        if data.len() < offset + 2 {
            return None;
        }
        let col_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if data.len() < offset + col_len {
            return None;
        }
        let first_column = String::from_utf8(data[offset..offset + col_len].to_vec()).ok()?;
        offset += col_len;

        // index_type
        if data.len() < offset + 1 {
            return None;
        }
        let index_type = match data[offset] {
            1 => IndexType::BTree,
            2 => IndexType::Fulltext,
            _ => return None,
        };
        offset += 1;

        // is_unique
        if data.len() < offset + 1 {
            return None;
        }
        let is_unique = data[offset] != 0;
        offset += 1;

        // btree_root
        if data.len() < offset + 8 {
            return None;
        }
        let btree_root = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // additional columns (backward-compatible: may not be present in old data)
        let mut column_names = vec![first_column];
        if data.len() >= offset + 2 {
            let extra_count =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            for _ in 0..extra_count {
                if data.len() < offset + 2 {
                    break;
                }
                let cl = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;
                if data.len() < offset + cl {
                    break;
                }
                let col = String::from_utf8(data[offset..offset + cl].to_vec()).ok()?;
                offset += cl;
                column_names.push(col);
            }
        }

        // stats_distinct_keys (optional for backward compat)
        let stats_distinct_keys = if data.len() >= offset + 8 {
            let n = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            n
        } else {
            0
        };

        Some((
            IndexDef {
                name,
                table_name,
                column_names,
                index_type,
                is_unique,
                btree_root,
                stats_distinct_keys,
            },
            offset,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_roundtrip() {
        let idx = IndexDef {
            name: "idx_users_email".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["email".to_string()],
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: 42,
            stats_distinct_keys: 0,
        };
        let bytes = idx.serialize();
        let (idx2, _) = IndexDef::deserialize(&bytes).unwrap();
        assert_eq!(idx2.name, "idx_users_email");
        assert_eq!(idx2.table_name, "users");
        assert_eq!(idx2.column_names, vec!["email".to_string()]);
        assert_eq!(idx2.index_type, IndexType::BTree);
        assert!(idx2.is_unique);
        assert_eq!(idx2.btree_root, 42);
    }

    #[test]
    fn test_composite_index_roundtrip() {
        let idx = IndexDef {
            name: "idx_composite".to_string(),
            table_name: "t".to_string(),
            column_names: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            index_type: IndexType::BTree,
            is_unique: false,
            btree_root: 99,
            stats_distinct_keys: 0,
        };
        let bytes = idx.serialize();
        let (idx2, _) = IndexDef::deserialize(&bytes).unwrap();
        assert_eq!(
            idx2.column_names,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }
}
