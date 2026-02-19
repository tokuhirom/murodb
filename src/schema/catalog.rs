/// System catalog: stores table and index definitions.
///
/// The catalog is stored as a B-tree with well-known keys:
///   "table:<name>" -> serialized TableDef
///   "index:<name>" -> serialized IndexDef
///
/// The catalog B-tree root is stored at a well-known page.

use crate::btree::ops::BTree;
use crate::error::{MuroError, Result};
use crate::schema::column::ColumnDef;
use crate::schema::index::IndexDef;
use crate::storage::page::PageId;
use crate::storage::pager::Pager;

/// Table definition.
#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub pk_column: Option<String>,
    pub data_btree_root: PageId,
}

impl TableDef {
    /// Serialize table definition.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // name
        let name_bytes = self.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        // column count
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        // columns
        for col in &self.columns {
            let col_bytes = col.serialize();
            buf.extend_from_slice(&(col_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(&col_bytes);
        }
        // pk_column
        match &self.pk_column {
            Some(pk) => {
                buf.push(1);
                let pk_bytes = pk.as_bytes();
                buf.extend_from_slice(&(pk_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(pk_bytes);
            }
            None => buf.push(0),
        }
        // data_btree_root
        buf.extend_from_slice(&self.data_btree_root.to_le_bytes());
        buf
    }

    /// Deserialize table definition.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let mut offset = 0;

        // name
        if data.len() < offset + 2 { return None; }
        let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let name = String::from_utf8(data[offset..offset + name_len].to_vec()).ok()?;
        offset += name_len;

        // column count
        if data.len() < offset + 2 { return None; }
        let col_count = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        // columns
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if data.len() < offset + 2 { return None; }
            let col_bytes_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let (col, _) = ColumnDef::deserialize(&data[offset..offset + col_bytes_len])?;
            columns.push(col);
            offset += col_bytes_len;
        }

        // pk_column
        if data.len() < offset + 1 { return None; }
        let has_pk = data[offset];
        offset += 1;
        let pk_column = if has_pk == 1 {
            if data.len() < offset + 2 { return None; }
            let pk_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let pk = String::from_utf8(data[offset..offset + pk_len].to_vec()).ok()?;
            offset += pk_len;
            Some(pk)
        } else {
            None
        };

        // data_btree_root
        if data.len() < offset + 8 { return None; }
        let data_btree_root = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());

        Some(TableDef {
            name,
            columns,
            pk_column,
            data_btree_root,
        })
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get PK column index.
    pub fn pk_column_index(&self) -> Option<usize> {
        self.pk_column.as_ref().and_then(|pk| self.column_index(pk))
    }
}

/// System catalog managing table and index definitions.
pub struct SystemCatalog {
    catalog_btree: BTree,
}

impl SystemCatalog {
    /// Create a new system catalog with a fresh B-tree.
    pub fn create(pager: &mut Pager) -> Result<Self> {
        let catalog_btree = BTree::create(pager)?;
        Ok(SystemCatalog { catalog_btree })
    }

    /// Open an existing system catalog.
    pub fn open(catalog_root: PageId) -> Self {
        SystemCatalog {
            catalog_btree: BTree::open(catalog_root),
        }
    }

    pub fn root_page_id(&self) -> PageId {
        self.catalog_btree.root_page_id()
    }

    /// Create a table. Returns the table definition with the allocated B-tree root.
    pub fn create_table(
        &mut self,
        pager: &mut Pager,
        name: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<TableDef> {
        // Check if table already exists
        let key = format!("table:{}", name);
        if self.catalog_btree.search(pager, key.as_bytes())?.is_some() {
            return Err(MuroError::Schema(format!("Table '{}' already exists", name)));
        }

        // Find PK column
        let pk_column = columns.iter().find(|c| c.is_primary_key).map(|c| c.name.clone());

        // Allocate a B-tree for the table data
        let data_btree = BTree::create(pager)?;
        let data_btree_root = data_btree.root_page_id();

        let table_def = TableDef {
            name: name.to_string(),
            columns,
            pk_column,
            data_btree_root,
        };

        // Store in catalog
        let serialized = table_def.serialize();
        self.catalog_btree.insert(pager, key.as_bytes(), &serialized)?;

        Ok(table_def)
    }

    /// Get a table definition by name.
    pub fn get_table(&self, pager: &mut Pager, name: &str) -> Result<Option<TableDef>> {
        let key = format!("table:{}", name);
        match self.catalog_btree.search(pager, key.as_bytes())? {
            Some(data) => Ok(TableDef::deserialize(&data)),
            None => Ok(None),
        }
    }

    /// Update a table definition.
    pub fn update_table(&mut self, pager: &mut Pager, table_def: &TableDef) -> Result<()> {
        let key = format!("table:{}", table_def.name);
        let serialized = table_def.serialize();
        self.catalog_btree.insert(pager, key.as_bytes(), &serialized)?;
        Ok(())
    }

    /// Create an index definition and store it in the catalog.
    pub fn create_index(
        &mut self,
        pager: &mut Pager,
        index_def: IndexDef,
    ) -> Result<IndexDef> {
        let key = format!("index:{}", index_def.name);
        if self.catalog_btree.search(pager, key.as_bytes())?.is_some() {
            return Err(MuroError::Schema(format!(
                "Index '{}' already exists",
                index_def.name
            )));
        }
        let serialized = index_def.serialize();
        self.catalog_btree.insert(pager, key.as_bytes(), &serialized)?;
        Ok(index_def)
    }

    /// Get an index definition by name.
    pub fn get_index(&self, pager: &mut Pager, name: &str) -> Result<Option<IndexDef>> {
        let key = format!("index:{}", name);
        match self.catalog_btree.search(pager, key.as_bytes())? {
            Some(data) => Ok(IndexDef::deserialize(&data).map(|(idx, _)| idx)),
            None => Ok(None),
        }
    }

    /// Get all indexes for a table.
    pub fn get_indexes_for_table(
        &self,
        pager: &mut Pager,
        table_name: &str,
    ) -> Result<Vec<IndexDef>> {
        let mut indexes = Vec::new();
        self.catalog_btree.scan(pager, |k, v| {
            if let Ok(key_str) = std::str::from_utf8(k) {
                if key_str.starts_with("index:") {
                    if let Some((idx, _)) = IndexDef::deserialize(v) {
                        if idx.table_name == table_name {
                            indexes.push(idx);
                        }
                    }
                }
            }
            Ok(true)
        })?;
        Ok(indexes)
    }

    /// List all table names.
    pub fn list_tables(&self, pager: &mut Pager) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        self.catalog_btree.scan(pager, |k, _v| {
            if let Ok(key_str) = std::str::from_utf8(k) {
                if let Some(name) = key_str.strip_prefix("table:") {
                    tables.push(name.to_string());
                }
            }
            Ok(true)
        })?;
        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::schema::index::IndexType;
    use crate::types::DataType;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    #[test]
    fn test_table_def_roundtrip() {
        let table = TableDef {
            name: "users".to_string(),
            columns: vec![
                ColumnDef::new("id", DataType::Int64).primary_key(),
                ColumnDef::new("name", DataType::Varchar),
                ColumnDef::new("data", DataType::Varbinary),
            ],
            pk_column: Some("id".to_string()),
            data_btree_root: 42,
        };

        let bytes = table.serialize();
        let table2 = TableDef::deserialize(&bytes).unwrap();
        assert_eq!(table2.name, "users");
        assert_eq!(table2.columns.len(), 3);
        assert_eq!(table2.pk_column, Some("id".to_string()));
        assert_eq!(table2.data_btree_root, 42);
    }

    #[test]
    fn test_catalog_create_and_get_table() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();

        let columns = vec![
            ColumnDef::new("id", DataType::Int64).primary_key(),
            ColumnDef::new("body", DataType::Varchar),
        ];

        let table_def = catalog.create_table(&mut pager, "posts", columns).unwrap();
        assert_eq!(table_def.name, "posts");
        assert_eq!(table_def.pk_column, Some("id".to_string()));

        let retrieved = catalog.get_table(&mut pager, "posts").unwrap().unwrap();
        assert_eq!(retrieved.name, "posts");
        assert_eq!(retrieved.columns.len(), 2);
    }

    #[test]
    fn test_catalog_duplicate_table() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();

        let columns = vec![ColumnDef::new("id", DataType::Int64).primary_key()];
        catalog.create_table(&mut pager, "t", columns.clone()).unwrap();
        assert!(catalog.create_table(&mut pager, "t", columns).is_err());
    }

    #[test]
    fn test_catalog_indexes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();

        let idx = IndexDef {
            name: "idx_t_col".to_string(),
            table_name: "t".to_string(),
            column_name: "col".to_string(),
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: 99,
        };

        catalog.create_index(&mut pager, idx).unwrap();

        let retrieved = catalog.get_index(&mut pager, "idx_t_col").unwrap().unwrap();
        assert_eq!(retrieved.column_name, "col");
        assert!(retrieved.is_unique);

        let indexes = catalog.get_indexes_for_table(&mut pager, "t").unwrap();
        assert_eq!(indexes.len(), 1);
    }

    #[test]
    fn test_catalog_persistence() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let catalog_root;
        {
            let mut pager = Pager::create(&db_path, &test_key()).unwrap();
            let mut catalog = SystemCatalog::create(&mut pager).unwrap();

            let columns = vec![
                ColumnDef::new("id", DataType::Int64).primary_key(),
                ColumnDef::new("name", DataType::Varchar),
            ];
            catalog.create_table(&mut pager, "users", columns).unwrap();
            catalog_root = catalog.root_page_id();
            pager.flush_meta().unwrap();
        }

        {
            let mut pager = Pager::open(&db_path, &test_key()).unwrap();
            let catalog = SystemCatalog::open(catalog_root);

            let table = catalog.get_table(&mut pager, "users").unwrap().unwrap();
            assert_eq!(table.name, "users");
            assert_eq!(table.columns.len(), 2);
        }
    }
}
