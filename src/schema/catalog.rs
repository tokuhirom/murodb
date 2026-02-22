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
use crate::storage::page_store::PageStore;

/// Table definition.
#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub pk_columns: Vec<String>,
    pub data_btree_root: PageId,
    pub next_rowid: i64,
    /// Row format version: 0 = legacy (no prefix), 1 = u16 column count prefix.
    /// Defaults to 0 for tables created before this field was added (backward compat).
    pub row_format_version: u8,
    /// Last analyzed approximate row count (0 means unknown / not analyzed).
    pub stats_row_count: u64,
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
        // pk_columns (backward-compatible: 0x00=none, 0x01=single, 0x02=composite)
        match self.pk_columns.len() {
            0 => buf.push(0),
            1 => {
                buf.push(1);
                let pk_bytes = self.pk_columns[0].as_bytes();
                buf.extend_from_slice(&(pk_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(pk_bytes);
            }
            n => {
                buf.push(2);
                buf.extend_from_slice(&(n as u16).to_le_bytes());
                for pk in &self.pk_columns {
                    let pk_bytes = pk.as_bytes();
                    buf.extend_from_slice(&(pk_bytes.len() as u16).to_le_bytes());
                    buf.extend_from_slice(pk_bytes);
                }
            }
        }
        // data_btree_root
        buf.extend_from_slice(&self.data_btree_root.to_le_bytes());
        // next_rowid
        buf.extend_from_slice(&self.next_rowid.to_le_bytes());
        // row_format_version
        buf.push(self.row_format_version);
        // stats_row_count
        buf.extend_from_slice(&self.stats_row_count.to_le_bytes());
        buf
    }

    /// Deserialize table definition.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        let mut offset = 0;

        // name
        if data.len() < offset + 2 {
            return None;
        }
        let name_len = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        let name = String::from_utf8(data[offset..offset + name_len].to_vec()).ok()?;
        offset += name_len;

        // column count
        if data.len() < offset + 2 {
            return None;
        }
        let col_count = u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        // columns
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if data.len() < offset + 2 {
                return None;
            }
            let col_bytes_len =
                u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            let (col, _) = ColumnDef::deserialize(&data[offset..offset + col_bytes_len])?;
            columns.push(col);
            offset += col_bytes_len;
        }

        // pk_columns (backward-compatible: 0x00=none, 0x01=single, 0x02=composite)
        if data.len() < offset + 1 {
            return None;
        }
        let pk_tag = data[offset];
        offset += 1;
        let pk_columns = match pk_tag {
            0 => Vec::new(),
            1 => {
                if data.len() < offset + 2 {
                    return None;
                }
                let pk_len =
                    u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;
                let pk = String::from_utf8(data[offset..offset + pk_len].to_vec()).ok()?;
                offset += pk_len;
                vec![pk]
            }
            2 => {
                if data.len() < offset + 2 {
                    return None;
                }
                let count =
                    u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
                offset += 2;
                let mut pks = Vec::with_capacity(count);
                for _ in 0..count {
                    if data.len() < offset + 2 {
                        return None;
                    }
                    let pk_len =
                        u16::from_le_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
                    offset += 2;
                    let pk = String::from_utf8(data[offset..offset + pk_len].to_vec()).ok()?;
                    offset += pk_len;
                    pks.push(pk);
                }
                pks
            }
            _ => return None,
        };

        // data_btree_root
        if data.len() < offset + 8 {
            return None;
        }
        let data_btree_root = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        offset += 8;

        // next_rowid (optional for backward compat, defaults to 0)
        let next_rowid = if data.len() >= offset + 8 {
            let v = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            v
        } else {
            0
        };

        // row_format_version (optional, defaults to 0 for old tables)
        let row_format_version = if data.len() > offset { data[offset] } else { 0 };
        if data.len() > offset {
            offset += 1;
        }

        // stats_row_count (optional, defaults to 0 for old tables)
        let stats_row_count = if data.len() >= offset + 8 {
            u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
        } else {
            0
        };

        Some(TableDef {
            name,
            columns,
            pk_columns,
            data_btree_root,
            next_rowid,
            row_format_version,
            stats_row_count,
        })
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get PK column index (for single-column PK).
    pub fn pk_column_index(&self) -> Option<usize> {
        if self.pk_columns.len() == 1 {
            self.column_index(&self.pk_columns[0])
        } else {
            None
        }
    }

    /// Get indices of all PK columns.
    pub fn pk_column_indices(&self) -> Vec<usize> {
        self.pk_columns
            .iter()
            .filter_map(|pk| self.column_index(pk))
            .collect()
    }

    /// Whether this table has a composite (multi-column) primary key.
    pub fn is_composite_pk(&self) -> bool {
        self.pk_columns.len() > 1
    }
}

/// System catalog managing table and index definitions.
pub struct SystemCatalog {
    catalog_btree: BTree,
}

impl SystemCatalog {
    /// Create a new system catalog with a fresh B-tree.
    pub fn create(pager: &mut impl PageStore) -> Result<Self> {
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

    /// Get a mutable reference to the catalog B-tree (for direct index updates).
    pub fn catalog_btree_mut(&mut self) -> &mut BTree {
        &mut self.catalog_btree
    }

    /// Create a table. Returns the table definition with the allocated B-tree root.
    pub fn create_table(
        &mut self,
        pager: &mut impl PageStore,
        name: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<TableDef> {
        // Check if table already exists
        let key = format!("table:{}", name);
        if self.catalog_btree.search(pager, key.as_bytes())?.is_some() {
            return Err(MuroError::Schema(format!(
                "Table '{}' already exists",
                name
            )));
        }

        // Find PK columns; if none, inject a hidden _rowid column
        let pk_names: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();
        let (columns, pk_columns) = if !pk_names.is_empty() {
            (columns, pk_names)
        } else {
            use crate::types::DataType;
            let rowid_col = ColumnDef::new("_rowid", DataType::BigInt)
                .primary_key()
                .hidden();
            let mut cols = vec![rowid_col];
            cols.extend(columns);
            (cols, vec!["_rowid".to_string()])
        };

        // Allocate a B-tree for the table data
        let data_btree = BTree::create(pager)?;
        let data_btree_root = data_btree.root_page_id();

        let table_def = TableDef {
            name: name.to_string(),
            columns,
            pk_columns,
            data_btree_root,
            next_rowid: 0,
            row_format_version: 1,
            stats_row_count: 0,
        };

        // Store in catalog
        let serialized = table_def.serialize();
        self.catalog_btree
            .insert(pager, key.as_bytes(), &serialized)?;

        Ok(table_def)
    }

    /// Get a table definition by name.
    pub fn get_table(&self, pager: &mut impl PageStore, name: &str) -> Result<Option<TableDef>> {
        let key = format!("table:{}", name);
        match self.catalog_btree.search(pager, key.as_bytes())? {
            Some(data) => Ok(TableDef::deserialize(&data)),
            None => Ok(None),
        }
    }

    /// Update a table definition.
    pub fn update_table(&mut self, pager: &mut impl PageStore, table_def: &TableDef) -> Result<()> {
        let key = format!("table:{}", table_def.name);
        let serialized = table_def.serialize();
        self.catalog_btree
            .insert(pager, key.as_bytes(), &serialized)?;
        Ok(())
    }

    /// Create an index definition and store it in the catalog.
    pub fn create_index(
        &mut self,
        pager: &mut impl PageStore,
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
        self.catalog_btree
            .insert(pager, key.as_bytes(), &serialized)?;
        Ok(index_def)
    }

    /// Get an index definition by name.
    pub fn get_index(&self, pager: &mut impl PageStore, name: &str) -> Result<Option<IndexDef>> {
        let key = format!("index:{}", name);
        match self.catalog_btree.search(pager, key.as_bytes())? {
            Some(data) => Ok(IndexDef::deserialize(&data).map(|(idx, _)| idx)),
            None => Ok(None),
        }
    }

    /// Update an existing index definition.
    pub fn update_index(&mut self, pager: &mut impl PageStore, index_def: &IndexDef) -> Result<()> {
        let key = format!("index:{}", index_def.name);
        let serialized = index_def.serialize();
        self.catalog_btree
            .insert(pager, key.as_bytes(), &serialized)?;
        Ok(())
    }

    /// Get all indexes for a table.
    pub fn get_indexes_for_table(
        &self,
        pager: &mut impl PageStore,
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

    /// Rename a table: delete old key, update name, insert with new key.
    /// Also updates all indexes for the table.
    pub fn rename_table(
        &mut self,
        pager: &mut impl PageStore,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        // Check old table exists
        let mut table_def = self
            .get_table(pager, old_name)?
            .ok_or_else(|| MuroError::Schema(format!("Table '{}' does not exist", old_name)))?;

        // Check new name doesn't exist
        if self.get_table(pager, new_name)?.is_some() {
            return Err(MuroError::Schema(format!(
                "Table '{}' already exists",
                new_name
            )));
        }

        // Delete old key
        let old_key = format!("table:{}", old_name);
        self.catalog_btree.delete(pager, old_key.as_bytes())?;

        // Update name and insert with new key
        table_def.name = new_name.to_string();
        let new_key = format!("table:{}", new_name);
        let serialized = table_def.serialize();
        self.catalog_btree
            .insert(pager, new_key.as_bytes(), &serialized)?;

        // Update all indexes for this table
        let indexes = self.get_indexes_for_table(pager, old_name)?;
        for mut idx in indexes {
            let idx_key = format!("index:{}", idx.name);
            idx.table_name = new_name.to_string();
            let idx_serialized = idx.serialize();
            self.catalog_btree
                .insert(pager, idx_key.as_bytes(), &idx_serialized)?;
        }

        Ok(())
    }

    /// Delete a table from the catalog.
    pub fn delete_table(&mut self, pager: &mut impl PageStore, name: &str) -> Result<()> {
        let key = format!("table:{}", name);
        if self.catalog_btree.search(pager, key.as_bytes())?.is_none() {
            return Err(MuroError::Schema(format!(
                "Table '{}' does not exist",
                name
            )));
        }
        self.catalog_btree.delete(pager, key.as_bytes())?;
        Ok(())
    }

    /// Delete an index from the catalog.
    pub fn delete_index(&mut self, pager: &mut impl PageStore, name: &str) -> Result<()> {
        let key = format!("index:{}", name);
        if self.catalog_btree.search(pager, key.as_bytes())?.is_none() {
            return Err(MuroError::Schema(format!(
                "Index '{}' does not exist",
                name
            )));
        }
        self.catalog_btree.delete(pager, key.as_bytes())?;
        Ok(())
    }

    /// Delete all indexes for a table.
    pub fn delete_indexes_for_table(
        &mut self,
        pager: &mut impl PageStore,
        table_name: &str,
    ) -> Result<()> {
        let indexes = self.get_indexes_for_table(pager, table_name)?;
        for idx in indexes {
            let key = format!("index:{}", idx.name);
            self.catalog_btree.delete(pager, key.as_bytes())?;
        }
        Ok(())
    }

    /// List all table names.
    pub fn list_tables(&self, pager: &mut impl PageStore) -> Result<Vec<String>> {
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
    use crate::storage::pager::Pager;
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
                ColumnDef::new("id", DataType::BigInt).primary_key(),
                ColumnDef::new("name", DataType::Varchar(None)),
                ColumnDef::new("data", DataType::Varbinary(None)),
            ],
            pk_columns: vec!["id".to_string()],
            data_btree_root: 42,
            next_rowid: 0,
            row_format_version: 1,
            stats_row_count: 0,
        };

        let bytes = table.serialize();
        let table2 = TableDef::deserialize(&bytes).unwrap();
        assert_eq!(table2.name, "users");
        assert_eq!(table2.columns.len(), 3);
        assert_eq!(table2.pk_columns, vec!["id".to_string()]);
        assert_eq!(table2.data_btree_root, 42);
        assert_eq!(table2.row_format_version, 1);
    }

    #[test]
    fn test_catalog_create_and_get_table() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");

        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut catalog = SystemCatalog::create(&mut pager).unwrap();

        let columns = vec![
            ColumnDef::new("id", DataType::BigInt).primary_key(),
            ColumnDef::new("body", DataType::Varchar(None)),
        ];

        let table_def = catalog.create_table(&mut pager, "posts", columns).unwrap();
        assert_eq!(table_def.name, "posts");
        assert_eq!(table_def.pk_columns, vec!["id".to_string()]);

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

        let columns = vec![ColumnDef::new("id", DataType::BigInt).primary_key()];
        catalog
            .create_table(&mut pager, "t", columns.clone())
            .unwrap();
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
            column_names: vec!["col".to_string()],
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: 99,
            stats_distinct_keys: 0,
            fts_stop_filter: false,
            fts_stop_df_ratio_ppm: 0,
        };

        catalog.create_index(&mut pager, idx).unwrap();

        let retrieved = catalog.get_index(&mut pager, "idx_t_col").unwrap().unwrap();
        assert_eq!(retrieved.column_names, vec!["col".to_string()]);
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
                ColumnDef::new("id", DataType::BigInt).primary_key(),
                ColumnDef::new("name", DataType::Varchar(None)),
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
