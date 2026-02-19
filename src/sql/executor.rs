/// SQL executor: executes statements against the B-tree storage.

use crate::btree::key_encoding::encode_i64;
use crate::btree::ops::BTree;
use crate::error::{MuroError, Result};
use crate::schema::catalog::{SystemCatalog, TableDef};
use crate::schema::column::ColumnDef;
use crate::schema::index::{IndexDef, IndexType};
use crate::sql::ast::*;
use crate::sql::eval::{eval_expr, is_truthy};
use crate::sql::parser::parse_sql;
use crate::sql::planner::{plan_select, Plan};
use crate::storage::pager::Pager;
use crate::types::{DataType, Value};

/// A result row.
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<(String, Value)>,
}

impl Row {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// Execution result.
#[derive(Debug)]
pub enum ExecResult {
    Rows(Vec<Row>),
    RowsAffected(u64),
    Ok,
}

/// Execute a SQL string.
pub fn execute(
    sql: &str,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
    execute_statement(&stmt, pager, catalog)
}

fn execute_statement(
    stmt: &Statement,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(ct, pager, catalog),
        Statement::CreateIndex(ci) => exec_create_index(ci, pager, catalog),
        Statement::CreateFulltextIndex(_fi) => {
            // FTS index creation is handled in step 9
            Ok(ExecResult::Ok)
        }
        Statement::Insert(ins) => exec_insert(ins, pager, catalog),
        Statement::Select(sel) => exec_select(sel, pager, catalog),
        Statement::Update(upd) => exec_update(upd, pager, catalog),
        Statement::Delete(del) => exec_delete(del, pager, catalog),
    }
}

fn exec_create_table(
    ct: &CreateTable,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let columns: Vec<ColumnDef> = ct
        .columns
        .iter()
        .map(|cs| {
            let mut col = ColumnDef::new(&cs.name, cs.data_type);
            if cs.is_primary_key {
                col = col.primary_key();
            }
            if cs.is_unique {
                col = col.unique();
            }
            if !cs.is_nullable {
                col = col.not_null();
            }
            col
        })
        .collect();

    let _table_def = catalog.create_table(pager, &ct.table_name, columns)?;

    // Create unique indexes for columns marked UNIQUE (non-PK)
    for col_spec in &ct.columns {
        if col_spec.is_unique && !col_spec.is_primary_key {
            let idx_btree = BTree::create(pager)?;
            let idx_def = IndexDef {
                name: format!("auto_unique_{}_{}", ct.table_name, col_spec.name),
                table_name: ct.table_name.clone(),
                column_name: col_spec.name.clone(),
                index_type: IndexType::BTree,
                is_unique: true,
                btree_root: idx_btree.root_page_id(),
            };
            catalog.create_index(pager, idx_def)?;
        }
    }

    Ok(ExecResult::Ok)
}

fn exec_create_index(
    ci: &CreateIndex,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &ci.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", ci.table_name)))?;

    // Verify column exists
    if table_def.column_index(&ci.column_name).is_none() {
        return Err(MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            ci.column_name, ci.table_name
        )));
    }

    let idx_btree = BTree::create(pager)?;

    // If unique, scan existing data for duplicates
    if ci.is_unique {
        let data_btree = BTree::open(table_def.data_btree_root);
        let col_idx = table_def.column_index(&ci.column_name).unwrap();

        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        data_btree.scan(pager, |_k, v| {
            let row_values = deserialize_row(v, &table_def.columns)?;
            if col_idx < row_values.len() {
                let val = &row_values[col_idx];
                if !val.is_null() {
                    let encoded = encode_value(val);
                    if seen_keys.contains(&encoded) {
                        return Err(MuroError::UniqueViolation(format!(
                            "Duplicate value in column '{}'",
                            ci.column_name
                        )));
                    }
                    seen_keys.push(encoded);
                }
            }
            Ok(true)
        })?;
    }

    // Collect existing data for index building
    let data_btree = BTree::open(table_def.data_btree_root);
    let col_idx = table_def.column_index(&ci.column_name).unwrap();
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    data_btree.scan(pager, |pk_key, v| {
        let row_values = deserialize_row(v, &table_def.columns)?;
        if col_idx < row_values.len() {
            let val = &row_values[col_idx];
            if !val.is_null() {
                let idx_key = encode_value(val);
                entries.push((idx_key, pk_key.to_vec()));
            }
        }
        Ok(true)
    })?;

    // Build index from collected entries
    let mut idx_btree_mut = BTree::open(idx_btree.root_page_id());
    for (idx_key, pk_key) in &entries {
        idx_btree_mut.insert(pager, idx_key, pk_key)?;
    }

    let idx_def = IndexDef {
        name: ci.index_name.clone(),
        table_name: ci.table_name.clone(),
        column_name: ci.column_name.clone(),
        index_type: IndexType::BTree,
        is_unique: ci.is_unique,
        btree_root: idx_btree_mut.root_page_id(),
    };
    catalog.create_index(pager, idx_def)?;

    Ok(ExecResult::Ok)
}

fn exec_insert(
    ins: &Insert,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &ins.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", ins.table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &ins.table_name)?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut rows_inserted = 0u64;

    for value_row in &ins.values {
        let values = resolve_insert_values(&table_def, &ins.columns, value_row)?;

        // Get PK value
        let pk_idx = table_def.pk_column_index().ok_or_else(|| {
            MuroError::Execution("Table has no primary key".into())
        })?;
        let pk_value = &values[pk_idx];
        let pk_key = encode_value(pk_value);

        // Check PK uniqueness
        if data_btree.search(pager, &pk_key)?.is_some() {
            return Err(MuroError::UniqueViolation(format!(
                "Duplicate primary key: {}",
                pk_value
            )));
        }

        // Check unique index constraints
        for idx in &indexes {
            if idx.is_unique {
                let col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!("Column '{}' not found", idx.column_name))
                })?;
                let val = &values[col_idx];
                if !val.is_null() {
                    let idx_key = encode_value(val);
                    let idx_btree = BTree::open(idx.btree_root);
                    if idx_btree.search(pager, &idx_key)?.is_some() {
                        return Err(MuroError::UniqueViolation(format!(
                            "Duplicate value in unique column '{}'",
                            idx.column_name
                        )));
                    }
                }
            }
        }

        // Serialize row and insert into data B-tree
        let row_data = serialize_row(&values, &table_def.columns);
        data_btree.insert(pager, &pk_key, &row_data)?;

        // Update secondary indexes
        for idx in &indexes {
            if idx.index_type == IndexType::BTree {
                let col_idx = table_def.column_index(&idx.column_name).unwrap();
                let val = &values[col_idx];
                if !val.is_null() {
                    let idx_key = encode_value(val);
                    let mut idx_btree = BTree::open(idx.btree_root);
                    idx_btree.insert(pager, &idx_key, &pk_key)?;
                }
            }
        }

        // Update data_btree_root if it changed (due to splits)
        if data_btree.root_page_id() != table_def.data_btree_root {
            let mut updated_table = table_def.clone();
            updated_table.data_btree_root = data_btree.root_page_id();
            catalog.update_table(pager, &updated_table)?;
        }

        rows_inserted += 1;
    }

    Ok(ExecResult::RowsAffected(rows_inserted))
}

fn exec_select(
    sel: &Select,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &sel.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", sel.table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &sel.table_name)?;
    let index_columns: Vec<(String, String)> = indexes
        .iter()
        .map(|idx| (idx.name.clone(), idx.column_name.clone()))
        .collect();

    let plan = plan_select(
        &sel.table_name,
        table_def.pk_column.as_deref(),
        &index_columns,
        &sel.where_clause,
    );

    let mut rows: Vec<Row> = Vec::new();

    match plan {
        Plan::PkSeek { key_expr, .. } => {
            let key_val = eval_expr(&key_expr, &|_| None)?;
            let pk_key = encode_value(&key_val);
            let data_btree = BTree::open(table_def.data_btree_root);
            if let Some(data) = data_btree.search(pager, &pk_key)? {
                let values = deserialize_row(&data, &table_def.columns)?;
                let row = build_row(&table_def, &values, &sel.columns)?;
                // Apply additional WHERE predicates if any
                if matches_where(&sel.where_clause, &table_def, &values)? {
                    rows.push(row);
                }
            }
        }
        Plan::IndexSeek { index_name, key_expr, .. } => {
            let key_val = eval_expr(&key_expr, &|_| None)?;
            let idx_key = encode_value(&key_val);
            let idx = indexes.iter().find(|i| i.name == index_name).unwrap();
            let idx_btree = BTree::open(idx.btree_root);
            if let Some(pk_key) = idx_btree.search(pager, &idx_key)? {
                let data_btree = BTree::open(table_def.data_btree_root);
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row(&data, &table_def.columns)?;
                    if matches_where(&sel.where_clause, &table_def, &values)? {
                        let row = build_row(&table_def, &values, &sel.columns)?;
                        rows.push(row);
                    }
                }
            }
        }
        Plan::FullScan { .. } => {
            let data_btree = BTree::open(table_def.data_btree_root);
            data_btree.scan(pager, |_k, v| {
                let values = deserialize_row(v, &table_def.columns)?;
                if matches_where(&sel.where_clause, &table_def, &values)? {
                    let row = build_row(&table_def, &values, &sel.columns)?;
                    rows.push(row);
                }
                Ok(true)
            })?;
        }
        Plan::FtsScan { .. } => {
            // FTS scan - handled in FTS steps
            // For now, fall back to full scan
            let data_btree = BTree::open(table_def.data_btree_root);
            data_btree.scan(pager, |_k, v| {
                let values = deserialize_row(v, &table_def.columns)?;
                if matches_where(&sel.where_clause, &table_def, &values)? {
                    let row = build_row(&table_def, &values, &sel.columns)?;
                    rows.push(row);
                }
                Ok(true)
            })?;
        }
    }

    // ORDER BY
    if let Some(order_items) = &sel.order_by {
        rows.sort_by(|a, b| {
            for item in order_items {
                if let Expr::ColumnRef(col) = &item.expr {
                    let va = a.get(col);
                    let vb = b.get(col);
                    let ord = cmp_values(va, vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if item.descending { ord.reverse() } else { ord };
                    }
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // LIMIT
    if let Some(limit) = sel.limit {
        rows.truncate(limit as usize);
    }

    Ok(ExecResult::Rows(rows))
}

fn exec_update(
    upd: &Update,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &upd.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", upd.table_name)))?;

    let data_btree = BTree::open(table_def.data_btree_root);

    // Collect rows to update (to avoid modifying during scan)
    let mut to_update: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    data_btree.scan(pager, |k, v| {
        let values = deserialize_row(v, &table_def.columns)?;
        if matches_where(&upd.where_clause, &table_def, &values)? {
            to_update.push((k.to_vec(), values));
        }
        Ok(true)
    })?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut count = 0u64;

    for (pk_key, mut values) in to_update {
        // Apply assignments
        for (col_name, expr) in &upd.assignments {
            let col_idx = table_def.column_index(col_name).ok_or_else(|| {
                MuroError::Execution(format!("Unknown column: {}", col_name))
            })?;
            let new_val = eval_expr(expr, &|name| {
                table_def.column_index(name).and_then(|i| values.get(i).cloned())
            })?;
            values[col_idx] = new_val;
        }

        let row_data = serialize_row(&values, &table_def.columns);
        data_btree.insert(pager, &pk_key, &row_data)?;
        count += 1;
    }

    Ok(ExecResult::RowsAffected(count))
}

fn exec_delete(
    del: &Delete,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &del.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", del.table_name)))?;

    let data_btree = BTree::open(table_def.data_btree_root);

    // Collect keys to delete
    let mut to_delete: Vec<Vec<u8>> = Vec::new();
    data_btree.scan(pager, |k, v| {
        let values = deserialize_row(v, &table_def.columns)?;
        if matches_where(&del.where_clause, &table_def, &values)? {
            to_delete.push(k.to_vec());
        }
        Ok(true)
    })?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut count = 0u64;

    for pk_key in to_delete {
        data_btree.delete(pager, &pk_key)?;
        count += 1;
    }

    Ok(ExecResult::RowsAffected(count))
}

// --- Row serialization ---
// Format: [null_bitmap][value1][value2]...
// Each value: for INT64: 8 bytes; for VARCHAR/VARBINARY: u32 len + bytes

pub fn serialize_row(values: &[Value], columns: &[ColumnDef]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Null bitmap (1 bit per column, packed into bytes)
    let bitmap_bytes = (columns.len() + 7) / 8;
    let mut bitmap = vec![0u8; bitmap_bytes];
    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend_from_slice(&bitmap);

    // Values
    for (_i, val) in values.iter().enumerate() {
        if val.is_null() {
            continue;
        }
        match val {
            Value::Int64(n) => buf.extend_from_slice(&n.to_le_bytes()),
            Value::Varchar(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Varbinary(b) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Null => {} // already skipped
        }
    }

    buf
}

pub fn deserialize_row(data: &[u8], columns: &[ColumnDef]) -> Result<Vec<Value>> {
    let bitmap_bytes = (columns.len() + 7) / 8;
    if data.len() < bitmap_bytes {
        return Err(MuroError::InvalidPage);
    }

    let bitmap = &data[..bitmap_bytes];
    let mut offset = bitmap_bytes;
    let mut values = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let is_null = bitmap[i / 8] & (1 << (i % 8)) != 0;
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match col.data_type {
            DataType::Int64 => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::Int64(n));
                offset += 8;
            }
            DataType::Varchar => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let s = String::from_utf8(data[offset..offset + len].to_vec())
                    .map_err(|_| MuroError::InvalidPage)?;
                values.push(Value::Varchar(s));
                offset += len;
            }
            DataType::Varbinary => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                values.push(Value::Varbinary(data[offset..offset + len].to_vec()));
                offset += len;
            }
        }
    }

    Ok(values)
}

/// Encode a Value for use as a B-tree key.
pub fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Int64(n) => encode_i64(*n).to_vec(),
        Value::Varchar(s) => s.as_bytes().to_vec(),
        Value::Varbinary(b) => b.clone(),
        Value::Null => Vec::new(),
    }
}

fn resolve_insert_values(
    table_def: &TableDef,
    explicit_columns: &Option<Vec<String>>,
    exprs: &[Expr],
) -> Result<Vec<Value>> {
    let mut values = vec![Value::Null; table_def.columns.len()];

    match explicit_columns {
        Some(cols) => {
            if cols.len() != exprs.len() {
                return Err(MuroError::Execution(
                    "Column count doesn't match value count".into(),
                ));
            }
            for (col_name, expr) in cols.iter().zip(exprs.iter()) {
                let idx = table_def.column_index(col_name).ok_or_else(|| {
                    MuroError::Execution(format!("Unknown column: {}", col_name))
                })?;
                values[idx] = eval_expr(expr, &|_| None)?;
            }
        }
        None => {
            if exprs.len() != table_def.columns.len() {
                return Err(MuroError::Execution(
                    "Value count doesn't match column count".into(),
                ));
            }
            for (i, expr) in exprs.iter().enumerate() {
                values[i] = eval_expr(expr, &|_| None)?;
            }
        }
    }

    Ok(values)
}

fn matches_where(
    where_clause: &Option<Expr>,
    table_def: &TableDef,
    values: &[Value],
) -> Result<bool> {
    match where_clause {
        None => Ok(true),
        Some(expr) => {
            let result = eval_expr(expr, &|name| {
                table_def
                    .column_index(name)
                    .and_then(|i| values.get(i).cloned())
            })?;
            Ok(is_truthy(&result))
        }
    }
}

fn build_row(
    table_def: &TableDef,
    values: &[Value],
    select_columns: &[SelectColumn],
) -> Result<Row> {
    let mut row_values = Vec::new();

    for sel_col in select_columns {
        match sel_col {
            SelectColumn::Star => {
                for (i, col) in table_def.columns.iter().enumerate() {
                    let val = values.get(i).cloned().unwrap_or(Value::Null);
                    row_values.push((col.name.clone(), val));
                }
            }
            SelectColumn::Expr(expr, alias) => {
                let val = eval_expr(expr, &|name| {
                    table_def.column_index(name).and_then(|i| values.get(i).cloned())
                })?;
                let name = alias.clone().unwrap_or_else(|| {
                    match expr {
                        Expr::ColumnRef(n) => n.clone(),
                        _ => "?column?".to_string(),
                    }
                });
                row_values.push((name, val));
            }
        }
    }

    Ok(Row { values: row_values })
}

fn cmp_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(Value::Int64(a)), Some(Value::Int64(b))) => a.cmp(b),
        (Some(Value::Varchar(a)), Some(Value::Varchar(b))) => a.cmp(b),
        (Some(Value::Null), _) | (None, _) => std::cmp::Ordering::Less,
        (_, Some(Value::Null)) | (_, None) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    fn setup() -> (Pager, SystemCatalog, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let catalog = SystemCatalog::create(&mut pager).unwrap();
        (pager, catalog, dir)
    }

    #[test]
    fn test_create_table_and_insert() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();

        execute("INSERT INTO t (id, name) VALUES (1, 'Alice')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t (id, name) VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
            assert_eq!(rows[1].get("name"), Some(&Value::Varchar("Bob".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_select_where() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, 'Alice')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (3, 'Charlie')", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t WHERE id = 2", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_update() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, 'Alice')", &mut pager, &mut catalog).unwrap();

        let result = execute("UPDATE t SET name = 'Alicia' WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 1);
        }

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alicia".into())));
        }
    }

    #[test]
    fn test_delete() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, 'Alice')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        execute("DELETE FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));
        }
    }

    #[test]
    fn test_order_by_and_limit() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (3, 'Charlie')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, 'Alice')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t ORDER BY id DESC LIMIT 2", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("id"), Some(&Value::Int64(3)));
            assert_eq!(rows[1].get("id"), Some(&Value::Int64(2)));
        }
    }

    #[test]
    fn test_unique_constraint() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, email VARCHAR UNIQUE)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, 'a@b.com')", &mut pager, &mut catalog).unwrap();

        // Duplicate PK
        let result = execute("INSERT INTO t VALUES (1, 'x@y.com')", &mut pager, &mut catalog);
        assert!(result.is_err());

        // Duplicate UNIQUE
        let result = execute("INSERT INTO t VALUES (2, 'a@b.com')", &mut pager, &mut catalog);
        assert!(result.is_err());

        // Different value should work
        execute("INSERT INTO t VALUES (2, 'c@d.com')", &mut pager, &mut catalog).unwrap();
    }

    #[test]
    fn test_null_values() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Null));
        }
    }

    #[test]
    fn test_many_inserts() {
        let (mut pager, mut catalog, _dir) = setup();

        execute("CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)", &mut pager, &mut catalog).unwrap();

        for i in 0..100 {
            let sql = format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i);
            execute(&sql, &mut pager, &mut catalog).unwrap();
        }

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 100);
        }
    }
}
