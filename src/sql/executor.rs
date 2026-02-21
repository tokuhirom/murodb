/// SQL executor: executes statements against the B-tree storage.
use std::collections::HashMap;
use std::collections::HashSet;

use crate::btree::key_encoding::{encode_i16, encode_i32, encode_i64, encode_i8};
use crate::btree::ops::BTree;
use crate::error::{MuroError, Result};
use crate::schema::catalog::{SystemCatalog, TableDef};
use crate::schema::column::{ColumnDef, DefaultValue};
use crate::schema::index::{IndexDef, IndexType};
use crate::sql::ast::*;
use crate::sql::eval::{eval_expr, is_truthy};
use crate::sql::parser::parse_sql;
use crate::sql::planner::{plan_select, Plan};
use crate::storage::page_store::PageStore;
use crate::types::{DataType, Value, ValueKey};

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
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
    execute_statement(&stmt, pager, catalog)
}

pub fn execute_statement(
    stmt: &Statement,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(ct, pager, catalog),
        Statement::CreateIndex(ci) => exec_create_index(ci, pager, catalog),
        Statement::CreateFulltextIndex(_fi) => {
            // TODO: FTS index integration with catalog is not yet complete.
            // FTS operations (create/update/delete) need catalog metadata support.
            Err(MuroError::Execution(
                "CREATE FULLTEXT INDEX is not yet fully integrated with the SQL engine".into(),
            ))
        }
        Statement::DropTable(dt) => exec_drop_table(dt, pager, catalog),
        Statement::DropIndex(di) => exec_drop_index(di, pager, catalog),
        Statement::AlterTable(at) => exec_alter_table(at, pager, catalog),
        Statement::RenameTable(rt) => exec_rename_table(rt, pager, catalog),
        Statement::Insert(ins) => exec_insert(ins, pager, catalog),
        Statement::Select(sel) => exec_select(sel, pager, catalog),
        Statement::Update(upd) => exec_update(upd, pager, catalog),
        Statement::Delete(del) => exec_delete(del, pager, catalog),
        Statement::ShowTables => exec_show_tables(pager, catalog),
        Statement::ShowCreateTable(name) => exec_show_create_table(name, pager, catalog),
        Statement::Describe(name) => exec_describe(name, pager, catalog),
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::ShowCheckpointStats
        | Statement::ShowDatabaseStats => Err(MuroError::Execution(
            "BEGIN/COMMIT/ROLLBACK/SHOW CHECKPOINT STATS/SHOW DATABASE STATS must be handled by Session".into(),
        )),
    }
}

fn exec_create_table(
    ct: &CreateTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Check IF NOT EXISTS
    if ct.if_not_exists && catalog.get_table(pager, &ct.table_name)?.is_some() {
        return Ok(ExecResult::Ok);
    }

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
            if cs.auto_increment {
                col = col.with_auto_increment();
            }
            if let Some(default_expr) = &cs.default_value {
                col.default_value = ast_expr_to_default(default_expr);
            }
            if let Some(check) = &cs.check_expr {
                col.check_expr = Some(expr_to_string(check));
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

/// Convert an AST expression (from DEFAULT clause) to a DefaultValue for storage.
fn ast_expr_to_default(expr: &Expr) -> Option<DefaultValue> {
    match expr {
        Expr::IntLiteral(n) => Some(DefaultValue::Integer(*n)),
        Expr::StringLiteral(s) => Some(DefaultValue::String(s.clone())),
        Expr::Null => Some(DefaultValue::Null),
        _ => None,
    }
}

/// Convert an AST expression to a string representation for storage (CHECK constraints).
fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::IntLiteral(n) => n.to_string(),
        Expr::StringLiteral(s) => format!("'{}'", s),
        Expr::Null => "NULL".to_string(),
        Expr::ColumnRef(name) => name.clone(),
        Expr::BinaryOp { left, op, right } => {
            let op_str = match op {
                BinaryOp::Eq => "=",
                BinaryOp::Ne => "!=",
                BinaryOp::Lt => "<",
                BinaryOp::Gt => ">",
                BinaryOp::Le => "<=",
                BinaryOp::Ge => ">=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Mod => "%",
            };
            format!(
                "{} {} {}",
                expr_to_string(left),
                op_str,
                expr_to_string(right)
            )
        }
        Expr::UnaryOp { op, operand } => {
            let op_str = match op {
                UnaryOp::Not => "NOT ",
                UnaryOp::Neg => "-",
            };
            format!("{}{}", op_str, expr_to_string(operand))
        }
        _ => "?".to_string(),
    }
}

fn exec_create_index(
    ci: &CreateIndex,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Check IF NOT EXISTS
    if ci.if_not_exists && catalog.get_index(pager, &ci.index_name)?.is_some() {
        return Ok(ExecResult::Ok);
    }

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

    let col_idx = table_def.column_index(&ci.column_name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            ci.column_name, ci.table_name
        ))
    })?;
    let col_data_type = table_def.columns[col_idx].data_type;

    // If unique, scan existing data for duplicates
    if ci.is_unique {
        let data_btree = BTree::open(table_def.data_btree_root);

        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        data_btree.scan(pager, |_k, v| {
            let row_values = deserialize_row(v, &table_def.columns)?;
            if col_idx < row_values.len() {
                let val = &row_values[col_idx];
                if !val.is_null() {
                    let encoded = encode_value(val, &col_data_type);
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
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    data_btree.scan(pager, |pk_key, v| {
        let row_values = deserialize_row(v, &table_def.columns)?;
        if col_idx < row_values.len() {
            let val = &row_values[col_idx];
            if !val.is_null() {
                let idx_key = encode_value(val, &col_data_type);
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

fn exec_drop_table(
    dt: &DropTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = match catalog.get_table(pager, &dt.table_name)? {
        Some(td) => td,
        None => {
            if dt.if_exists {
                return Ok(ExecResult::Ok);
            }
            return Err(MuroError::Schema(format!(
                "Table '{}' does not exist",
                dt.table_name
            )));
        }
    };

    // Free the data B-tree pages
    let data_btree = BTree::open(table_def.data_btree_root);
    let pages_to_free = data_btree.collect_all_pages(pager)?;
    for page_id in pages_to_free {
        pager.free_page(page_id);
    }

    // Free index B-tree pages
    let indexes = catalog.get_indexes_for_table(pager, &dt.table_name)?;
    for idx in &indexes {
        let idx_btree = BTree::open(idx.btree_root);
        let idx_pages = idx_btree.collect_all_pages(pager)?;
        for page_id in idx_pages {
            pager.free_page(page_id);
        }
    }

    // Delete all indexes for this table first
    catalog.delete_indexes_for_table(pager, &dt.table_name)?;
    // Delete the table
    catalog.delete_table(pager, &dt.table_name)?;

    Ok(ExecResult::Ok)
}

fn exec_drop_index(
    di: &DropIndex,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let idx_def = match catalog.get_index(pager, &di.index_name)? {
        Some(idx) => idx,
        None => {
            if di.if_exists {
                return Ok(ExecResult::Ok);
            }
            return Err(MuroError::Schema(format!(
                "Index '{}' does not exist",
                di.index_name
            )));
        }
    };

    // Free the index B-tree pages
    let idx_btree = BTree::open(idx_def.btree_root);
    let pages_to_free = idx_btree.collect_all_pages(pager)?;
    for page_id in pages_to_free {
        pager.free_page(page_id);
    }

    catalog.delete_index(pager, &di.index_name)?;
    Ok(ExecResult::Ok)
}

fn exec_alter_table(
    at: &AlterTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &at.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", at.table_name)))?;

    match &at.operation {
        AlterTableOp::AddColumn(col_spec) => {
            exec_alter_add_column(table_def, col_spec, pager, catalog)
        }
        AlterTableOp::DropColumn(col_name) => {
            exec_alter_drop_column(table_def, col_name, &at.table_name, pager, catalog)
        }
        AlterTableOp::ModifyColumn(col_spec) => {
            exec_alter_modify_column(table_def, col_spec, &at.table_name, pager, catalog)
        }
        AlterTableOp::ChangeColumn(old_name, col_spec) => exec_alter_change_column(
            table_def,
            old_name,
            col_spec,
            &at.table_name,
            pager,
            catalog,
        ),
    }
}

fn exec_alter_add_column(
    mut table_def: TableDef,
    col_spec: &ColumnSpec,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Validate: column doesn't already exist
    if table_def.column_index(&col_spec.name).is_some() {
        return Err(MuroError::Schema(format!(
            "Column '{}' already exists in table '{}'",
            col_spec.name, table_def.name
        )));
    }
    // Don't allow adding PK column
    if col_spec.is_primary_key {
        return Err(MuroError::Schema(
            "Cannot add a PRIMARY KEY column with ALTER TABLE".into(),
        ));
    }

    let mut col = ColumnDef::new(&col_spec.name, col_spec.data_type);
    if col_spec.is_unique {
        col = col.unique();
    }
    if !col_spec.is_nullable {
        col = col.not_null();
    }
    if col_spec.auto_increment {
        col = col.with_auto_increment();
    }
    if let Some(default_expr) = &col_spec.default_value {
        col.default_value = ast_expr_to_default(default_expr);
    }
    if let Some(check) = &col_spec.check_expr {
        col.check_expr = Some(expr_to_string(check));
    }

    table_def.columns.push(col);
    catalog.update_table(pager, &table_def)?;

    Ok(ExecResult::Ok)
}

fn exec_alter_drop_column(
    mut table_def: TableDef,
    col_name: &str,
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let col_idx = table_def.column_index(col_name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            col_name, table_name
        ))
    })?;

    // Can't drop PK column
    if table_def.columns[col_idx].is_primary_key {
        return Err(MuroError::Schema("Cannot drop PRIMARY KEY column".into()));
    }

    // Check if any index references this column
    let indexes = catalog.get_indexes_for_table(pager, table_name)?;
    for idx in &indexes {
        if idx.column_name == col_name {
            return Err(MuroError::Schema(format!(
                "Cannot drop column '{}': index '{}' depends on it",
                col_name, idx.name
            )));
        }
    }

    // Full table rewrite: scan all rows, remove the dropped column, re-insert
    let old_columns = table_def.columns.clone();
    let data_btree = BTree::open(table_def.data_btree_root);

    let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    data_btree.scan(pager, |k, v| {
        let row_values = deserialize_row(v, &old_columns)?;
        entries.push((k.to_vec(), row_values));
        Ok(true)
    })?;

    // Create new column list without the dropped column
    table_def.columns.remove(col_idx);

    // Rewrite all rows
    // Free old data pages and create a new B-tree
    let old_pages = data_btree.collect_all_pages(pager)?;
    for page_id in old_pages {
        pager.free_page(page_id);
    }
    let new_data_btree = BTree::create(pager)?;
    let mut new_btree = BTree::open(new_data_btree.root_page_id());

    for (key, mut row_values) in entries {
        row_values.remove(col_idx);
        let new_data = serialize_row(&row_values, &table_def.columns);
        new_btree.insert(pager, &key, &new_data)?;
    }

    table_def.data_btree_root = new_btree.root_page_id();
    catalog.update_table(pager, &table_def)?;

    Ok(ExecResult::Ok)
}

fn exec_alter_modify_column(
    mut table_def: TableDef,
    col_spec: &ColumnSpec,
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let col_idx = table_def.column_index(&col_spec.name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            col_spec.name, table_name
        ))
    })?;

    let old_col = &table_def.columns[col_idx];
    let type_changed = old_col.data_type != col_spec.data_type;

    if type_changed {
        // Full table rewrite with type coercion
        let old_columns = table_def.columns.clone();
        let data_btree = BTree::open(table_def.data_btree_root);

        let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
        data_btree.scan(pager, |k, v| {
            let row_values = deserialize_row(v, &old_columns)?;
            entries.push((k.to_vec(), row_values));
            Ok(true)
        })?;

        // Update column def
        update_column_def(&mut table_def.columns[col_idx], col_spec);

        // Rewrite with coerced values
        let old_pages = data_btree.collect_all_pages(pager)?;
        for page_id in old_pages {
            pager.free_page(page_id);
        }
        let new_data_btree = BTree::create(pager)?;
        let mut new_btree = BTree::open(new_data_btree.root_page_id());

        for (key, mut row_values) in entries {
            row_values[col_idx] = coerce_value(&row_values[col_idx], col_spec.data_type)?;
            let new_data = serialize_row(&row_values, &table_def.columns);
            new_btree.insert(pager, &key, &new_data)?;
        }

        table_def.data_btree_root = new_btree.root_page_id();
    } else {
        // Metadata-only change
        update_column_def(&mut table_def.columns[col_idx], col_spec);
    }

    catalog.update_table(pager, &table_def)?;
    Ok(ExecResult::Ok)
}

fn exec_alter_change_column(
    mut table_def: TableDef,
    old_name: &str,
    col_spec: &ColumnSpec,
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let col_idx = table_def.column_index(old_name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            old_name, table_name
        ))
    })?;

    let old_col = &table_def.columns[col_idx];
    let type_changed = old_col.data_type != col_spec.data_type;

    // Update any indexes referencing the old column name
    let indexes = catalog.get_indexes_for_table(pager, table_name)?;
    for mut idx in indexes {
        if idx.column_name == old_name {
            idx.column_name = col_spec.name.clone();
            // Re-store the index with updated column name
            let idx_key = format!("index:{}", idx.name);
            let idx_serialized = idx.serialize();
            catalog
                .catalog_btree_mut()
                .insert(pager, idx_key.as_bytes(), &idx_serialized)?;
        }
    }

    if type_changed {
        let old_columns = table_def.columns.clone();
        let data_btree = BTree::open(table_def.data_btree_root);

        let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
        data_btree.scan(pager, |k, v| {
            let row_values = deserialize_row(v, &old_columns)?;
            entries.push((k.to_vec(), row_values));
            Ok(true)
        })?;

        // Update column def (including name change)
        update_column_def(&mut table_def.columns[col_idx], col_spec);

        let old_pages = data_btree.collect_all_pages(pager)?;
        for page_id in old_pages {
            pager.free_page(page_id);
        }
        let new_data_btree = BTree::create(pager)?;
        let mut new_btree = BTree::open(new_data_btree.root_page_id());

        for (key, mut row_values) in entries {
            row_values[col_idx] = coerce_value(&row_values[col_idx], col_spec.data_type)?;
            let new_data = serialize_row(&row_values, &table_def.columns);
            new_btree.insert(pager, &key, &new_data)?;
        }

        table_def.data_btree_root = new_btree.root_page_id();
    } else {
        update_column_def(&mut table_def.columns[col_idx], col_spec);
    }

    catalog.update_table(pager, &table_def)?;
    Ok(ExecResult::Ok)
}

/// Update a ColumnDef in place from a ColumnSpec.
fn update_column_def(col: &mut ColumnDef, spec: &ColumnSpec) {
    col.name = spec.name.clone();
    col.data_type = spec.data_type;
    col.is_unique = spec.is_unique;
    col.is_nullable = spec.is_nullable;
    col.auto_increment = spec.auto_increment;
    if let Some(default_expr) = &spec.default_value {
        col.default_value = ast_expr_to_default(default_expr);
    } else {
        col.default_value = None;
    }
    if let Some(check) = &spec.check_expr {
        col.check_expr = Some(expr_to_string(check));
    } else {
        col.check_expr = None;
    }
}

/// Coerce a value to a target data type.
fn coerce_value(value: &Value, target_type: DataType) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => match target_type {
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                Ok(Value::Integer(*n))
            }
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(n.to_string())),
            DataType::Varbinary(_) => Err(MuroError::Execution(
                "Cannot coerce integer to VARBINARY".into(),
            )),
        },
        Value::Varchar(s) => match target_type {
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                let n: i64 = s.parse().map_err(|_| {
                    MuroError::Execution(format!("Cannot convert '{}' to integer", s))
                })?;
                Ok(Value::Integer(n))
            }
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(s.clone())),
            DataType::Varbinary(_) => Ok(Value::Varbinary(s.as_bytes().to_vec())),
        },
        Value::Varbinary(b) => match target_type {
            DataType::Varchar(_) | DataType::Text => {
                let s = String::from_utf8(b.clone()).map_err(|_| {
                    MuroError::Execution("Cannot convert VARBINARY to VARCHAR".into())
                })?;
                Ok(Value::Varchar(s))
            }
            DataType::Varbinary(_) => Ok(Value::Varbinary(b.clone())),
            _ => Err(MuroError::Execution(
                "Cannot coerce VARBINARY to integer type".into(),
            )),
        },
    }
}

fn exec_rename_table(
    rt: &RenameTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    catalog.rename_table(pager, &rt.old_name, &rt.new_name)?;
    Ok(ExecResult::Ok)
}

fn exec_insert(
    ins: &Insert,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let mut table_def = catalog
        .get_table(pager, &ins.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", ins.table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &ins.table_name)?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut rows_inserted = 0u64;

    for value_row in &ins.values {
        let mut values = resolve_insert_values(&table_def, &ins.columns, value_row)?;

        // Apply DEFAULT values for NULL columns that have defaults
        for (i, col) in table_def.columns.iter().enumerate() {
            if values[i].is_null() && !col.is_hidden {
                if let Some(default) = &col.default_value {
                    values[i] = match default {
                        DefaultValue::Integer(n) => Value::Integer(*n),
                        DefaultValue::String(s) => Value::Varchar(s.clone()),
                        DefaultValue::Null => Value::Null,
                    };
                }
            }
        }

        // Auto-generate for AUTO_INCREMENT columns
        let pk_idx = table_def
            .pk_column_index()
            .ok_or_else(|| MuroError::Execution("Table has no primary key".into()))?;

        if table_def.columns[pk_idx].auto_increment && values[pk_idx].is_null() {
            table_def.next_rowid += 1;
            values[pk_idx] = Value::Integer(table_def.next_rowid);
        } else if table_def.columns[pk_idx].is_hidden && values[pk_idx].is_null() {
            // Auto-generate _rowid for hidden PK columns
            table_def.next_rowid += 1;
            values[pk_idx] = Value::Integer(table_def.next_rowid);
        }

        // Validate NOT NULL constraints
        for (i, col) in table_def.columns.iter().enumerate() {
            if !col.is_nullable && values[i].is_null() {
                return Err(MuroError::Execution(format!(
                    "Column '{}' cannot be NULL",
                    col.name
                )));
            }
        }

        // Validate all values against their column types
        for (i, val) in values.iter().enumerate() {
            if !val.is_null() {
                validate_value(val, &table_def.columns[i].data_type)?;
            }
        }

        // Validate CHECK constraints
        for (i, col) in table_def.columns.iter().enumerate() {
            if let Some(check_sql) = &col.check_expr {
                if !values[i].is_null() {
                    let check_expr = crate::sql::parser::parse_sql(&format!(
                        "SELECT * FROM _dummy WHERE {}",
                        check_sql
                    ));
                    if let Ok(Statement::Select(sel)) = check_expr {
                        if let Some(where_expr) = &sel.where_clause {
                            let result = eval_expr(where_expr, &|name| {
                                table_def
                                    .column_index(name)
                                    .and_then(|idx| values.get(idx).cloned())
                            })?;
                            if !is_truthy(&result) {
                                return Err(MuroError::Execution(format!(
                                    "CHECK constraint failed for column '{}'",
                                    col.name
                                )));
                            }
                        }
                    }
                }
            }
        }

        let pk_value = &values[pk_idx];
        let pk_data_type = &table_def.columns[pk_idx].data_type;
        let pk_key = encode_value(pk_value, pk_data_type);

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
                    let idx_key = encode_value(val, &table_def.columns[col_idx].data_type);
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
                let col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let val = &values[col_idx];
                if !val.is_null() {
                    let idx_key = encode_value(val, &table_def.columns[col_idx].data_type);
                    let mut idx_btree = BTree::open(idx.btree_root);
                    idx_btree.insert(pager, &idx_key, &pk_key)?;
                }
            }
        }

        // Update table_def if btree root changed or next_rowid changed
        table_def.data_btree_root = data_btree.root_page_id();
        catalog.update_table(pager, &table_def)?;

        rows_inserted += 1;
    }

    Ok(ExecResult::RowsAffected(rows_inserted))
}

fn exec_select(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &sel.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", sel.table_name)))?;

    // If there are JOINs, use the join execution path
    if !sel.joins.is_empty() {
        return exec_select_join(sel, &table_def, pager, catalog);
    }

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

    let need_aggregation = has_aggregates(&sel.columns, &sel.having) || sel.group_by.is_some();

    if need_aggregation {
        // Aggregation path: collect raw values first
        let mut raw_rows: Vec<Vec<Value>> = Vec::new();

        match plan {
            Plan::PkSeek { key_expr, .. } => {
                let key_val = eval_expr(&key_expr, &|_| None)?;
                let pk_idx = table_def
                    .pk_column_index()
                    .ok_or_else(|| MuroError::Execution("Table has no primary key".into()))?;
                let pk_key = encode_value(&key_val, &table_def.columns[pk_idx].data_type);
                let data_btree = BTree::open(table_def.data_btree_root);
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row(&data, &table_def.columns)?;
                    if matches_where(&sel.where_clause, &table_def, &values)? {
                        raw_rows.push(values);
                    }
                }
            }
            Plan::IndexSeek {
                index_name,
                key_expr,
                ..
            } => {
                let key_val = eval_expr(&key_expr, &|_| None)?;
                let idx = indexes
                    .iter()
                    .find(|i| i.name == index_name)
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let idx_col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let idx_key = encode_value(&key_val, &table_def.columns[idx_col_idx].data_type);
                let idx_btree = BTree::open(idx.btree_root);
                if let Some(pk_key) = idx_btree.search(pager, &idx_key)? {
                    let data_btree = BTree::open(table_def.data_btree_root);
                    if let Some(data) = data_btree.search(pager, &pk_key)? {
                        let values = deserialize_row(&data, &table_def.columns)?;
                        if matches_where(&sel.where_clause, &table_def, &values)? {
                            raw_rows.push(values);
                        }
                    }
                }
            }
            Plan::FullScan { .. } | Plan::FtsScan { .. } => {
                let data_btree = BTree::open(table_def.data_btree_root);
                data_btree.scan(pager, |_k, v| {
                    let values = deserialize_row(v, &table_def.columns)?;
                    if matches_where(&sel.where_clause, &table_def, &values)? {
                        raw_rows.push(values);
                    }
                    Ok(true)
                })?;
            }
        }

        let mut rows = execute_aggregation(raw_rows, &table_def, sel)?;

        // ORDER BY
        if let Some(order_items) = &sel.order_by {
            sort_rows(&mut rows, order_items);
        }

        // OFFSET
        if let Some(offset) = sel.offset {
            let offset = offset as usize;
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }

        // LIMIT
        if let Some(limit) = sel.limit {
            rows.truncate(limit as usize);
        }

        Ok(ExecResult::Rows(rows))
    } else {
        // Non-aggregation path (original)
        let mut rows: Vec<Row> = Vec::new();

        match plan {
            Plan::PkSeek { key_expr, .. } => {
                let key_val = eval_expr(&key_expr, &|_| None)?;
                let pk_idx = table_def
                    .pk_column_index()
                    .ok_or_else(|| MuroError::Execution("Table has no primary key".into()))?;
                let pk_key = encode_value(&key_val, &table_def.columns[pk_idx].data_type);
                let data_btree = BTree::open(table_def.data_btree_root);
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row(&data, &table_def.columns)?;
                    let row = build_row(&table_def, &values, &sel.columns)?;
                    if matches_where(&sel.where_clause, &table_def, &values)? {
                        rows.push(row);
                    }
                }
            }
            Plan::IndexSeek {
                index_name,
                key_expr,
                ..
            } => {
                let key_val = eval_expr(&key_expr, &|_| None)?;
                let idx = indexes
                    .iter()
                    .find(|i| i.name == index_name)
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let idx_col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let idx_key = encode_value(&key_val, &table_def.columns[idx_col_idx].data_type);
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
            Plan::FullScan { .. } | Plan::FtsScan { .. } => {
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

        // SELECT DISTINCT
        if sel.distinct {
            let mut seen = HashSet::new();
            rows.retain(|row| {
                let key: Vec<ValueKey> = row
                    .values
                    .iter()
                    .map(|(_, v)| ValueKey(v.clone()))
                    .collect();
                seen.insert(key)
            });
        }

        // ORDER BY
        if let Some(order_items) = &sel.order_by {
            sort_rows(&mut rows, order_items);
        }

        // OFFSET
        if let Some(offset) = sel.offset {
            let offset = offset as usize;
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }

        // LIMIT
        if let Some(limit) = sel.limit {
            rows.truncate(limit as usize);
        }

        Ok(ExecResult::Rows(rows))
    }
}

/// Scan all rows of a table into qualified name format: Vec<Vec<(String, Value)>>
/// where each (String, Value) has name = "tablename.column"
fn scan_table_qualified(
    table_name: &str,
    alias: Option<&str>,
    table_def: &TableDef,
    pager: &mut impl PageStore,
) -> Result<Vec<Vec<(String, Value)>>> {
    let qualifier = alias.unwrap_or(table_name);
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut result = Vec::new();
    data_btree.scan(pager, |_k, v| {
        let values = deserialize_row(v, &table_def.columns)?;
        let mut row: Vec<(String, Value)> = Vec::with_capacity(table_def.columns.len());
        for (i, col) in table_def.columns.iter().enumerate() {
            let val = values.get(i).cloned().unwrap_or(Value::Null);
            row.push((format!("{}.{}", qualifier, col.name), val));
        }
        result.push(row);
        Ok(true)
    })?;
    Ok(result)
}

/// Make a null row for LEFT JOIN when there's no match on the right side.
fn null_row_qualified(qualifier: &str, table_def: &TableDef) -> Vec<(String, Value)> {
    table_def
        .columns
        .iter()
        .map(|col| (format!("{}.{}", qualifier, col.name), Value::Null))
        .collect()
}

/// Resolve a column name against a joined row.
/// Supports "table.column" qualified names and unqualified "column" names.
fn resolve_join_column<'a>(
    name: &str,
    row: &'a [(String, Value)],
) -> std::result::Result<Option<&'a Value>, String> {
    // If already qualified (contains a dot, but not ".*")
    if name.contains('.') && !name.ends_with(".*") {
        for (k, v) in row {
            if k == name {
                return Ok(Some(v));
            }
        }
        return Ok(None);
    }

    // Unqualified: search all columns, check for ambiguity
    let mut found: Option<&Value> = None;
    let mut found_count = 0;
    for (k, v) in row {
        let col_part = k.rsplit('.').next().unwrap_or(k);
        if col_part == name {
            found = Some(v);
            found_count += 1;
        }
    }
    if found_count > 1 {
        return Err(format!("Ambiguous column name: {}", name));
    }
    Ok(found)
}

/// Evaluate a WHERE/ON expression against a joined row (Vec of qualified (name, value) pairs).
fn eval_join_expr(expr: &Expr, row: &[(String, Value)]) -> Result<Value> {
    eval_expr(expr, &|name| {
        resolve_join_column(name, row).ok().flatten().cloned()
    })
}

fn exec_select_join(
    sel: &Select,
    base_table_def: &TableDef,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Collect hidden qualified column names for Star expansion filtering
    let base_qualifier = sel.table_alias.as_deref().unwrap_or(&sel.table_name);
    let mut hidden_columns: Vec<String> = base_table_def
        .columns
        .iter()
        .filter(|c| c.is_hidden)
        .map(|c| format!("{}.{}", base_qualifier, c.name))
        .collect();

    // 1. Scan the base (FROM) table
    let mut joined_rows = scan_table_qualified(
        &sel.table_name,
        sel.table_alias.as_deref(),
        base_table_def,
        pager,
    )?;

    // 2. For each JOIN, perform nested loop join
    for join in &sel.joins {
        let right_table_def = catalog
            .get_table(pager, &join.table_name)?
            .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", join.table_name)))?;

        let right_qualifier = join.alias.as_deref().unwrap_or(&join.table_name);
        hidden_columns.extend(
            right_table_def
                .columns
                .iter()
                .filter(|c| c.is_hidden)
                .map(|c| format!("{}.{}", right_qualifier, c.name)),
        );
        let right_rows = scan_table_qualified(
            &join.table_name,
            join.alias.as_deref(),
            &right_table_def,
            pager,
        )?;

        let mut new_rows: Vec<Vec<(String, Value)>> = Vec::new();

        match join.join_type {
            JoinType::Inner => {
                for left in &joined_rows {
                    for right in &right_rows {
                        let mut combined: Vec<(String, Value)> =
                            Vec::with_capacity(left.len() + right.len());
                        combined.extend(left.iter().cloned());
                        combined.extend(right.iter().cloned());

                        if let Some(on_expr) = &join.on_condition {
                            let val = eval_join_expr(on_expr, &combined)?;
                            if is_truthy(&val) {
                                new_rows.push(combined);
                            }
                        } else {
                            new_rows.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                for left in &joined_rows {
                    let mut matched = false;
                    for right in &right_rows {
                        let mut combined: Vec<(String, Value)> =
                            Vec::with_capacity(left.len() + right.len());
                        combined.extend(left.iter().cloned());
                        combined.extend(right.iter().cloned());

                        if let Some(on_expr) = &join.on_condition {
                            let val = eval_join_expr(on_expr, &combined)?;
                            if is_truthy(&val) {
                                new_rows.push(combined);
                                matched = true;
                            }
                        } else {
                            new_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined: Vec<(String, Value)> = left.clone();
                        combined.extend(null_row_qualified(right_qualifier, &right_table_def));
                        new_rows.push(combined);
                    }
                }
            }
            JoinType::Cross => {
                for left in &joined_rows {
                    for right in &right_rows {
                        let mut combined: Vec<(String, Value)> =
                            Vec::with_capacity(left.len() + right.len());
                        combined.extend(left.iter().cloned());
                        combined.extend(right.iter().cloned());
                        new_rows.push(combined);
                    }
                }
            }
        }

        joined_rows = new_rows;
    }

    // 3. Apply WHERE filter
    if let Some(where_expr) = &sel.where_clause {
        let mut filter_error: Option<MuroError> = None;
        joined_rows.retain(|row| {
            if filter_error.is_some() {
                return false;
            }
            match eval_join_expr(where_expr, row) {
                Ok(val) => is_truthy(&val),
                Err(e) => {
                    filter_error = Some(e);
                    false
                }
            }
        });
        if let Some(e) = filter_error {
            return Err(e);
        }
    }

    let need_aggregation = has_aggregates(&sel.columns, &sel.having) || sel.group_by.is_some();

    if need_aggregation {
        // Aggregation path for joins
        let mut rows = execute_aggregation_join(&joined_rows, sel, &hidden_columns)?;

        // ORDER BY
        if let Some(order_items) = &sel.order_by {
            sort_rows(&mut rows, order_items);
        }

        // OFFSET
        if let Some(offset) = sel.offset {
            let offset = offset as usize;
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }

        // LIMIT
        if let Some(limit) = sel.limit {
            rows.truncate(limit as usize);
        }

        Ok(ExecResult::Rows(rows))
    } else {
        // 4. ORDER BY (before projection, so all columns are accessible)
        if let Some(order_items) = &sel.order_by {
            joined_rows.sort_by(|a, b| {
                for item in order_items {
                    if let Expr::ColumnRef(col) = &item.expr {
                        let va = resolve_join_column(col, a).ok().flatten();
                        let vb = resolve_join_column(col, b).ok().flatten();
                        let ord = cmp_values(va, vb);
                        if ord != std::cmp::Ordering::Equal {
                            return if item.descending { ord.reverse() } else { ord };
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // 5. OFFSET
        if let Some(offset) = sel.offset {
            let offset = offset as usize;
            if offset >= joined_rows.len() {
                joined_rows.clear();
            } else {
                joined_rows = joined_rows.into_iter().skip(offset).collect();
            }
        }

        // 6. LIMIT
        if let Some(limit) = sel.limit {
            joined_rows.truncate(limit as usize);
        }

        // 7. Project SELECT columns
        let mut rows: Vec<Row> = Vec::new();
        for jrow in &joined_rows {
            let row = build_join_row(jrow, &sel.columns, &hidden_columns)?;
            rows.push(row);
        }

        // SELECT DISTINCT
        if sel.distinct {
            let mut seen = HashSet::new();
            rows.retain(|row| {
                let key: Vec<ValueKey> = row
                    .values
                    .iter()
                    .map(|(_, v)| ValueKey(v.clone()))
                    .collect();
                seen.insert(key)
            });
        }

        Ok(ExecResult::Rows(rows))
    }
}

fn build_join_row(
    jrow: &[(String, Value)],
    select_columns: &[SelectColumn],
    hidden_columns: &[String],
) -> Result<Row> {
    let mut row_values = Vec::new();

    for sel_col in select_columns {
        match sel_col {
            SelectColumn::Star => {
                // Output all columns, using just the column part as the name, skip hidden
                for (qualified_name, val) in jrow {
                    if hidden_columns.contains(qualified_name) {
                        continue;
                    }
                    let col_name = qualified_name
                        .rsplit('.')
                        .next()
                        .unwrap_or(qualified_name)
                        .to_string();
                    row_values.push((col_name, val.clone()));
                }
            }
            SelectColumn::Expr(expr, alias) => {
                // Check for table.* pattern
                if let Expr::ColumnRef(ref_name) = expr {
                    if ref_name.ends_with(".*") {
                        let prefix = &ref_name[..ref_name.len() - 2]; // "table"
                        for (qualified_name, val) in jrow {
                            if qualified_name.starts_with(prefix)
                                && qualified_name.as_bytes().get(prefix.len()) == Some(&b'.')
                            {
                                let col_name = qualified_name
                                    .rsplit('.')
                                    .next()
                                    .unwrap_or(qualified_name)
                                    .to_string();
                                row_values.push((col_name, val.clone()));
                            }
                        }
                        continue;
                    }
                }

                let val = eval_join_expr(expr, jrow)?;
                let name = alias.clone().unwrap_or_else(|| match expr {
                    Expr::ColumnRef(n) => n.clone(),
                    _ => "?column?".to_string(),
                });
                row_values.push((name, val));
            }
        }
    }

    Ok(Row { values: row_values })
}

fn sort_rows(rows: &mut [Row], order_items: &[OrderByItem]) {
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

fn exec_update(
    upd: &Update,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &upd.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", upd.table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &upd.table_name)?;

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

    for (pk_key, old_values) in to_update {
        let mut new_values = old_values.clone();

        // Apply assignments
        for (col_name, expr) in &upd.assignments {
            let col_idx = table_def
                .column_index(col_name)
                .ok_or_else(|| MuroError::Execution(format!("Unknown column: {}", col_name)))?;
            let new_val = eval_expr(expr, &|name| {
                table_def
                    .column_index(name)
                    .and_then(|i| new_values.get(i).cloned())
            })?;
            new_values[col_idx] = new_val;
        }

        // Check unique constraints on updated indexed columns
        for idx in &indexes {
            if idx.is_unique && idx.index_type == IndexType::BTree {
                let col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let old_val = &old_values[col_idx];
                let new_val = &new_values[col_idx];
                if old_val != new_val && !new_val.is_null() {
                    let idx_key = encode_value(new_val, &table_def.columns[col_idx].data_type);
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

        // Update secondary indexes: remove old entries, insert new entries
        for idx in &indexes {
            if idx.index_type == IndexType::BTree {
                let col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let old_val = &old_values[col_idx];
                let new_val = &new_values[col_idx];
                if old_val != new_val {
                    // Remove old index entry
                    if !old_val.is_null() {
                        let old_idx_key =
                            encode_value(old_val, &table_def.columns[col_idx].data_type);
                        let mut idx_btree = BTree::open(idx.btree_root);
                        idx_btree.delete(pager, &old_idx_key)?;
                    }
                    // Insert new index entry
                    if !new_val.is_null() {
                        let new_idx_key =
                            encode_value(new_val, &table_def.columns[col_idx].data_type);
                        let mut idx_btree = BTree::open(idx.btree_root);
                        idx_btree.insert(pager, &new_idx_key, &pk_key)?;
                    }
                }
            }
        }

        let row_data = serialize_row(&new_values, &table_def.columns);
        data_btree.insert(pager, &pk_key, &row_data)?;
        count += 1;
    }

    Ok(ExecResult::RowsAffected(count))
}

fn exec_delete(
    del: &Delete,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &del.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", del.table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &del.table_name)?;

    let data_btree = BTree::open(table_def.data_btree_root);

    // Collect keys and row values to delete
    let mut to_delete: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    data_btree.scan(pager, |k, v| {
        let values = deserialize_row(v, &table_def.columns)?;
        if matches_where(&del.where_clause, &table_def, &values)? {
            to_delete.push((k.to_vec(), values));
        }
        Ok(true)
    })?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut count = 0u64;

    for (pk_key, values) in &to_delete {
        // Delete from secondary indexes
        for idx in &indexes {
            if idx.index_type == IndexType::BTree {
                let col_idx = table_def.column_index(&idx.column_name).ok_or_else(|| {
                    MuroError::Schema(format!(
                        "Index column '{}' not found in table",
                        idx.column_name
                    ))
                })?;
                let val = &values[col_idx];
                if !val.is_null() {
                    let idx_key = encode_value(val, &table_def.columns[col_idx].data_type);
                    let mut idx_btree = BTree::open(idx.btree_root);
                    idx_btree.delete(pager, &idx_key)?;
                }
            }
        }

        data_btree.delete(pager, pk_key)?;
        count += 1;
    }

    Ok(ExecResult::RowsAffected(count))
}

fn exec_show_tables(pager: &mut impl PageStore, catalog: &mut SystemCatalog) -> Result<ExecResult> {
    let tables = catalog.list_tables(pager)?;
    let rows = tables
        .into_iter()
        .map(|name| Row {
            values: vec![("Table".to_string(), Value::Varchar(name))],
        })
        .collect();
    Ok(ExecResult::Rows(rows))
}

fn exec_show_create_table(
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let mut sql = format!("CREATE TABLE {} (\n", table_name);
    let visible_columns: Vec<&ColumnDef> =
        table_def.columns.iter().filter(|c| !c.is_hidden).collect();

    for (i, col) in visible_columns.iter().enumerate() {
        sql.push_str(&format!("  {} {}", col.name, col.data_type));
        if col.is_primary_key {
            sql.push_str(" PRIMARY KEY");
        }
        if col.auto_increment {
            sql.push_str(" AUTO_INCREMENT");
        }
        if col.is_unique && !col.is_primary_key {
            sql.push_str(" UNIQUE");
        }
        if !col.is_nullable && !col.is_primary_key {
            sql.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default_value {
            match default {
                DefaultValue::Integer(n) => sql.push_str(&format!(" DEFAULT {}", n)),
                DefaultValue::String(s) => sql.push_str(&format!(" DEFAULT '{}'", s)),
                DefaultValue::Null => sql.push_str(" DEFAULT NULL"),
            }
        }
        if let Some(check) = &col.check_expr {
            sql.push_str(&format!(" CHECK ({})", check));
        }
        if i < visible_columns.len() - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }
    sql.push(')');

    let rows = vec![Row {
        values: vec![
            ("Table".to_string(), Value::Varchar(table_name.to_string())),
            ("Create Table".to_string(), Value::Varchar(sql)),
        ],
    }];
    Ok(ExecResult::Rows(rows))
}

fn exec_describe(
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let mut rows = Vec::new();
    for col in &table_def.columns {
        if col.is_hidden {
            continue;
        }
        let null_str = if col.is_nullable { "YES" } else { "NO" };
        let key_str = if col.is_primary_key {
            "PRI"
        } else if col.is_unique {
            "UNI"
        } else {
            ""
        };
        let default_str = match &col.default_value {
            Some(DefaultValue::Integer(n)) => n.to_string(),
            Some(DefaultValue::String(s)) => s.clone(),
            Some(DefaultValue::Null) => "NULL".to_string(),
            None => "NULL".to_string(),
        };
        let extra_str = if col.auto_increment {
            "auto_increment"
        } else {
            ""
        };

        rows.push(Row {
            values: vec![
                ("Field".to_string(), Value::Varchar(col.name.clone())),
                (
                    "Type".to_string(),
                    Value::Varchar(col.data_type.to_string()),
                ),
                ("Null".to_string(), Value::Varchar(null_str.to_string())),
                ("Key".to_string(), Value::Varchar(key_str.to_string())),
                ("Default".to_string(), Value::Varchar(default_str)),
                ("Extra".to_string(), Value::Varchar(extra_str.to_string())),
            ],
        });
    }
    Ok(ExecResult::Rows(rows))
}

// --- Row serialization ---
// Format: [null_bitmap][value1][value2]...
// Each value: for integers: 1/2/4/8 bytes by type; for VARCHAR/TEXT/VARBINARY: u32 len + bytes

pub fn serialize_row(values: &[Value], columns: &[ColumnDef]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Stored column count (u16) — allows deserialize_row to handle short rows
    // after ALTER TABLE ADD COLUMN without rewriting existing data.
    buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());

    // Null bitmap (1 bit per column, packed into bytes)
    let bitmap_bytes = columns.len().div_ceil(8);
    let mut bitmap = vec![0u8; bitmap_bytes];
    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend_from_slice(&bitmap);

    // Values
    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            continue;
        }
        match val {
            Value::Integer(n) => match columns[i].data_type {
                DataType::TinyInt => buf.extend_from_slice(&(*n as i8).to_le_bytes()),
                DataType::SmallInt => buf.extend_from_slice(&(*n as i16).to_le_bytes()),
                DataType::Int => buf.extend_from_slice(&(*n as i32).to_le_bytes()),
                DataType::BigInt => buf.extend_from_slice(&n.to_le_bytes()),
                _ => buf.extend_from_slice(&n.to_le_bytes()),
            },
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
    // Read stored column count from the u16 prefix
    if data.len() < 2 {
        return Err(MuroError::InvalidPage);
    }
    let stored_col_count = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
    let data = &data[2..]; // skip the u16 prefix

    let bitmap_bytes = stored_col_count.div_ceil(8);
    if data.len() < bitmap_bytes {
        return Err(MuroError::InvalidPage);
    }

    let bitmap = &data[..bitmap_bytes];
    let mut offset = bitmap_bytes;
    let mut values = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        // Columns beyond what was stored get default/NULL
        if i >= stored_col_count {
            values.push(default_value_for_column(col));
            continue;
        }

        let is_null = bitmap[i / 8] & (1 << (i % 8)) != 0;
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match col.data_type {
            DataType::TinyInt => {
                if offset + 1 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = data[offset] as i8;
                values.push(Value::Integer(n as i64));
                offset += 1;
            }
            DataType::SmallInt => {
                if offset + 2 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                values.push(Value::Integer(n as i64));
                offset += 2;
            }
            DataType::Int => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                values.push(Value::Integer(n as i64));
                offset += 4;
            }
            DataType::BigInt => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::Integer(n));
                offset += 8;
            }
            DataType::Varchar(_) | DataType::Text => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let s = String::from_utf8(data[offset..offset + len].to_vec())
                    .map_err(|_| MuroError::InvalidPage)?;
                values.push(Value::Varchar(s));
                offset += len;
            }
            DataType::Varbinary(_) => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
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

/// Get the default value for a newly-added column.
fn default_value_for_column(col: &ColumnDef) -> Value {
    match &col.default_value {
        Some(DefaultValue::Integer(n)) => Value::Integer(*n),
        Some(DefaultValue::String(s)) => Value::Varchar(s.clone()),
        Some(DefaultValue::Null) | None => Value::Null,
    }
}

/// Encode a Value for use as a B-tree key.
/// For integer types, the encoding width depends on the DataType.
pub fn encode_value(value: &Value, data_type: &DataType) -> Vec<u8> {
    match (value, data_type) {
        (Value::Integer(n), DataType::TinyInt) => encode_i8(*n as i8).to_vec(),
        (Value::Integer(n), DataType::SmallInt) => encode_i16(*n as i16).to_vec(),
        (Value::Integer(n), DataType::Int) => encode_i32(*n as i32).to_vec(),
        (Value::Integer(n), DataType::BigInt) => encode_i64(*n).to_vec(),
        (Value::Integer(n), _) => encode_i64(*n).to_vec(),
        (Value::Varchar(s), _) => s.as_bytes().to_vec(),
        (Value::Varbinary(b), _) => b.clone(),
        (Value::Null, _) => Vec::new(),
    }
}

/// Validate that a value fits within the constraints of the data type.
fn validate_value(value: &Value, data_type: &DataType) -> Result<()> {
    match (value, data_type) {
        (Value::Integer(n), DataType::TinyInt) if *n < -128 || *n > 127 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for TINYINT (-128 to 127)",
                n
            )))
        }
        (Value::Integer(n), DataType::SmallInt) if *n < -32768 || *n > 32767 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for SMALLINT (-32768 to 32767)",
                n
            )))
        }
        (Value::Integer(n), DataType::Int) if *n < i32::MIN as i64 || *n > i32::MAX as i64 => {
            Err(MuroError::Execution(format!(
                "Value {} out of range for INT ({} to {})",
                n,
                i32::MIN,
                i32::MAX
            )))
        }
        (Value::Varchar(s), DataType::Varchar(Some(max))) if s.len() as u32 > *max => {
            Err(MuroError::Execution(format!(
                "String length {} exceeds VARCHAR({})",
                s.len(),
                max
            )))
        }
        (Value::Varbinary(b), DataType::Varbinary(Some(max))) if b.len() as u32 > *max => {
            Err(MuroError::Execution(format!(
                "Binary length {} exceeds VARBINARY({})",
                b.len(),
                max
            )))
        }
        _ => Ok(()),
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
                let idx = table_def
                    .column_index(col_name)
                    .ok_or_else(|| MuroError::Execution(format!("Unknown column: {}", col_name)))?;
                let val = eval_expr(expr, &|_| None)?;
                // Handle DEFAULT keyword
                if matches!(expr, Expr::DefaultValue) {
                    // Leave as Null - will be filled by default value logic
                    continue;
                }
                values[idx] = val;
            }
        }
        None => {
            // When no columns are specified, hidden columns are excluded from the count
            let visible_indices: Vec<usize> = table_def
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| !c.is_hidden && !c.auto_increment)
                .map(|(i, _)| i)
                .collect();
            if exprs.len() != visible_indices.len() {
                // Also try with auto_increment columns included
                let all_visible: Vec<usize> = table_def
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| !c.is_hidden)
                    .map(|(i, _)| i)
                    .collect();
                if exprs.len() == all_visible.len() {
                    for (expr_idx, &col_idx) in all_visible.iter().enumerate() {
                        if matches!(exprs[expr_idx], Expr::DefaultValue) {
                            continue;
                        }
                        values[col_idx] = eval_expr(&exprs[expr_idx], &|_| None)?;
                    }
                    return Ok(values);
                }
                return Err(MuroError::Execution(
                    "Value count doesn't match column count".into(),
                ));
            }
            for (expr_idx, &col_idx) in visible_indices.iter().enumerate() {
                if matches!(exprs[expr_idx], Expr::DefaultValue) {
                    continue;
                }
                values[col_idx] = eval_expr(&exprs[expr_idx], &|_| None)?;
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
                    if col.is_hidden {
                        continue;
                    }
                    let val = values.get(i).cloned().unwrap_or(Value::Null);
                    row_values.push((col.name.clone(), val));
                }
            }
            SelectColumn::Expr(expr, alias) => {
                let val = eval_expr(expr, &|name| {
                    table_def
                        .column_index(name)
                        .and_then(|i| values.get(i).cloned())
                })?;
                let name = alias.clone().unwrap_or_else(|| match expr {
                    Expr::ColumnRef(n) => n.clone(),
                    _ => "?column?".to_string(),
                });
                row_values.push((name, val));
            }
        }
    }

    Ok(Row { values: row_values })
}

// --- Aggregation infrastructure ---

/// Check if any SelectColumn or HAVING clause contains an aggregate function.
fn has_aggregates(columns: &[SelectColumn], having: &Option<Expr>) -> bool {
    for col in columns {
        if let SelectColumn::Expr(expr, _) = col {
            if expr_contains_aggregate(expr) {
                return true;
            }
        }
    }
    if let Some(h) = having {
        if expr_contains_aggregate(h) {
            return true;
        }
    }
    false
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::AggregateFunc { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { operand, .. } => expr_contains_aggregate(operand),
        Expr::Like { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                if expr_contains_aggregate(op) {
                    return true;
                }
            }
            for (cond, then) in when_clauses {
                if expr_contains_aggregate(cond) || expr_contains_aggregate(then) {
                    return true;
                }
            }
            if let Some(e) = else_clause {
                if expr_contains_aggregate(e) {
                    return true;
                }
            }
            false
        }
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::GreaterThanZero(inner) => expr_contains_aggregate(inner),
        _ => false,
    }
}

/// Accumulator for aggregate functions.
enum Accumulator {
    Count { count: i64 },
    CountDistinct { values: HashSet<ValueKey> },
    Sum { total: Option<i64> },
    Min { val: Option<Value> },
    Max { val: Option<Value> },
    Avg { sum: i64, count: i64 },
}

impl Accumulator {
    fn new(name: &str, distinct: bool) -> Self {
        match name {
            "COUNT" if distinct => Accumulator::CountDistinct {
                values: HashSet::new(),
            },
            "COUNT" => Accumulator::Count { count: 0 },
            "SUM" => Accumulator::Sum { total: None },
            "MIN" => Accumulator::Min { val: None },
            "MAX" => Accumulator::Max { val: None },
            "AVG" => Accumulator::Avg { sum: 0, count: 0 },
            _ => Accumulator::Count { count: 0 },
        }
    }

    fn feed(&mut self, val: &Value) {
        match self {
            Accumulator::Count { count } => {
                // COUNT(col) skips NULLs; COUNT(*) uses arg=None so this won't be called for NULLs
                if !val.is_null() {
                    *count += 1;
                }
            }
            Accumulator::CountDistinct { values } => {
                if !val.is_null() {
                    values.insert(ValueKey(val.clone()));
                }
            }
            Accumulator::Sum { total } => {
                if let Value::Integer(n) = val {
                    *total = Some(total.unwrap_or(0) + n);
                }
                // Skip NULLs and non-integer values
            }
            Accumulator::Min { val: current } => {
                if val.is_null() {
                    return;
                }
                match current {
                    None => *current = Some(val.clone()),
                    Some(cur) => {
                        if cmp_values(Some(val), Some(cur)) == std::cmp::Ordering::Less {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            Accumulator::Max { val: current } => {
                if val.is_null() {
                    return;
                }
                match current {
                    None => *current = Some(val.clone()),
                    Some(cur) => {
                        if cmp_values(Some(val), Some(cur)) == std::cmp::Ordering::Greater {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Value::Integer(n) = val {
                    *sum += n;
                    *count += 1;
                }
            }
        }
    }

    fn feed_count_star(&mut self) {
        if let Accumulator::Count { count } = self {
            *count += 1;
        }
    }

    fn finalize(&self) -> Value {
        match self {
            Accumulator::Count { count } => Value::Integer(*count),
            Accumulator::CountDistinct { values } => Value::Integer(values.len() as i64),
            Accumulator::Sum { total } => match total {
                Some(n) => Value::Integer(*n),
                None => Value::Null,
            },
            Accumulator::Min { val } => val.clone().unwrap_or(Value::Null),
            Accumulator::Max { val } => val.clone().unwrap_or(Value::Null),
            Accumulator::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Integer(*sum / *count)
                }
            }
        }
    }
}

/// Collect all AggregateFunc expressions from a list of SelectColumns and an optional HAVING clause.
/// Returns a list of (index, name, arg, distinct) for each aggregate found.
struct AggregateInfo {
    name: String,
    arg: Option<Expr>,
    distinct: bool,
}

fn collect_aggregates(columns: &[SelectColumn], having: &Option<Expr>) -> Vec<AggregateInfo> {
    let mut aggs = Vec::new();
    for col in columns {
        if let SelectColumn::Expr(expr, _) = col {
            collect_aggregates_from_expr(expr, &mut aggs);
        }
    }
    if let Some(h) = having {
        collect_aggregates_from_expr(h, &mut aggs);
    }
    aggs
}

fn collect_aggregates_from_expr(expr: &Expr, aggs: &mut Vec<AggregateInfo>) {
    match expr {
        Expr::AggregateFunc {
            name,
            arg,
            distinct,
        } => {
            // Check if we already have an identical aggregate
            let already_exists = aggs.iter().any(|a| {
                a.name == *name
                    && a.distinct == *distinct
                    && format!("{:?}", a.arg) == format!("{:?}", arg.as_deref().cloned())
            });
            if !already_exists {
                aggs.push(AggregateInfo {
                    name: name.clone(),
                    arg: arg.as_deref().cloned(),
                    distinct: *distinct,
                });
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aggregates_from_expr(left, aggs);
            collect_aggregates_from_expr(right, aggs);
        }
        Expr::UnaryOp { operand, .. } => {
            collect_aggregates_from_expr(operand, aggs);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_aggregates_from_expr(arg, aggs);
            }
        }
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_aggregates_from_expr(op, aggs);
            }
            for (cond, then) in when_clauses {
                collect_aggregates_from_expr(cond, aggs);
                collect_aggregates_from_expr(then, aggs);
            }
            if let Some(e) = else_clause {
                collect_aggregates_from_expr(e, aggs);
            }
        }
        Expr::Cast { expr, .. } => collect_aggregates_from_expr(expr, aggs),
        Expr::Like { expr, pattern, .. } => {
            collect_aggregates_from_expr(expr, aggs);
            collect_aggregates_from_expr(pattern, aggs);
        }
        Expr::IsNull { expr, .. } => collect_aggregates_from_expr(expr, aggs),
        Expr::InList { expr, list, .. } => {
            collect_aggregates_from_expr(expr, aggs);
            for item in list {
                collect_aggregates_from_expr(item, aggs);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_aggregates_from_expr(expr, aggs);
            collect_aggregates_from_expr(low, aggs);
            collect_aggregates_from_expr(high, aggs);
        }
        Expr::GreaterThanZero(inner) => collect_aggregates_from_expr(inner, aggs),
        _ => {}
    }
}

/// Find the index of an aggregate in the aggs list that matches a given AggregateFunc expression.
fn find_aggregate_index(
    aggs: &[AggregateInfo],
    name: &str,
    arg: &Option<Box<Expr>>,
    distinct: bool,
) -> Option<usize> {
    aggs.iter().position(|a| {
        a.name == name
            && a.distinct == distinct
            && format!("{:?}", a.arg) == format!("{:?}", arg.as_deref().cloned())
    })
}

/// Substitute aggregate expressions in an Expr with their computed values.
/// Returns a new Expr with aggregates replaced by their finalized values.
fn substitute_aggregates(expr: &Expr, aggs: &[AggregateInfo], agg_values: &[Value]) -> Expr {
    match expr {
        Expr::AggregateFunc {
            name,
            arg,
            distinct,
        } => {
            if let Some(idx) = find_aggregate_index(aggs, name, arg, *distinct) {
                match &agg_values[idx] {
                    Value::Integer(n) => Expr::IntLiteral(*n),
                    Value::Varchar(s) => Expr::StringLiteral(s.clone()),
                    Value::Null => Expr::Null,
                    Value::Varbinary(b) => Expr::BlobLiteral(b.clone()),
                }
            } else {
                Expr::Null
            }
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_aggregates(left, aggs, agg_values)),
            op: *op,
            right: Box::new(substitute_aggregates(right, aggs, agg_values)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op: *op,
            operand: Box::new(substitute_aggregates(operand, aggs, agg_values)),
        },
        _ => expr.clone(),
    }
}

/// Execute the aggregation pipeline for non-join queries.
/// Takes raw rows (Vec<Vec<Value>>), groups them, computes aggregates,
/// applies HAVING, and returns projected Rows.
fn execute_aggregation(
    raw_rows: Vec<Vec<Value>>,
    table_def: &TableDef,
    sel: &Select,
) -> Result<Vec<Row>> {
    let aggs = collect_aggregates(&sel.columns, &sel.having);
    let has_group_by = sel.group_by.is_some();

    // Build groups: group_key -> list of raw rows
    let mut groups: Vec<(Vec<ValueKey>, Vec<Vec<Value>>)> = Vec::new();
    let mut group_index: HashMap<Vec<ValueKey>, usize> = HashMap::new();

    for raw_row in &raw_rows {
        let group_key = if let Some(group_exprs) = &sel.group_by {
            let mut key = Vec::with_capacity(group_exprs.len());
            for gexpr in group_exprs {
                let val = eval_expr(gexpr, &|name| {
                    table_def
                        .column_index(name)
                        .and_then(|i| raw_row.get(i).cloned())
                })?;
                key.push(ValueKey(val));
            }
            key
        } else {
            // No GROUP BY: all rows in one group
            vec![]
        };

        if let Some(&idx) = group_index.get(&group_key) {
            groups[idx].1.push(raw_row.clone());
        } else {
            let idx = groups.len();
            group_index.insert(group_key.clone(), idx);
            groups.push((group_key, vec![raw_row.clone()]));
        }
    }

    // If no rows and no GROUP BY, produce a single group (for SELECT COUNT(*) FROM empty_table)
    if groups.is_empty() && !has_group_by {
        groups.push((vec![], vec![]));
    }

    let mut result_rows = Vec::new();

    for (_group_key, group_rows) in &groups {
        // Create accumulators for each aggregate
        let mut accumulators: Vec<Accumulator> = aggs
            .iter()
            .map(|a| Accumulator::new(&a.name, a.distinct))
            .collect();

        // Feed rows into accumulators
        for raw_row in group_rows {
            for (i, agg_info) in aggs.iter().enumerate() {
                if let Some(arg_expr) = &agg_info.arg {
                    let val = eval_expr(arg_expr, &|name| {
                        table_def
                            .column_index(name)
                            .and_then(|j| raw_row.get(j).cloned())
                    })?;
                    accumulators[i].feed(&val);
                } else {
                    // COUNT(*)
                    accumulators[i].feed_count_star();
                }
            }
        }

        // Finalize aggregates
        let agg_values: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();

        // Apply HAVING filter
        if let Some(having_expr) = &sel.having {
            let substituted = substitute_aggregates(having_expr, &aggs, &agg_values);
            // Use a representative row from the group for column references
            let rep_row = group_rows.first();
            let result = eval_expr(&substituted, &|name| {
                if let Some(row) = rep_row {
                    table_def
                        .column_index(name)
                        .and_then(|i| row.get(i).cloned())
                } else {
                    None
                }
            })?;
            if !is_truthy(&result) {
                continue;
            }
        }

        // Project SELECT columns
        let rep_row = group_rows.first();
        let mut row_values = Vec::new();

        for sel_col in &sel.columns {
            match sel_col {
                SelectColumn::Star => {
                    if let Some(raw) = rep_row {
                        for (i, col) in table_def.columns.iter().enumerate() {
                            if col.is_hidden {
                                continue;
                            }
                            let val = raw.get(i).cloned().unwrap_or(Value::Null);
                            row_values.push((col.name.clone(), val));
                        }
                    }
                }
                SelectColumn::Expr(expr, alias) => {
                    let substituted = substitute_aggregates(expr, &aggs, &agg_values);
                    let val = eval_expr(&substituted, &|name| {
                        if let Some(row) = rep_row {
                            table_def
                                .column_index(name)
                                .and_then(|i| row.get(i).cloned())
                        } else {
                            None
                        }
                    })?;
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::ColumnRef(n) => n.clone(),
                        Expr::AggregateFunc {
                            name,
                            arg,
                            distinct,
                        } => {
                            let arg_str = match arg {
                                None => "*".to_string(),
                                Some(a) => {
                                    if *distinct {
                                        format!("DISTINCT {:?}", a)
                                    } else {
                                        format!("{:?}", a)
                                    }
                                }
                            };
                            format!("{}({})", name, arg_str)
                        }
                        _ => "?column?".to_string(),
                    });
                    row_values.push((name, val));
                }
            }
        }

        result_rows.push(Row { values: row_values });
    }

    Ok(result_rows)
}

/// Execute the aggregation pipeline for join queries.
fn execute_aggregation_join(
    joined_rows: &[Vec<(String, Value)>],
    sel: &Select,
    hidden_columns: &[String],
) -> Result<Vec<Row>> {
    let aggs = collect_aggregates(&sel.columns, &sel.having);
    let has_group_by = sel.group_by.is_some();

    // Build groups
    #[allow(clippy::type_complexity)]
    let mut groups: Vec<(Vec<ValueKey>, Vec<&Vec<(String, Value)>>)> = Vec::new();
    let mut group_index: HashMap<Vec<ValueKey>, usize> = HashMap::new();

    for jrow in joined_rows {
        let group_key = if let Some(group_exprs) = &sel.group_by {
            let mut key = Vec::with_capacity(group_exprs.len());
            for gexpr in group_exprs {
                let val = eval_join_expr(gexpr, jrow)?;
                key.push(ValueKey(val));
            }
            key
        } else {
            vec![]
        };

        if let Some(&idx) = group_index.get(&group_key) {
            groups[idx].1.push(jrow);
        } else {
            let idx = groups.len();
            group_index.insert(group_key.clone(), idx);
            groups.push((group_key, vec![jrow]));
        }
    }

    if groups.is_empty() && !has_group_by {
        groups.push((vec![], vec![]));
    }

    let mut result_rows = Vec::new();

    for (_group_key, group_rows) in &groups {
        let mut accumulators: Vec<Accumulator> = aggs
            .iter()
            .map(|a| Accumulator::new(&a.name, a.distinct))
            .collect();

        for jrow in group_rows {
            for (i, agg_info) in aggs.iter().enumerate() {
                if let Some(arg_expr) = &agg_info.arg {
                    let val = eval_join_expr(arg_expr, jrow)?;
                    accumulators[i].feed(&val);
                } else {
                    accumulators[i].feed_count_star();
                }
            }
        }

        let agg_values: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();

        if let Some(having_expr) = &sel.having {
            let substituted = substitute_aggregates(having_expr, &aggs, &agg_values);
            let rep_row = group_rows.first().map(|r| r.as_slice()).unwrap_or(&[]);
            let result = eval_join_expr(&substituted, rep_row)?;
            if !is_truthy(&result) {
                continue;
            }
        }

        let rep_row = group_rows.first().map(|r| r.as_slice()).unwrap_or(&[]);
        let mut row_values = Vec::new();

        for sel_col in &sel.columns {
            match sel_col {
                SelectColumn::Star => {
                    for (qualified_name, val) in rep_row {
                        if hidden_columns.contains(qualified_name) {
                            continue;
                        }
                        let col_name = qualified_name
                            .rsplit('.')
                            .next()
                            .unwrap_or(qualified_name)
                            .to_string();
                        row_values.push((col_name, val.clone()));
                    }
                }
                SelectColumn::Expr(expr, alias) => {
                    let substituted = substitute_aggregates(expr, &aggs, &agg_values);
                    let val = eval_join_expr(&substituted, rep_row)?;
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::ColumnRef(n) => n.clone(),
                        Expr::AggregateFunc {
                            name,
                            arg,
                            distinct,
                        } => {
                            let arg_str = match arg {
                                None => "*".to_string(),
                                Some(a) => {
                                    if *distinct {
                                        format!("DISTINCT {:?}", a)
                                    } else {
                                        format!("{:?}", a)
                                    }
                                }
                            };
                            format!("{}({})", name, arg_str)
                        }
                        _ => "?column?".to_string(),
                    });
                    row_values.push((name, val));
                }
            }
        }

        result_rows.push(Row { values: row_values });
    }

    Ok(result_rows)
}

fn cmp_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(Value::Integer(a)), Some(Value::Integer(b))) => a.cmp(b),
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
    use crate::storage::pager::Pager;
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

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        execute(
            "INSERT INTO t (id, name) VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t (id, name) VALUES (2, 'Bob')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

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

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'Charlie')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

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

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "UPDATE t SET name = 'Alicia' WHERE id = 1",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
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

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
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

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'Charlie')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        let result = execute(
            "SELECT * FROM t ORDER BY id DESC LIMIT 2",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("id"), Some(&Value::Integer(3)));
            assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
        }
    }

    #[test]
    fn test_unique_constraint() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'a@b.com')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        // Duplicate PK
        let result = execute(
            "INSERT INTO t VALUES (1, 'x@y.com')",
            &mut pager,
            &mut catalog,
        );
        assert!(result.is_err());

        // Duplicate UNIQUE
        let result = execute(
            "INSERT INTO t VALUES (2, 'a@b.com')",
            &mut pager,
            &mut catalog,
        );
        assert!(result.is_err());

        // Different value should work
        execute(
            "INSERT INTO t VALUES (2, 'c@d.com')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    #[test]
    fn test_null_values() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Null));
        }
    }

    #[test]
    fn test_many_inserts() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

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
