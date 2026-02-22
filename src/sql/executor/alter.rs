use super::*;

pub(super) fn exec_alter_table(
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

pub(super) fn exec_alter_add_column(
    mut table_def: TableDef,
    col_spec: &ColumnSpec,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    validate_column_collation(
        &col_spec.name,
        col_spec.data_type,
        col_spec.collation.as_deref(),
    )?;

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

    // Upgrade v0 tables: ADD COLUMN relies on v1 format's stored column count
    // to know how many columns old rows have (short-row tolerance).
    ensure_row_format_v1(&mut table_def, pager, catalog)?;

    // NOT NULL without DEFAULT: error if table has existing rows
    if !col_spec.is_nullable && col_spec.default_value.is_none() {
        let data_btree = BTree::open(table_def.data_btree_root);
        let mut has_rows = false;
        data_btree.scan(pager, |_k, _v| {
            has_rows = true;
            Ok(false) // stop after first row
        })?;
        if has_rows {
            return Err(MuroError::Schema(format!(
                "Cannot add NOT NULL column '{}' without DEFAULT to a table with existing rows",
                col_spec.name
            )));
        }
    }

    let mut col = ColumnDef::new(&col_spec.name, col_spec.data_type);
    if let Some(collation) = &col_spec.collation {
        col = col.with_collation(collation);
    }
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

    // Create unique index if UNIQUE was specified, and backfill existing rows
    if col_spec.is_unique && !col_spec.is_primary_key {
        let new_col = table_def.columns.last().unwrap();
        let default_val = default_value_for_column(new_col);

        let idx_btree = BTree::create(pager)?;
        let mut idx_btree_mut = BTree::open(idx_btree.root_page_id());

        // Backfill: insert default value for all existing rows into the index.
        // For non-NULL defaults, duplicates are detected during backfill.
        if !default_val.is_null() {
            let idx_key = encode_value(&default_val, &new_col.data_type);
            let data_btree = BTree::open(table_def.data_btree_root);
            let mut pk_keys: Vec<Vec<u8>> = Vec::new();
            data_btree.scan(pager, |k, _v| {
                pk_keys.push(k.to_vec());
                Ok(true)
            })?;

            if pk_keys.len() > 1 {
                return Err(MuroError::Schema(format!(
                    "Cannot add UNIQUE column '{}' with non-NULL DEFAULT: {} existing rows would all have the same value",
                    col_spec.name, pk_keys.len()
                )));
            }
            for pk_key in &pk_keys {
                idx_btree_mut.insert(pager, &idx_key, pk_key)?;
            }
        }

        let idx_def = IndexDef {
            name: format!("auto_unique_{}_{}", table_def.name, col_spec.name),
            table_name: table_def.name.clone(),
            column_names: vec![col_spec.name.clone()],
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: idx_btree_mut.root_page_id(),
            stats_distinct_keys: 0,
            fts_stop_filter: false,
            fts_stop_df_ratio_ppm: 0,
        };
        catalog.create_index(pager, idx_def)?;
    }

    Ok(ExecResult::Ok)
}

pub(super) fn exec_alter_drop_column(
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
        if idx.column_names.contains(&col_name.to_string()) {
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
        let row_values = deserialize_row_versioned(v, &old_columns, table_def.row_format_version)?;
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
    table_def.row_format_version = 1; // rewritten rows are v1 format
    catalog.update_table(pager, &table_def)?;

    Ok(ExecResult::Ok)
}

pub(super) fn exec_alter_modify_column(
    mut table_def: TableDef,
    col_spec: &ColumnSpec,
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    validate_column_collation(
        &col_spec.name,
        col_spec.data_type,
        col_spec.collation.as_deref(),
    )?;

    let col_idx = table_def.column_index(&col_spec.name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            col_spec.name, table_name
        ))
    })?;

    let old_col = &table_def.columns[col_idx];
    let type_changed = old_col.data_type != col_spec.data_type;
    let adding_not_null = old_col.is_nullable && !col_spec.is_nullable;

    // If adding NOT NULL constraint, validate existing rows
    if adding_not_null {
        validate_no_nulls_in_column(&table_def, col_idx, pager)?;
    }

    if type_changed {
        // Full table rewrite with type coercion
        let old_columns = table_def.columns.clone();
        let data_btree = BTree::open(table_def.data_btree_root);

        let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
        data_btree.scan(pager, |k, v| {
            let row_values =
                deserialize_row_versioned(v, &old_columns, table_def.row_format_version)?;
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
        table_def.row_format_version = 1; // rewritten rows are v1 format
    } else {
        // Metadata-only change
        update_column_def(&mut table_def.columns[col_idx], col_spec);
    }

    catalog.update_table(pager, &table_def)?;

    // Reconcile unique index: create or drop as needed
    reconcile_unique_index(&table_def, col_spec, &col_spec.name, pager, catalog)?;

    Ok(ExecResult::Ok)
}

pub(super) fn exec_alter_change_column(
    mut table_def: TableDef,
    old_name: &str,
    col_spec: &ColumnSpec,
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    validate_column_collation(
        &col_spec.name,
        col_spec.data_type,
        col_spec.collation.as_deref(),
    )?;

    let col_idx = table_def.column_index(old_name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            old_name, table_name
        ))
    })?;

    let old_col = &table_def.columns[col_idx];
    let type_changed = old_col.data_type != col_spec.data_type;
    let adding_not_null = old_col.is_nullable && !col_spec.is_nullable;

    // If adding NOT NULL constraint, validate existing rows
    if adding_not_null {
        validate_no_nulls_in_column(&table_def, col_idx, pager)?;
    }

    // Update any indexes referencing the old column name
    let indexes = catalog.get_indexes_for_table(pager, table_name)?;
    for mut idx in indexes {
        let mut changed = false;
        for cn in &mut idx.column_names {
            if cn == old_name {
                *cn = col_spec.name.clone();
                changed = true;
            }
        }
        if changed {
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
            let row_values =
                deserialize_row_versioned(v, &old_columns, table_def.row_format_version)?;
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
        table_def.row_format_version = 1; // rewritten rows are v1 format
    } else {
        update_column_def(&mut table_def.columns[col_idx], col_spec);
    }

    catalog.update_table(pager, &table_def)?;

    // Reconcile unique index: create or drop as needed
    reconcile_unique_index(&table_def, col_spec, old_name, pager, catalog)?;

    Ok(ExecResult::Ok)
}

/// Reconcile unique index for a column after ALTER TABLE MODIFY/CHANGE.
/// Creates a new unique index if UNIQUE was added, or drops existing one if UNIQUE was removed.
pub(super) fn reconcile_unique_index(
    table_def: &TableDef,
    col_spec: &ColumnSpec,
    old_col_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    let indexes = catalog.get_indexes_for_table(pager, &table_def.name)?;
    let existing_unique = indexes.iter().find(|idx| {
        idx.is_unique
            && idx.column_names.len() == 1
            && (idx.column_names[0] == col_spec.name || idx.column_names[0] == old_col_name)
    });

    if col_spec.is_unique && existing_unique.is_none() {
        // Need to create a unique index — first verify no duplicates
        let col_idx = table_def
            .column_index(&col_spec.name)
            .ok_or_else(|| MuroError::Schema(format!("Column '{}' not found", col_spec.name)))?;
        let data_btree = BTree::open(table_def.data_btree_root);
        let col_data_type = table_def.columns[col_idx].data_type;

        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        let mut idx_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        data_btree.scan(pager, |pk_key, v| {
            let row_values =
                deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
            if col_idx < row_values.len() {
                let val = &row_values[col_idx];
                if !val.is_null() {
                    let encoded = encode_value(val, &col_data_type);
                    if seen_keys.contains(&encoded) {
                        return Err(MuroError::UniqueViolation(format!(
                            "Duplicate value in column '{}'; cannot add UNIQUE constraint",
                            col_spec.name
                        )));
                    }
                    seen_keys.push(encoded.clone());
                    idx_entries.push((encoded, pk_key.to_vec()));
                }
            }
            Ok(true)
        })?;

        let idx_btree = BTree::create(pager)?;
        let mut idx_btree_mut = BTree::open(idx_btree.root_page_id());
        for (idx_key, pk_key) in &idx_entries {
            idx_btree_mut.insert(pager, idx_key, pk_key)?;
        }

        let idx_def = IndexDef {
            name: format!("auto_unique_{}_{}", table_def.name, col_spec.name),
            table_name: table_def.name.clone(),
            column_names: vec![col_spec.name.clone()],
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: idx_btree_mut.root_page_id(),
            stats_distinct_keys: 0,
            fts_stop_filter: false,
            fts_stop_df_ratio_ppm: 0,
        };
        catalog.create_index(pager, idx_def)?;
    } else if !col_spec.is_unique {
        if let Some(idx) = existing_unique {
            // Drop the unique index since UNIQUE was removed
            let idx_btree = BTree::open(idx.btree_root);
            let pages = idx_btree.collect_all_pages(pager)?;
            for page_id in pages {
                pager.free_page(page_id);
            }
            catalog.delete_index(pager, &idx.name)?;
        }
    }
    Ok(())
}

/// Validate that no rows have NULL in the given column.
pub(super) fn validate_no_nulls_in_column(
    table_def: &TableDef,
    col_idx: usize,
    pager: &mut impl PageStore,
) -> Result<()> {
    let data_btree = BTree::open(table_def.data_btree_root);
    let col_name = &table_def.columns[col_idx].name;
    data_btree.scan(pager, |_k, v| {
        let row_values =
            deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
        if col_idx < row_values.len() && row_values[col_idx].is_null() {
            return Err(MuroError::Schema(format!(
                "Column '{}' contains NULL values; cannot set NOT NULL",
                col_name
            )));
        }
        Ok(true)
    })?;
    Ok(())
}

/// Update a ColumnDef in place from a ColumnSpec.
pub(super) fn update_column_def(col: &mut ColumnDef, spec: &ColumnSpec) {
    col.name = spec.name.clone();
    col.data_type = spec.data_type;
    col.collation = spec.collation.clone();
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
pub(super) fn coerce_value(value: &Value, target_type: DataType) -> Result<Value> {
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0; // -2^63
    const I64_UPPER_EXCLUSIVE_F64: f64 = 9_223_372_036_854_775_808.0; // 2^63

    fn float_to_i64_checked(n: f64) -> Result<i64> {
        if !n.is_finite() {
            return Err(MuroError::Execution(format!(
                "Cannot convert non-finite float '{}' to integer",
                n
            )));
        }
        if !(I64_MIN_F64..I64_UPPER_EXCLUSIVE_F64).contains(&n) {
            return Err(MuroError::Execution(format!(
                "Float '{}' out of range for integer conversion",
                n
            )));
        }
        Ok(n as i64)
    }

    fn validate_float_for_target(n: f64, target_type: DataType) -> Result<f64> {
        if !n.is_finite() {
            return Err(MuroError::Execution(format!(
                "Cannot convert non-finite float '{}' to {}",
                n, target_type
            )));
        }
        if target_type == DataType::Float && (n < f32::MIN as f64 || n > f32::MAX as f64) {
            return Err(MuroError::Execution(format!(
                "Float '{}' out of range for FLOAT",
                n
            )));
        }
        Ok(n)
    }

    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(n) => match target_type {
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                Ok(Value::Integer(*n))
            }
            DataType::Float | DataType::Double => Ok(Value::Float(validate_float_for_target(
                *n as f64,
                target_type,
            )?)),
            DataType::Date | DataType::DateTime | DataType::Timestamp => Err(MuroError::Execution(
                "Cannot coerce integer to date/time type".into(),
            )),
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(n.to_string())),
            DataType::Varbinary(_) => Err(MuroError::Execution(
                "Cannot coerce integer to VARBINARY".into(),
            )),
        },
        Value::Float(n) => match target_type {
            DataType::Float | DataType::Double => {
                Ok(Value::Float(validate_float_for_target(*n, target_type)?))
            }
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                Ok(Value::Integer(float_to_i64_checked(*n)?))
            }
            DataType::Date | DataType::DateTime | DataType::Timestamp => Err(MuroError::Execution(
                "Cannot coerce float to date/time type".into(),
            )),
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(n.to_string())),
            DataType::Varbinary(_) => Err(MuroError::Execution(
                "Cannot coerce floating-point value to VARBINARY".into(),
            )),
        },
        Value::Varchar(s) => match target_type {
            DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
                let n: i64 = s.parse().map_err(|_| {
                    MuroError::Execution(format!("Cannot convert '{}' to integer", s))
                })?;
                Ok(Value::Integer(n))
            }
            DataType::Float | DataType::Double => {
                let n: f64 = s.parse().map_err(|_| {
                    MuroError::Execution(format!("Cannot convert '{}' to float", s))
                })?;
                Ok(Value::Float(validate_float_for_target(n, target_type)?))
            }
            DataType::Date => {
                let d = parse_date_string(s).ok_or_else(|| {
                    MuroError::Execution(format!("Cannot convert '{}' to DATE", s))
                })?;
                Ok(Value::Date(d))
            }
            DataType::DateTime => {
                let dt = parse_datetime_string(s).ok_or_else(|| {
                    MuroError::Execution(format!("Cannot convert '{}' to DATETIME", s))
                })?;
                Ok(Value::DateTime(dt))
            }
            DataType::Timestamp => {
                let ts = parse_timestamp_string(s).ok_or_else(|| {
                    MuroError::Execution(format!("Cannot convert '{}' to TIMESTAMP", s))
                })?;
                Ok(Value::Timestamp(ts))
            }
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(s.clone())),
            DataType::Varbinary(_) => Ok(Value::Varbinary(s.as_bytes().to_vec())),
        },
        Value::Date(d) => match target_type {
            DataType::Date => Ok(Value::Date(*d)),
            DataType::DateTime => Ok(Value::DateTime((*d as i64) * 1_000_000)),
            DataType::Timestamp => Ok(Value::Timestamp((*d as i64) * 1_000_000)),
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(format_date(*d))),
            _ => Err(MuroError::Execution(
                "Cannot coerce DATE to target type".into(),
            )),
        },
        Value::DateTime(dt) => match target_type {
            DataType::DateTime => Ok(Value::DateTime(*dt)),
            DataType::Timestamp => Ok(Value::Timestamp(*dt)),
            DataType::Date => Ok(Value::Date((*dt / 1_000_000) as i32)),
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(format_datetime(*dt))),
            _ => Err(MuroError::Execution(
                "Cannot coerce DATETIME to target type".into(),
            )),
        },
        Value::Timestamp(ts) => match target_type {
            DataType::Timestamp => Ok(Value::Timestamp(*ts)),
            DataType::DateTime => Ok(Value::DateTime(*ts)),
            DataType::Date => Ok(Value::Date((*ts / 1_000_000) as i32)),
            DataType::Varchar(_) | DataType::Text => Ok(Value::Varchar(format_datetime(*ts))),
            _ => Err(MuroError::Execution(
                "Cannot coerce TIMESTAMP to target type".into(),
            )),
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
