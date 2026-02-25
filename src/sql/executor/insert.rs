use super::*;
use std::collections::HashSet;

pub(super) fn exec_insert(
    ins: &Insert,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let mut table_def = catalog
        .get_table(pager, &ins.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", ins.table_name)))?;

    // Upgrade v0 tables before writing v1-format rows
    ensure_row_format_v1(&mut table_def, pager, catalog)?;

    let mut indexes = catalog.get_indexes_for_table(pager, &ins.table_name)?;

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut rows_inserted = 0u64;

    for value_row in &ins.values {
        cancellation_point()?;
        let mut values = resolve_insert_values(&table_def, &ins.columns, value_row)?;

        // Apply DEFAULT values for NULL columns that have defaults
        for (i, col) in table_def.columns.iter().enumerate() {
            if values[i].is_null() && !col.is_hidden {
                if let Some(default) = &col.default_value {
                    values[i] = match default {
                        DefaultValue::Integer(n) => Value::Integer(*n),
                        DefaultValue::Float(n) => Value::Float(*n),
                        DefaultValue::String(s) => Value::Varchar(s.clone()),
                        DefaultValue::Null => Value::Null,
                    };
                }
            }
        }

        // Auto-generate for AUTO_INCREMENT / hidden _rowid columns
        let pk_indices = table_def.pk_column_indices();
        if pk_indices.len() == 1 {
            let pk_idx = pk_indices[0];
            if (table_def.columns[pk_idx].auto_increment || table_def.columns[pk_idx].is_hidden)
                && values[pk_idx].is_null()
            {
                table_def.next_rowid += 1;
                values[pk_idx] = Value::Integer(table_def.next_rowid);
            }
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

        // Coerce values to declared column types before validation/serialization.
        for (i, col) in table_def.columns.iter().enumerate() {
            if !values[i].is_null() {
                values[i] = coerce_value(&values[i], col.data_type)?;
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

        enforce_child_foreign_keys(&table_def, &values, pager, catalog)?;

        let pk_key = encode_pk_key(&table_def, &values);

        // Detect conflicts: PK first, then unique indexes
        let pk_duplicate = data_btree.search(pager, &pk_key)?.is_some();
        // For ON DUPLICATE KEY UPDATE / REPLACE, also check unique index conflicts
        let unique_conflict_pk = if !pk_duplicate {
            find_unique_index_conflict(&table_def, &indexes, &values, pager)?
        } else {
            None
        };
        let has_conflict = pk_duplicate || unique_conflict_pk.is_some();
        // The PK key of the conflicting row (same as pk_key for PK conflict,
        // or the existing row's PK for unique index conflict)
        let conflict_pk_key = if pk_duplicate {
            Some(pk_key.clone())
        } else {
            unique_conflict_pk
        };

        if has_conflict {
            let conflict_pk = conflict_pk_key.unwrap();
            if ins.is_replace {
                // REPLACE INTO: pre-collect all conflicting rows (PK + all UNIQUE conflicts),
                // validate FK effects for all of them, then delete.
                let conflicts = collect_replace_conflicts(
                    &table_def,
                    &indexes,
                    &values,
                    &conflict_pk,
                    pager,
                    &data_btree,
                )?;
                let deleting_rows: Vec<Vec<Value>> =
                    conflicts.iter().map(|(_, row)| row.clone()).collect();
                let deleting_keys: Vec<Vec<u8>> =
                    conflicts.iter().map(|(pk, _)| pk.clone()).collect();
                validate_replace_child_foreign_keys_after_conflict_deletes(
                    &table_def,
                    &values,
                    &deleting_keys,
                    pager,
                    catalog,
                )?;
                enforce_parent_restrict_on_delete(
                    &table_def,
                    &deleting_rows,
                    &deleting_keys,
                    pager,
                    catalog,
                )?;
                for (pk, existing_values) in conflicts {
                    delete_from_secondary_indexes(
                        &table_def,
                        &mut indexes,
                        &existing_values,
                        &pk,
                        pager,
                    )?;
                    data_btree.delete(pager, &pk)?;
                }
                data_btree = BTree::open(data_btree.root_page_id());
            } else if let Some(ref assignments) = ins.on_duplicate_key_update {
                // ON DUPLICATE KEY UPDATE: read original, apply updates, write back
                let existing_data = data_btree.search(pager, &conflict_pk)?.unwrap();
                let original_values = deserialize_row_versioned(
                    &existing_data,
                    &table_def.columns,
                    table_def.row_format_version,
                )?;
                let mut updated_values = original_values.clone();

                // Apply update assignments (expressions can reference current row values)
                for (col_name, expr) in assignments {
                    let col_idx = table_def.column_index(col_name).ok_or_else(|| {
                        MuroError::Execution(format!("Unknown column: {}", col_name))
                    })?;
                    let val = eval_expr(expr, &|name| {
                        table_def
                            .column_index(name)
                            .and_then(|idx| updated_values.get(idx).cloned())
                    })?;
                    updated_values[col_idx] = val;
                }

                // Check unique constraints on updated values (excluding self)
                // Must be done BEFORE deleting indexes to avoid inconsistency on error
                check_unique_index_constraints_excluding(
                    &table_def,
                    &indexes,
                    &updated_values,
                    &conflict_pk,
                    pager,
                )?;
                enforce_child_foreign_keys(&table_def, &updated_values, pager, catalog)?;
                enforce_parent_restrict_on_update(
                    &table_def,
                    &conflict_pk,
                    &original_values,
                    &updated_values,
                    pager,
                    catalog,
                )?;

                // Delete old secondary index entries using original values
                delete_from_secondary_indexes(
                    &table_def,
                    &mut indexes,
                    &original_values,
                    &conflict_pk,
                    pager,
                )?;

                // Update the data row (delete + insert)
                let row_data = serialize_row(&updated_values, &table_def.columns);
                data_btree.delete(pager, &conflict_pk)?;
                data_btree = BTree::open(data_btree.root_page_id());
                data_btree.insert(pager, &conflict_pk, &row_data)?;

                // Insert new secondary index entries with updated values
                insert_into_secondary_indexes(
                    &table_def,
                    &mut indexes,
                    &updated_values,
                    &conflict_pk,
                    pager,
                )?;

                // Update table_def
                table_def.data_btree_root = data_btree.root_page_id();
                catalog.update_table(pager, &table_def)?;
                persist_indexes(catalog, pager, &indexes)?;

                // MySQL reports 2 affected rows for ON DUPLICATE KEY UPDATE
                rows_inserted += 2;
                continue;
            } else if pk_duplicate {
                return Err(MuroError::UniqueViolation(
                    "Duplicate primary key".to_string(),
                ));
            } else {
                return Err(MuroError::UniqueViolation(
                    "Duplicate value in unique index".to_string(),
                ));
            }
        }

        if !ins.is_replace {
            check_unique_index_constraints(&table_def, &indexes, &values, pager)?;
        }

        // Serialize row and insert into data B-tree
        let row_data = serialize_row(&values, &table_def.columns);
        data_btree.insert(pager, &pk_key, &row_data)?;

        // Update secondary indexes
        insert_into_secondary_indexes(&table_def, &mut indexes, &values, &pk_key, pager)?;

        // Update table_def if btree root changed or next_rowid changed
        table_def.data_btree_root = data_btree.root_page_id();
        catalog.update_table(pager, &table_def)?;
        persist_indexes(catalog, pager, &indexes)?;

        rows_inserted += 1;
    }

    Ok(ExecResult::RowsAffected(rows_inserted))
}

fn collect_replace_conflicts(
    table_def: &TableDef,
    indexes: &[IndexDef],
    new_values: &[Value],
    conflict_pk: &[u8],
    pager: &mut impl PageStore,
    data_btree: &BTree,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let mut conflict_keys: HashSet<Vec<u8>> = HashSet::new();
    conflict_keys.insert(conflict_pk.to_vec());
    for idx in indexes {
        if !idx.is_unique {
            continue;
        }
        let col_indices: Vec<usize> = idx
            .column_names
            .iter()
            .filter_map(|cn| table_def.column_index(cn))
            .collect();
        if col_indices.len() != idx.column_names.len() {
            continue;
        }
        let is_composite = idx.column_names.len() > 1;
        let encoded =
            encode_index_key_from_row(new_values, &col_indices, &table_def.columns, is_composite);
        if let Some(idx_key) = encoded {
            let idx_btree = BTree::open(idx.btree_root);
            if let Some(existing_pk_key) = idx_btree.search(pager, &idx_key)? {
                conflict_keys.insert(existing_pk_key);
            }
        }
    }
    let mut conflicts = Vec::new();
    for pk in conflict_keys {
        cancellation_point()?;
        if let Some(existing_data) = data_btree.search(pager, &pk)? {
            let existing_values = deserialize_row_versioned(
                &existing_data,
                &table_def.columns,
                table_def.row_format_version,
            )?;
            conflicts.push((pk, existing_values));
        }
    }
    Ok(conflicts)
}

fn validate_replace_child_foreign_keys_after_conflict_deletes(
    table_def: &TableDef,
    row_values: &[Value],
    deleting_pk_keys: &[Vec<u8>],
    pager: &mut impl PageStore,
    _catalog: &mut SystemCatalog,
) -> Result<()> {
    if deleting_pk_keys.is_empty() {
        return Ok(());
    }

    let deleting_pk_set: HashSet<Vec<u8>> = deleting_pk_keys.iter().cloned().collect();
    for fk in &table_def.foreign_keys {
        if fk.ref_table != table_def.name {
            continue;
        }

        let mut child_values = Vec::with_capacity(fk.columns.len());
        for col in &fk.columns {
            let idx = table_def.column_index(col).ok_or_else(|| {
                MuroError::Schema(format!("Column '{}.{}' not found", table_def.name, col))
            })?;
            child_values.push(row_values[idx].clone());
        }
        if child_values.iter().any(Value::is_null) {
            continue;
        }

        let data_btree = BTree::open(table_def.data_btree_root);
        let mut exists = false;
        data_btree.scan(pager, |_pk, row| {
            let parent_row =
                deserialize_row_versioned(row, &table_def.columns, table_def.row_format_version)?;
            let parent_pk = encode_pk_key(table_def, &parent_row);
            if deleting_pk_set.contains(&parent_pk) {
                return Ok(true);
            }

            for (i, ref_col) in fk.ref_columns.iter().enumerate() {
                let idx = table_def.column_index(ref_col).ok_or_else(|| {
                    MuroError::Schema(format!("Column '{}.{}' not found", table_def.name, ref_col))
                })?;
                if parent_row[idx] != child_values[i] {
                    return Ok(true);
                }
            }
            exists = true;
            Ok(false)
        })?;

        if !exists {
            return Err(MuroError::Execution(format!(
                "FOREIGN KEY constraint fails: ({}) REFERENCES {}({})",
                fk.columns.join(", "),
                fk.ref_table,
                fk.ref_columns.join(", "),
            )));
        }
    }
    Ok(())
}

/// Convert a Value to a literal Expr.
pub(super) fn resolve_insert_values(
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
