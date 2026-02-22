use super::*;

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
                // REPLACE INTO: delete the conflicting row, then insert new one
                let existing_data = data_btree.search(pager, &conflict_pk)?.unwrap();
                let existing_values = deserialize_row_versioned(
                    &existing_data,
                    &table_def.columns,
                    table_def.row_format_version,
                )?;
                delete_from_secondary_indexes(
                    &table_def,
                    &mut indexes,
                    &existing_values,
                    &conflict_pk,
                    pager,
                )?;
                data_btree.delete(pager, &conflict_pk)?;
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

        // For REPLACE: also delete rows conflicting on other unique indexes
        // (handles case where PK is new but unique index conflicts with a different row)
        if ins.is_replace {
            replace_delete_unique_conflicts(
                &table_def,
                &mut indexes,
                &values,
                pager,
                &mut data_btree,
            )?;
        } else {
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
