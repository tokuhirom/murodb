use super::*;

pub(super) fn exec_update(
    upd: &Update,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let mut table_def = catalog
        .get_table(pager, &upd.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", upd.table_name)))?;

    // Upgrade v0 tables before writing v1-format rows
    ensure_row_format_v1(&mut table_def, pager, catalog)?;

    let mut indexes = catalog.get_indexes_for_table(pager, &upd.table_name)?;
    let index_columns: Vec<(String, Vec<String>)> = indexes
        .iter()
        .filter(|idx| idx.index_type == IndexType::BTree)
        .map(|idx| (idx.name.clone(), idx.column_names.clone()))
        .collect();
    let plan = plan_select(
        &upd.table_name,
        &table_def.pk_columns,
        &index_columns,
        &upd.where_clause,
    );

    let data_btree = BTree::open(table_def.data_btree_root);

    // Collect rows to update (to avoid modifying during scan)
    let mut to_update: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    match plan {
        Plan::PkSeek { key_exprs, .. }
            if key_exprs.iter().all(|(_, e)| is_row_independent_expr(e)) =>
        {
            let pk_key = eval_pk_seek_key(&table_def, &key_exprs)?;
            if let Some(data) = data_btree.search(pager, &pk_key)? {
                let values = deserialize_row_versioned(
                    &data,
                    &table_def.columns,
                    table_def.row_format_version,
                )?;
                if matches_where(&upd.where_clause, &table_def, &values)? {
                    to_update.push((pk_key, values));
                }
            }
        }
        Plan::IndexSeek {
            index_name,
            column_names,
            key_exprs,
            ..
        } if key_exprs.iter().all(is_row_independent_expr) => {
            let idx_key = eval_index_seek_key(&table_def, &column_names, &key_exprs)?;
            let idx = indexes
                .iter()
                .find(|i| i.name == index_name)
                .ok_or_else(|| MuroError::Execution(format!("Index '{}' not found", index_name)))?;
            let pk_keys = index_seek_pk_keys(idx, &idx_key, pager)?;
            for pk_key in pk_keys {
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if matches_where(&upd.where_clause, &table_def, &values)? {
                        to_update.push((pk_key, values));
                    }
                }
            }
        }
        Plan::IndexRangeSeek {
            index_name,
            column_names,
            prefix_key_exprs,
            lower,
            upper,
            ..
        } if prefix_key_exprs.iter().all(is_row_independent_expr)
            && lower
                .as_ref()
                .is_none_or(|(expr, _)| is_row_independent_expr(expr.as_ref()))
            && upper
                .as_ref()
                .is_none_or(|(expr, _)| is_row_independent_expr(expr.as_ref())) =>
        {
            let prefix_len = prefix_key_exprs.len();
            let bound_columns = column_names[..prefix_len + 1].to_vec();
            let lower_key = lower
                .as_ref()
                .map(|(expr, inclusive)| {
                    let mut key_exprs = prefix_key_exprs.clone();
                    key_exprs.push(*expr.clone());
                    eval_index_seek_key(&table_def, &bound_columns, &key_exprs)
                        .map(|key| (key, *inclusive))
                })
                .transpose()?;
            let upper_key = upper
                .as_ref()
                .map(|(expr, inclusive)| {
                    let mut key_exprs = prefix_key_exprs.clone();
                    key_exprs.push(*expr.clone());
                    eval_index_seek_key(&table_def, &bound_columns, &key_exprs)
                        .map(|key| (key, *inclusive))
                })
                .transpose()?;
            let idx = indexes
                .iter()
                .find(|i| i.name == index_name)
                .ok_or_else(|| MuroError::Execution(format!("Index '{}' not found", index_name)))?;
            let pk_keys = index_seek_pk_keys_range(idx, lower_key, upper_key, pager)?;
            for pk_key in pk_keys {
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if matches_where(&upd.where_clause, &table_def, &values)? {
                        to_update.push((pk_key, values));
                    }
                }
            }
        }
        Plan::PkSeek { .. }
        | Plan::IndexSeek { .. }
        | Plan::IndexRangeSeek { .. }
        | Plan::FullScan { .. }
        | Plan::FtsScan { .. } => {
            data_btree.scan(pager, |k, v| {
                let values =
                    deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
                if matches_where(&upd.where_clause, &table_def, &values)? {
                    to_update.push((k.to_vec(), values));
                }
                Ok(true)
            })?;
        }
    }

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

        // Coerce values to declared column types before index update/serialization.
        for (i, col) in table_def.columns.iter().enumerate() {
            if !new_values[i].is_null() {
                new_values[i] = coerce_value(&new_values[i], col.data_type)?;
            }
        }

        // Validate all values against their column types
        for (i, val) in new_values.iter().enumerate() {
            if !val.is_null() {
                validate_value(val, &table_def.columns[i].data_type)?;
            }
        }

        // Check unique constraints on new values
        check_unique_index_constraints(&table_def, &indexes, &new_values, pager)?;

        // Update secondary indexes: remove old entries, insert new entries
        delete_from_secondary_indexes(&table_def, &mut indexes, &old_values, &pk_key, pager)?;
        insert_into_secondary_indexes(&table_def, &mut indexes, &new_values, &pk_key, pager)?;

        let row_data = serialize_row(&new_values, &table_def.columns);
        data_btree.insert(pager, &pk_key, &row_data)?;
        count += 1;
    }

    persist_indexes(catalog, pager, &indexes)?;
    Ok(ExecResult::RowsAffected(count))
}

pub(super) fn exec_delete(
    del: &Delete,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &del.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", del.table_name)))?;

    let mut indexes = catalog.get_indexes_for_table(pager, &del.table_name)?;
    let index_columns: Vec<(String, Vec<String>)> = indexes
        .iter()
        .filter(|idx| idx.index_type == IndexType::BTree)
        .map(|idx| (idx.name.clone(), idx.column_names.clone()))
        .collect();
    let plan = plan_select(
        &del.table_name,
        &table_def.pk_columns,
        &index_columns,
        &del.where_clause,
    );

    let data_btree = BTree::open(table_def.data_btree_root);

    // Collect keys and row values to delete
    let mut to_delete: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    match plan {
        Plan::PkSeek { key_exprs, .. }
            if key_exprs.iter().all(|(_, e)| is_row_independent_expr(e)) =>
        {
            let pk_key = eval_pk_seek_key(&table_def, &key_exprs)?;
            if let Some(data) = data_btree.search(pager, &pk_key)? {
                let values = deserialize_row_versioned(
                    &data,
                    &table_def.columns,
                    table_def.row_format_version,
                )?;
                if matches_where(&del.where_clause, &table_def, &values)? {
                    to_delete.push((pk_key, values));
                }
            }
        }
        Plan::IndexSeek {
            index_name,
            column_names,
            key_exprs,
            ..
        } if key_exprs.iter().all(is_row_independent_expr) => {
            let idx_key = eval_index_seek_key(&table_def, &column_names, &key_exprs)?;
            let idx = indexes
                .iter()
                .find(|i| i.name == index_name)
                .ok_or_else(|| MuroError::Execution(format!("Index '{}' not found", index_name)))?;
            let pk_keys = index_seek_pk_keys(idx, &idx_key, pager)?;
            for pk_key in pk_keys {
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if matches_where(&del.where_clause, &table_def, &values)? {
                        to_delete.push((pk_key, values));
                    }
                }
            }
        }
        Plan::IndexRangeSeek {
            index_name,
            column_names,
            prefix_key_exprs,
            lower,
            upper,
            ..
        } if prefix_key_exprs.iter().all(is_row_independent_expr)
            && lower
                .as_ref()
                .is_none_or(|(expr, _)| is_row_independent_expr(expr.as_ref()))
            && upper
                .as_ref()
                .is_none_or(|(expr, _)| is_row_independent_expr(expr.as_ref())) =>
        {
            let prefix_len = prefix_key_exprs.len();
            let bound_columns = column_names[..prefix_len + 1].to_vec();
            let lower_key = lower
                .as_ref()
                .map(|(expr, inclusive)| {
                    let mut key_exprs = prefix_key_exprs.clone();
                    key_exprs.push(*expr.clone());
                    eval_index_seek_key(&table_def, &bound_columns, &key_exprs)
                        .map(|key| (key, *inclusive))
                })
                .transpose()?;
            let upper_key = upper
                .as_ref()
                .map(|(expr, inclusive)| {
                    let mut key_exprs = prefix_key_exprs.clone();
                    key_exprs.push(*expr.clone());
                    eval_index_seek_key(&table_def, &bound_columns, &key_exprs)
                        .map(|key| (key, *inclusive))
                })
                .transpose()?;
            let idx = indexes
                .iter()
                .find(|i| i.name == index_name)
                .ok_or_else(|| MuroError::Execution(format!("Index '{}' not found", index_name)))?;
            let pk_keys = index_seek_pk_keys_range(idx, lower_key, upper_key, pager)?;
            for pk_key in pk_keys {
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if matches_where(&del.where_clause, &table_def, &values)? {
                        to_delete.push((pk_key, values));
                    }
                }
            }
        }
        Plan::PkSeek { .. }
        | Plan::IndexSeek { .. }
        | Plan::IndexRangeSeek { .. }
        | Plan::FullScan { .. }
        | Plan::FtsScan { .. } => {
            data_btree.scan(pager, |k, v| {
                let values =
                    deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
                if matches_where(&del.where_clause, &table_def, &values)? {
                    to_delete.push((k.to_vec(), values));
                }
                Ok(true)
            })?;
        }
    }

    let mut data_btree = BTree::open(table_def.data_btree_root);
    let mut count = 0u64;

    for (pk_key, values) in &to_delete {
        delete_from_secondary_indexes(&table_def, &mut indexes, values, pk_key, pager)?;
        data_btree.delete(pager, pk_key)?;
        count += 1;
    }

    persist_indexes(catalog, pager, &indexes)?;
    Ok(ExecResult::RowsAffected(count))
}
