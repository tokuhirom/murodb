use super::*;

pub(super) fn encode_index_key_from_row(
    row_values: &[Value],
    col_indices: &[usize],
    columns: &[crate::schema::column::ColumnDef],
    is_composite: bool,
) -> Option<Vec<u8>> {
    if is_composite {
        // For composite: skip if any value is NULL
        let mut vals = Vec::new();
        let mut types = Vec::new();
        for &ci in col_indices {
            if ci >= row_values.len() || row_values[ci].is_null() {
                return None;
            }
            vals.push(&row_values[ci]);
            types.push(&columns[ci].data_type);
        }
        Some(encode_composite_key(&vals, &types))
    } else {
        let ci = col_indices[0];
        if ci >= row_values.len() || row_values[ci].is_null() {
            return None;
        }
        Some(encode_value(&row_values[ci], &columns[ci].data_type))
    }
}

/// Evaluate PK seek key from planner key expressions.
pub(super) fn eval_pk_seek_key(
    table_def: &TableDef,
    key_exprs: &[(String, Expr)],
) -> Result<Vec<u8>> {
    if table_def.is_composite_pk() {
        let mut vals = Vec::new();
        let mut types = Vec::new();
        for (col_name, expr) in key_exprs {
            let val = eval_expr(expr, &|_| None)?;
            let col_idx = table_def.column_index(col_name).ok_or_else(|| {
                MuroError::Execution(format!("PK column '{}' not found", col_name))
            })?;
            types.push(table_def.columns[col_idx].data_type);
            vals.push(val);
        }
        let val_refs: Vec<&Value> = vals.iter().collect();
        let type_refs: Vec<&DataType> = types.iter().collect();
        Ok(encode_composite_key(&val_refs, &type_refs))
    } else {
        let (col_name, expr) = &key_exprs[0];
        let key_val = eval_expr(expr, &|_| None)?;
        let col_idx = table_def
            .column_index(col_name)
            .ok_or_else(|| MuroError::Execution(format!("PK column '{}' not found", col_name)))?;
        Ok(encode_value(
            &key_val,
            &table_def.columns[col_idx].data_type,
        ))
    }
}

/// Evaluate index seek key from planner key expressions.
pub(super) fn eval_index_seek_key(
    table_def: &TableDef,
    column_names: &[String],
    key_exprs: &[Expr],
) -> Result<Vec<u8>> {
    if column_names.len() > 1 {
        let mut vals = Vec::new();
        let mut types = Vec::new();
        for (col_name, expr) in column_names.iter().zip(key_exprs.iter()) {
            let val = eval_expr(expr, &|_| None)?;
            let col_idx = table_def.column_index(col_name).ok_or_else(|| {
                MuroError::Execution(format!("Index column '{}' not found", col_name))
            })?;
            types.push(table_def.columns[col_idx].data_type);
            vals.push(val);
        }
        let val_refs: Vec<&Value> = vals.iter().collect();
        let type_refs: Vec<&DataType> = types.iter().collect();
        Ok(encode_composite_key(&val_refs, &type_refs))
    } else {
        let key_val = eval_expr(&key_exprs[0], &|_| None)?;
        let col_idx = table_def.column_index(&column_names[0]).ok_or_else(|| {
            MuroError::Execution(format!("Index column '{}' not found", column_names[0]))
        })?;
        Ok(encode_value(
            &key_val,
            &table_def.columns[col_idx].data_type,
        ))
    }
}

/// Encode the primary key for a row.
pub(super) fn encode_pk_key(table_def: &TableDef, values: &[Value]) -> Vec<u8> {
    if table_def.is_composite_pk() {
        let pk_indices = table_def.pk_column_indices();
        let pk_vals: Vec<&Value> = pk_indices.iter().map(|&i| &values[i]).collect();
        let pk_types: Vec<&DataType> = pk_indices
            .iter()
            .map(|&i| &table_def.columns[i].data_type)
            .collect();
        encode_composite_key(&pk_vals, &pk_types)
    } else if let Some(pk_idx) = table_def.pk_column_index() {
        encode_value(&values[pk_idx], &table_def.columns[pk_idx].data_type)
    } else {
        Vec::new()
    }
}

/// Look up PK keys from an index for a given index key.
/// For unique indexes, uses exact search. For non-unique indexes,
/// uses prefix scan to find all matching entries.
pub(super) fn index_seek_pk_keys(
    idx: &IndexDef,
    idx_key: &[u8],
    pager: &mut impl PageStore,
) -> Result<Vec<Vec<u8>>> {
    let idx_btree = BTree::open(idx.btree_root);
    if idx.is_unique {
        if let Some(pk_key) = idx_btree.search(pager, idx_key)? {
            Ok(vec![pk_key])
        } else {
            Ok(vec![])
        }
    } else {
        // Non-unique: scan entries whose key starts with idx_key prefix
        let mut pk_keys = Vec::new();
        idx_btree.scan_from(pager, idx_key, |k, v| {
            if k.starts_with(idx_key) {
                pk_keys.push(v.to_vec());
                Ok(true)
            } else {
                Ok(false) // past the prefix range, stop scanning
            }
        })?;
        Ok(pk_keys)
    }
}

/// Check unique index constraints for a set of values.
pub(super) fn check_unique_index_constraints(
    table_def: &TableDef,
    indexes: &[IndexDef],
    values: &[Value],
    pager: &mut impl PageStore,
) -> Result<()> {
    for idx in indexes {
        if idx.is_unique {
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
                encode_index_key_from_row(values, &col_indices, &table_def.columns, is_composite);
            if let Some(idx_key) = encoded {
                let idx_btree = BTree::open(idx.btree_root);
                if idx_btree.search(pager, &idx_key)?.is_some() {
                    return Err(MuroError::UniqueViolation(format!(
                        "Duplicate value in unique column(s) '{}'",
                        idx.column_names.join(", ")
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Find the first unique index conflict for the given values.
/// Returns the PK key of the conflicting row, or None if no conflict.
pub(super) fn find_unique_index_conflict(
    table_def: &TableDef,
    indexes: &[IndexDef],
    values: &[Value],
    pager: &mut impl PageStore,
) -> Result<Option<Vec<u8>>> {
    for idx in indexes {
        if idx.is_unique {
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
                encode_index_key_from_row(values, &col_indices, &table_def.columns, is_composite);
            if let Some(idx_key) = encoded {
                let idx_btree = BTree::open(idx.btree_root);
                if let Some(existing_pk_key) = idx_btree.search(pager, &idx_key)? {
                    return Ok(Some(existing_pk_key));
                }
            }
        }
    }
    Ok(None)
}

/// Check unique index constraints for updated values, excluding the row identified by `excluded_pk`.
/// This prevents false positives when the row's own index entry is still present.
pub(super) fn check_unique_index_constraints_excluding(
    table_def: &TableDef,
    indexes: &[IndexDef],
    values: &[Value],
    excluded_pk: &[u8],
    pager: &mut impl PageStore,
) -> Result<()> {
    for idx in indexes {
        if idx.is_unique {
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
                encode_index_key_from_row(values, &col_indices, &table_def.columns, is_composite);
            if let Some(idx_key) = encoded {
                let idx_btree = BTree::open(idx.btree_root);
                if let Some(existing_pk_key) = idx_btree.search(pager, &idx_key)? {
                    // Skip if the conflicting entry belongs to the row we're updating
                    if existing_pk_key != excluded_pk {
                        return Err(MuroError::UniqueViolation(format!(
                            "Duplicate value in unique column(s) '{}'",
                            idx.column_names.join(", ")
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

/// For REPLACE INTO: delete rows that conflict on any unique index.
/// MySQL's REPLACE deletes ALL conflicting rows (PK + all unique indexes).
pub(super) fn replace_delete_unique_conflicts(
    table_def: &TableDef,
    indexes: &mut [IndexDef],
    new_values: &[Value],
    pager: &mut impl PageStore,
    data_btree: &mut BTree,
) -> Result<()> {
    for idx_pos in 0..indexes.len() {
        let idx = indexes[idx_pos].clone();
        if idx.is_unique {
            let col_indices: Vec<usize> = idx
                .column_names
                .iter()
                .filter_map(|cn| table_def.column_index(cn))
                .collect();
            if col_indices.len() != idx.column_names.len() {
                continue;
            }
            let is_composite = idx.column_names.len() > 1;
            let encoded = encode_index_key_from_row(
                new_values,
                &col_indices,
                &table_def.columns,
                is_composite,
            );
            if let Some(idx_key) = encoded {
                let idx_btree = BTree::open(idx.btree_root);
                if let Some(existing_pk_key) = idx_btree.search(pager, &idx_key)? {
                    // Found a conflicting row via this unique index — delete it
                    if let Some(existing_data) = data_btree.search(pager, &existing_pk_key)? {
                        let existing_values = deserialize_row_versioned(
                            &existing_data,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        delete_from_secondary_indexes(
                            table_def,
                            indexes,
                            &existing_values,
                            &existing_pk_key,
                            pager,
                        )?;
                        data_btree.delete(pager, &existing_pk_key)?;
                        *data_btree = BTree::open(data_btree.root_page_id());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Insert values into secondary indexes.
/// For non-unique indexes, the B-tree key is `index_key + pk_key` so that
/// duplicate indexed values each get their own B-tree entry.
pub(super) fn insert_into_secondary_indexes(
    table_def: &TableDef,
    indexes: &mut [IndexDef],
    values: &[Value],
    pk_key: &[u8],
    pager: &mut impl PageStore,
) -> Result<()> {
    for idx in indexes.iter_mut() {
        if idx.index_type == IndexType::BTree {
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
                encode_index_key_from_row(values, &col_indices, &table_def.columns, is_composite);
            if let Some(idx_key) = encoded {
                let mut idx_btree = BTree::open(idx.btree_root);
                if idx.is_unique {
                    idx_btree.insert(pager, &idx_key, pk_key)?;
                } else {
                    // Append pk_key to make the B-tree key unique
                    let mut full_key = idx_key;
                    full_key.extend_from_slice(pk_key);
                    idx_btree.insert(pager, &full_key, pk_key)?;
                }
                idx.btree_root = idx_btree.root_page_id();
            }
        } else if idx.index_type == IndexType::Fulltext {
            let Some(col_name) = idx.column_names.first() else {
                continue;
            };
            let Some(col_idx) = table_def.column_index(col_name) else {
                continue;
            };
            let Some(text) = values.get(col_idx).and_then(value_to_fts_text) else {
                continue;
            };
            let mut root_page_id = idx.btree_root;
            let mut meta_btree = BTree::open(root_page_id);
            let doc_id = match fts_get_doc_id(&meta_btree, pager, pk_key)? {
                Some(id) => id,
                None => {
                    let id = fts_allocate_doc_id(&mut meta_btree, pager)?;
                    fts_put_doc_mapping(&mut meta_btree, pager, pk_key, id)?;
                    id
                }
            };
            root_page_id = meta_btree.root_page_id();
            let mut fts = FtsIndex::open(root_page_id, SQL_FTS_TERM_KEY);
            fts.apply_pending(pager, &[FtsPendingOp::Add { doc_id, text }])?;
            idx.btree_root = fts.root_page_id();
        }
    }
    Ok(())
}

/// Delete values from secondary indexes.
/// For non-unique indexes, the B-tree key is `index_key + pk_key`.
pub(super) fn delete_from_secondary_indexes(
    table_def: &TableDef,
    indexes: &mut [IndexDef],
    values: &[Value],
    pk_key: &[u8],
    pager: &mut impl PageStore,
) -> Result<()> {
    for idx in indexes.iter_mut() {
        if idx.index_type == IndexType::BTree {
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
                encode_index_key_from_row(values, &col_indices, &table_def.columns, is_composite);
            if let Some(idx_key) = encoded {
                let mut idx_btree = BTree::open(idx.btree_root);
                if idx.is_unique {
                    idx_btree.delete(pager, &idx_key)?;
                } else {
                    let mut full_key = idx_key;
                    full_key.extend_from_slice(pk_key);
                    idx_btree.delete(pager, &full_key)?;
                }
                idx.btree_root = idx_btree.root_page_id();
            }
        } else if idx.index_type == IndexType::Fulltext {
            let Some(col_name) = idx.column_names.first() else {
                continue;
            };
            let Some(col_idx) = table_def.column_index(col_name) else {
                continue;
            };
            let Some(text) = values.get(col_idx).and_then(value_to_fts_text) else {
                continue;
            };
            let mut root_page_id = idx.btree_root;
            let mut meta_btree = BTree::open(root_page_id);
            if let Some(doc_id) = fts_get_doc_id(&meta_btree, pager, pk_key)? {
                root_page_id = meta_btree.root_page_id();
                let mut fts = FtsIndex::open(root_page_id, SQL_FTS_TERM_KEY);
                fts.apply_pending(pager, &[FtsPendingOp::Remove { doc_id, text }])?;
                root_page_id = fts.root_page_id();
                meta_btree = BTree::open(root_page_id);
                fts_delete_doc_mapping(&mut meta_btree, pager, pk_key, doc_id)?;
                idx.btree_root = meta_btree.root_page_id();
            }
        }
    }
    Ok(())
}

pub(super) fn persist_indexes(
    catalog: &mut SystemCatalog,
    pager: &mut impl PageStore,
    indexes: &[IndexDef],
) -> Result<()> {
    for idx in indexes {
        catalog.update_index(pager, idx)?;
    }
    Ok(())
}
