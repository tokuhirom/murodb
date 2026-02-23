use super::*;
use std::collections::HashSet;

#[derive(Clone)]
struct ChildMatch {
    pk_key: Vec<u8>,
    row_values: Vec<Value>,
}

#[derive(Clone)]
struct PendingDeleteAction {
    child_table_name: String,
    fk: ForeignKeyDef,
    parent_key: Vec<Value>,
}

#[derive(Clone)]
struct PendingUpdateAction {
    child_table_name: String,
    fk: ForeignKeyDef,
    old_parent: Vec<Value>,
    new_parent: Vec<Value>,
}

pub(super) fn enforce_child_foreign_keys(
    table_def: &TableDef,
    row_values: &[Value],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    for fk in &table_def.foreign_keys {
        let child_values = fk_child_values(table_def, fk, row_values)?;
        if child_values.iter().any(Value::is_null) {
            continue;
        }

        let parent_def = catalog.get_table(pager, &fk.ref_table)?.ok_or_else(|| {
            MuroError::Schema(format!("Referenced table '{}' not found", fk.ref_table))
        })?;
        let parent_btree = BTree::open(parent_def.data_btree_root);
        let mut exists = false;
        parent_btree.scan(pager, |_k, row| {
            let parent_row =
                deserialize_row_versioned(row, &parent_def.columns, parent_def.row_format_version)?;
            if fk_parent_matches_row(&parent_def, fk, &parent_row, &child_values)? {
                exists = true;
                return Ok(false);
            }
            Ok(true)
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

pub(super) fn enforce_parent_restrict_on_delete(
    parent_table: &TableDef,
    rows_to_delete: &[Vec<Value>],
    deleting_pk_keys: &[Vec<u8>],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    let mut visited = HashSet::new();
    enforce_parent_restrict_on_delete_inner(
        parent_table,
        rows_to_delete,
        deleting_pk_keys,
        pager,
        catalog,
        &mut visited,
    )
}

fn enforce_parent_restrict_on_delete_inner(
    parent_table: &TableDef,
    rows_to_delete: &[Vec<Value>],
    deleting_pk_keys: &[Vec<u8>],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    visited: &mut HashSet<(String, Vec<u8>)>,
) -> Result<()> {
    if rows_to_delete.is_empty() {
        return Ok(());
    }
    if rows_to_delete.len() != deleting_pk_keys.len() {
        return Err(MuroError::Execution(
            "internal error: delete rows/key length mismatch".into(),
        ));
    }

    let incoming = incoming_foreign_keys(&parent_table.name, pager, catalog)?;
    if incoming.is_empty() {
        return Ok(());
    }

    let deleting_pk_set: HashSet<Vec<u8>> = deleting_pk_keys.iter().cloned().collect();
    let mut pending_all = Vec::new();
    for (parent_row, parent_pk) in rows_to_delete.iter().zip(deleting_pk_keys.iter()) {
        // Break cascade loops on cyclic FK graphs.
        if !visited.insert((parent_table.name.clone(), parent_pk.clone())) {
            continue;
        }
        for (child_table_name, fk) in &incoming {
            let parent_key = fk_parent_values_from_row(parent_table, fk, parent_row)?;
            let Some(child_table) = catalog.get_table(pager, child_table_name)? else {
                continue;
            };
            let ignored_pk_set = if child_table.name == parent_table.name {
                Some(&deleting_pk_set)
            } else {
                None
            };
            let matches =
                find_child_references(&child_table, fk, &parent_key, None, ignored_pk_set, pager)?;
            if matches.is_empty() {
                continue;
            }

            if fk.on_delete == crate::schema::catalog::ForeignKeyAction::Restrict {
                return Err(MuroError::Execution(format!(
                    "Cannot delete parent row: referenced by {} via FOREIGN KEY ({})",
                    child_table.name,
                    fk.columns.join(", "),
                )));
            }
            pending_all.push(PendingDeleteAction {
                child_table_name: child_table_name.clone(),
                fk: fk.clone(),
                parent_key,
            });
        }
    }

    for action in pending_all {
        let Some(mut child_table) = catalog.get_table(pager, &action.child_table_name)? else {
            continue;
        };
        let ignored_pk_set = if child_table.name == parent_table.name {
            Some(&deleting_pk_set)
        } else {
            None
        };
        let matches = find_child_references(
            &child_table,
            &action.fk,
            &action.parent_key,
            None,
            ignored_pk_set,
            pager,
        )?;
        if matches.is_empty() {
            continue;
        }
        match action.fk.on_delete {
            crate::schema::catalog::ForeignKeyAction::Cascade => {
                cascade_delete_child_rows(&mut child_table, &matches, pager, catalog, visited)?;
            }
            crate::schema::catalog::ForeignKeyAction::SetNull => {
                set_null_child_references(
                    &mut child_table,
                    &action.fk,
                    &matches,
                    pager,
                    catalog,
                    visited,
                )?;
            }
            crate::schema::catalog::ForeignKeyAction::Restrict => {}
        }
    }

    Ok(())
}

pub(super) fn enforce_parent_restrict_on_update(
    parent_table: &TableDef,
    current_pk_key: &[u8],
    old_values: &[Value],
    new_values: &[Value],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    let mut visited = HashSet::new();
    enforce_parent_restrict_on_update_inner(
        parent_table,
        current_pk_key,
        old_values,
        new_values,
        pager,
        catalog,
        &mut visited,
    )
}

fn enforce_parent_restrict_on_update_inner(
    parent_table: &TableDef,
    current_pk_key: &[u8],
    old_values: &[Value],
    new_values: &[Value],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    visited: &mut HashSet<(String, Vec<u8>)>,
) -> Result<()> {
    if !visited.insert((parent_table.name.clone(), current_pk_key.to_vec())) {
        return Ok(());
    }

    let incoming = incoming_foreign_keys(&parent_table.name, pager, catalog)?;
    if incoming.is_empty() {
        return Ok(());
    }

    let mut pending = Vec::new();
    for (child_table_name, fk) in &incoming {
        let old_parent = fk_parent_values_from_row(parent_table, fk, old_values)?;
        let new_parent = fk_parent_values_from_row(parent_table, fk, new_values)?;
        if old_parent == new_parent {
            continue;
        }

        let self_override = if child_table_name == &parent_table.name {
            Some((current_pk_key, new_values))
        } else {
            None
        };

        let Some(child_table) = catalog.get_table(pager, child_table_name)? else {
            continue;
        };
        let matches =
            find_child_references(&child_table, fk, &old_parent, self_override, None, pager)?;
        if matches.is_empty() {
            continue;
        }

        if fk.on_update == crate::schema::catalog::ForeignKeyAction::Restrict {
            return Err(MuroError::Execution(format!(
                "Cannot update parent key: referenced by {} via FOREIGN KEY ({})",
                child_table.name,
                fk.columns.join(", "),
            )));
        }
        pending.push(PendingUpdateAction {
            child_table_name: child_table_name.clone(),
            fk: fk.clone(),
            old_parent,
            new_parent,
        });
    }

    for action in pending {
        let self_override = if action.child_table_name == parent_table.name {
            Some((current_pk_key, new_values))
        } else {
            None
        };
        let Some(mut child_table) = catalog.get_table(pager, &action.child_table_name)? else {
            continue;
        };
        let matches = find_child_references(
            &child_table,
            &action.fk,
            &action.old_parent,
            self_override,
            None,
            pager,
        )?;
        if matches.is_empty() {
            continue;
        }
        match action.fk.on_update {
            crate::schema::catalog::ForeignKeyAction::Cascade => {
                cascade_update_child_references(
                    &mut child_table,
                    &action.fk,
                    &matches,
                    &action.new_parent,
                    pager,
                    catalog,
                    visited,
                )?;
            }
            crate::schema::catalog::ForeignKeyAction::SetNull => {
                set_null_child_references(
                    &mut child_table,
                    &action.fk,
                    &matches,
                    pager,
                    catalog,
                    visited,
                )?;
            }
            crate::schema::catalog::ForeignKeyAction::Restrict => {}
        }
    }

    Ok(())
}

fn incoming_foreign_keys(
    parent_table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<Vec<(String, ForeignKeyDef)>> {
    let mut incoming = Vec::new();
    for table_name in catalog.list_tables(pager)? {
        let Some(child_table) = catalog.get_table(pager, &table_name)? else {
            continue;
        };
        for fk in &child_table.foreign_keys {
            if fk.ref_table == parent_table_name {
                incoming.push((child_table.name.clone(), fk.clone()));
            }
        }
    }
    Ok(incoming)
}

fn find_child_references(
    child_table: &TableDef,
    fk: &ForeignKeyDef,
    parent_values: &[Value],
    self_override: Option<(&[u8], &[Value])>,
    ignored_pk_set: Option<&HashSet<Vec<u8>>>,
    pager: &mut impl PageStore,
) -> Result<Vec<ChildMatch>> {
    let data_btree = BTree::open(child_table.data_btree_root);
    let mut matches = Vec::new();
    data_btree.scan(pager, |k, row| {
        if ignored_pk_set.is_some_and(|set| set.contains(k)) {
            return Ok(true);
        }
        let row_values = if let Some((override_pk, override_values)) = self_override {
            if k == override_pk {
                override_values.to_vec()
            } else {
                deserialize_row_versioned(
                    row,
                    &child_table.columns,
                    child_table.row_format_version,
                )?
            }
        } else {
            deserialize_row_versioned(row, &child_table.columns, child_table.row_format_version)?
        };

        let child_values = fk_child_values(child_table, fk, &row_values)?;
        if child_values.iter().any(Value::is_null) {
            return Ok(true);
        }
        if child_values == parent_values {
            matches.push(ChildMatch {
                pk_key: k.to_vec(),
                row_values,
            });
        }
        Ok(true)
    })?;
    Ok(matches)
}

fn cascade_delete_child_rows(
    child_table: &mut TableDef,
    matches: &[ChildMatch],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    visited: &mut HashSet<(String, Vec<u8>)>,
) -> Result<()> {
    let rows_to_delete: Vec<Vec<Value>> = matches.iter().map(|m| m.row_values.clone()).collect();
    let deleting_pk_keys: Vec<Vec<u8>> = matches.iter().map(|m| m.pk_key.clone()).collect();
    // Apply parent-side FK behavior for rows that will be deleted by CASCADE as well.
    enforce_parent_restrict_on_delete_inner(
        child_table,
        &rows_to_delete,
        &deleting_pk_keys,
        pager,
        catalog,
        visited,
    )?;

    let mut indexes = catalog.get_indexes_for_table(pager, &child_table.name)?;
    let mut data_btree = BTree::open(child_table.data_btree_root);

    for m in matches {
        delete_from_secondary_indexes(child_table, &mut indexes, &m.row_values, &m.pk_key, pager)?;
        data_btree.delete(pager, &m.pk_key)?;
    }

    child_table.data_btree_root = data_btree.root_page_id();
    catalog.update_table(pager, child_table)?;
    persist_indexes(catalog, pager, &indexes)?;
    Ok(())
}

fn set_null_child_references(
    child_table: &mut TableDef,
    fk: &ForeignKeyDef,
    matches: &[ChildMatch],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    visited: &mut HashSet<(String, Vec<u8>)>,
) -> Result<()> {
    let fk_indices = fk_child_column_indices(child_table, fk)?;
    for idx in &fk_indices {
        if !child_table.columns[*idx].is_nullable {
            return Err(MuroError::Execution(format!(
                "Cannot SET NULL on non-nullable child column '{}.{}'",
                child_table.name, child_table.columns[*idx].name
            )));
        }
    }

    update_child_rows(
        child_table,
        matches,
        pager,
        catalog,
        None,
        visited,
        |new_values| {
            for idx in &fk_indices {
                new_values[*idx] = Value::Null;
            }
        },
    )
}

fn cascade_update_child_references(
    child_table: &mut TableDef,
    fk: &ForeignKeyDef,
    matches: &[ChildMatch],
    new_parent_values: &[Value],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    visited: &mut HashSet<(String, Vec<u8>)>,
) -> Result<()> {
    let fk_indices = fk_child_column_indices(child_table, fk)?;
    update_child_rows(
        child_table,
        matches,
        pager,
        catalog,
        Some(fk),
        visited,
        |new_values| {
            for (i, idx) in fk_indices.iter().enumerate() {
                new_values[*idx] = new_parent_values[i].clone();
            }
        },
    )
}

fn update_child_rows<F>(
    child_table: &mut TableDef,
    matches: &[ChildMatch],
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
    skip_fk_validation: Option<&ForeignKeyDef>,
    visited: &mut HashSet<(String, Vec<u8>)>,
    mut apply_update: F,
) -> Result<()>
where
    F: FnMut(&mut Vec<Value>),
{
    ensure_row_format_v1(child_table, pager, catalog)?;

    let mut indexes = catalog.get_indexes_for_table(pager, &child_table.name)?;
    let mut data_btree = BTree::open(child_table.data_btree_root);
    let mut seen_new_pk_keys: HashSet<Vec<u8>> = HashSet::new();

    for m in matches {
        let mut new_values = m.row_values.clone();
        apply_update(&mut new_values);
        let new_pk_key = encode_pk_key(child_table, &new_values);
        if !seen_new_pk_keys.insert(new_pk_key.clone()) {
            return Err(MuroError::UniqueViolation(
                "Duplicate primary key".to_string(),
            ));
        }
        if new_pk_key != m.pk_key && data_btree.search(pager, &new_pk_key)?.is_some() {
            return Err(MuroError::UniqueViolation(
                "Duplicate primary key".to_string(),
            ));
        }

        check_unique_index_constraints_excluding(
            child_table,
            &indexes,
            &new_values,
            &m.pk_key,
            pager,
        )?;
        enforce_child_foreign_keys_with_skip(
            child_table,
            &new_values,
            skip_fk_validation,
            pager,
            catalog,
        )?;
        enforce_parent_restrict_on_update_inner(
            child_table,
            &m.pk_key,
            &m.row_values,
            &new_values,
            pager,
            catalog,
            visited,
        )?;

        delete_from_secondary_indexes(child_table, &mut indexes, &m.row_values, &m.pk_key, pager)?;
        insert_into_secondary_indexes(child_table, &mut indexes, &new_values, &new_pk_key, pager)?;

        let row_data = serialize_row(&new_values, &child_table.columns);
        if new_pk_key != m.pk_key {
            data_btree.delete(pager, &m.pk_key)?;
        }
        data_btree.insert(pager, &new_pk_key, &row_data)?;
    }

    child_table.data_btree_root = data_btree.root_page_id();
    catalog.update_table(pager, child_table)?;
    persist_indexes(catalog, pager, &indexes)?;
    Ok(())
}

fn enforce_child_foreign_keys_with_skip(
    table_def: &TableDef,
    row_values: &[Value],
    skip_fk: Option<&ForeignKeyDef>,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    for fk in &table_def.foreign_keys {
        if skip_fk.is_some_and(|s| {
            s.columns == fk.columns
                && s.ref_table == fk.ref_table
                && s.ref_columns == fk.ref_columns
        }) {
            continue;
        }
        let child_values = fk_child_values(table_def, fk, row_values)?;
        if child_values.iter().any(Value::is_null) {
            continue;
        }

        let parent_def = catalog.get_table(pager, &fk.ref_table)?.ok_or_else(|| {
            MuroError::Schema(format!("Referenced table '{}' not found", fk.ref_table))
        })?;
        let parent_btree = BTree::open(parent_def.data_btree_root);
        let mut exists = false;
        parent_btree.scan(pager, |_k, row| {
            let parent_row =
                deserialize_row_versioned(row, &parent_def.columns, parent_def.row_format_version)?;
            if fk_parent_matches_row(&parent_def, fk, &parent_row, &child_values)? {
                exists = true;
                return Ok(false);
            }
            Ok(true)
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

fn fk_child_column_indices(table_def: &TableDef, fk: &ForeignKeyDef) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(fk.columns.len());
    for col in &fk.columns {
        let idx = table_def.column_index(col).ok_or_else(|| {
            MuroError::Schema(format!("Column '{}.{}' not found", table_def.name, col))
        })?;
        indices.push(idx);
    }
    Ok(indices)
}

fn fk_child_values(
    table_def: &TableDef,
    fk: &ForeignKeyDef,
    row_values: &[Value],
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(fk.columns.len());
    for col in &fk.columns {
        let idx = table_def.column_index(col).ok_or_else(|| {
            MuroError::Schema(format!("Column '{}.{}' not found", table_def.name, col))
        })?;
        values.push(row_values[idx].clone());
    }
    Ok(values)
}

fn fk_parent_values_from_row(
    table_def: &TableDef,
    fk: &ForeignKeyDef,
    row_values: &[Value],
) -> Result<Vec<Value>> {
    let mut values = Vec::with_capacity(fk.ref_columns.len());
    for col in &fk.ref_columns {
        let idx = table_def.column_index(col).ok_or_else(|| {
            MuroError::Schema(format!("Column '{}.{}' not found", table_def.name, col))
        })?;
        values.push(row_values[idx].clone());
    }
    Ok(values)
}

fn fk_parent_matches_row(
    parent_table: &TableDef,
    fk: &ForeignKeyDef,
    parent_row: &[Value],
    child_values: &[Value],
) -> Result<bool> {
    let parent_values = fk_parent_values_from_row(parent_table, fk, parent_row)?;
    Ok(parent_values == child_values)
}
