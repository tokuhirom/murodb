use super::*;

pub(super) fn exec_rename_table(
    rt: &RenameTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    catalog.rename_table(pager, &rt.old_name, &rt.new_name)?;
    Ok(ExecResult::Ok)
}

/// Upgrade a table from row_format_version 0 to 1 and persist to catalog.
/// This must be called before any write to a v0 table, because serialize_row
/// always writes v1 format (with u16 column-count prefix).
pub(super) fn ensure_row_format_v1(
    table_def: &mut TableDef,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<()> {
    if table_def.row_format_version >= 1 {
        return Ok(());
    }

    // Rewrite all existing rows from v0 format to v1 format
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    data_btree.scan(pager, |k, v| {
        let values = deserialize_row_versioned(v, &table_def.columns, 0)?;
        entries.push((k.to_vec(), values));
        Ok(true)
    })?;

    if !entries.is_empty() {
        let mut data_btree = BTree::open(table_def.data_btree_root);
        for (pk_key, values) in &entries {
            let row_data = serialize_row(values, &table_def.columns);
            data_btree.insert(pager, pk_key, &row_data)?;
        }
    }

    table_def.row_format_version = 1;
    catalog.update_table(pager, table_def)?;
    Ok(())
}
