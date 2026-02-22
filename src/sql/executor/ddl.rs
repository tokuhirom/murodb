use super::*;
use std::collections::{HashMap, HashSet};

pub(super) fn exec_create_table(
    ct: &CreateTable,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Check IF NOT EXISTS
    if ct.if_not_exists && catalog.get_table(pager, &ct.table_name)?.is_some() {
        return Ok(ExecResult::Ok);
    }

    let col_names: Vec<&str> = ct.columns.iter().map(|c| c.name.as_str()).collect();

    // --- Validate all constraints BEFORE creating any catalog entries ---

    let has_col_pk = ct.columns.iter().any(|c| c.is_primary_key);
    let mut table_level_pk: Option<Vec<String>> = None;
    let mut table_level_uniques: Vec<(String, Vec<String>)> = Vec::new();

    for constraint in &ct.constraints {
        match constraint {
            TableConstraint::PrimaryKey(cols) => {
                if has_col_pk {
                    return Err(MuroError::Schema(
                        "Cannot have both column-level and table-level PRIMARY KEY".into(),
                    ));
                }
                for col_name in cols {
                    if !col_names.contains(&col_name.as_str()) {
                        return Err(MuroError::Schema(format!(
                            "Column '{}' not found for PRIMARY KEY constraint",
                            col_name
                        )));
                    }
                }
                table_level_pk = Some(cols.clone());
            }
            TableConstraint::Unique(name, cols) => {
                for col_name in cols {
                    if !col_names.contains(&col_name.as_str()) {
                        return Err(MuroError::Schema(format!(
                            "Column '{}' not found for UNIQUE constraint",
                            col_name
                        )));
                    }
                }
                let idx_name = name
                    .clone()
                    .unwrap_or_else(|| format!("auto_unique_{}_{}", ct.table_name, cols.join("_")));
                if table_level_uniques.iter().any(|(n, _)| n == &idx_name) {
                    return Err(MuroError::Schema(format!(
                        "Duplicate UNIQUE constraint '{}'",
                        idx_name
                    )));
                }
                table_level_uniques.push((idx_name, cols.clone()));
            }
        }
    }

    // Collect column-level UNIQUE index names and detect duplicates with table-level
    let mut all_index_names: Vec<String> = table_level_uniques
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    for col_spec in &ct.columns {
        if col_spec.is_unique && !col_spec.is_primary_key {
            let idx_name = format!("auto_unique_{}_{}", ct.table_name, col_spec.name);
            if all_index_names.contains(&idx_name) {
                return Err(MuroError::Schema(format!(
                    "Duplicate UNIQUE constraint on column '{}'",
                    col_spec.name
                )));
            }
            all_index_names.push(idx_name);
        }
    }

    // --- Build column definitions ---

    let mut columns: Vec<ColumnDef> = ct
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

    // Apply table-level PK to columns before creating the table
    if let Some(ref pk_cols) = table_level_pk {
        for col_name in pk_cols {
            if let Some(col) = columns.iter_mut().find(|c| c.name == *col_name) {
                col.is_primary_key = true;
                col.is_nullable = false;
            }
        }
    }

    // --- Now create the table (all validation passed) ---

    let _table_def = catalog.create_table(pager, &ct.table_name, columns)?;

    // Apply table-level PK: update pk_columns and remove _rowid
    if let Some(pk_cols) = table_level_pk {
        let mut table_def = catalog.get_table(pager, &ct.table_name)?.unwrap();
        if let Some(rowid_idx) = table_def.column_index("_rowid") {
            if table_def.columns[rowid_idx].is_hidden {
                table_def.columns.remove(rowid_idx);
            }
        }
        table_def.pk_columns = pk_cols;
        catalog.update_table(pager, &table_def)?;
    }

    // Create table-level UNIQUE indexes
    for (idx_name, cols) in table_level_uniques {
        let idx_btree = BTree::create(pager)?;
        let idx_def = IndexDef {
            name: idx_name,
            table_name: ct.table_name.clone(),
            column_names: cols,
            index_type: IndexType::BTree,
            is_unique: true,
            btree_root: idx_btree.root_page_id(),
            stats_distinct_keys: 0,
            stats_num_min: 0,
            stats_num_max: 0,
            stats_num_bounds_known: false,
            stats_num_hist_bins: Vec::new(),
            fts_stop_filter: false,
            fts_stop_df_ratio_ppm: 0,
        };
        catalog.create_index(pager, idx_def)?;
    }

    // Create unique indexes for columns marked UNIQUE (non-PK)
    for col_spec in &ct.columns {
        if col_spec.is_unique && !col_spec.is_primary_key {
            let idx_btree = BTree::create(pager)?;
            let idx_def = IndexDef {
                name: format!("auto_unique_{}_{}", ct.table_name, col_spec.name),
                table_name: ct.table_name.clone(),
                column_names: vec![col_spec.name.clone()],
                index_type: IndexType::BTree,
                is_unique: true,
                btree_root: idx_btree.root_page_id(),
                stats_distinct_keys: 0,
                stats_num_min: 0,
                stats_num_max: 0,
                stats_num_bounds_known: false,
                stats_num_hist_bins: Vec::new(),
                fts_stop_filter: false,
                fts_stop_df_ratio_ppm: 0,
            };
            catalog.create_index(pager, idx_def)?;
        }
    }

    Ok(ExecResult::Ok)
}

/// Convert an AST expression (from DEFAULT clause) to a DefaultValue for storage.
pub(super) fn ast_expr_to_default(expr: &Expr) -> Option<DefaultValue> {
    match expr {
        Expr::IntLiteral(n) => Some(DefaultValue::Integer(*n)),
        Expr::FloatLiteral(n) => Some(DefaultValue::Float(*n)),
        Expr::StringLiteral(s) => Some(DefaultValue::String(s.clone())),
        Expr::Null => Some(DefaultValue::Null),
        _ => None,
    }
}

/// Convert an AST expression to a string representation for storage (CHECK constraints).
pub(super) fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::IntLiteral(n) => n.to_string(),
        Expr::FloatLiteral(n) => n.to_string(),
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

pub(super) fn exec_create_index(
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

    // Verify all columns exist
    let mut col_indices = Vec::new();
    for col_name in &ci.column_names {
        let col_idx = table_def.column_index(col_name).ok_or_else(|| {
            MuroError::Schema(format!(
                "Column '{}' not found in table '{}'",
                col_name, ci.table_name
            ))
        })?;
        col_indices.push(col_idx);
    }

    let is_composite = ci.column_names.len() > 1;

    let idx_btree = BTree::create(pager)?;

    // If unique, scan existing data for duplicates
    if ci.is_unique {
        let data_btree = BTree::open(table_def.data_btree_root);
        let mut seen_keys: Vec<Vec<u8>> = Vec::new();
        data_btree.scan(pager, |_k, v| {
            let row_values =
                deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
            let encoded = encode_index_key_from_row(
                &row_values,
                &col_indices,
                &table_def.columns,
                is_composite,
            );
            if let Some(key) = encoded {
                if seen_keys.contains(&key) {
                    return Err(MuroError::UniqueViolation(format!(
                        "Duplicate value in column(s) '{}'",
                        ci.column_names.join(", ")
                    )));
                }
                seen_keys.push(key);
            }
            Ok(true)
        })?;
    }

    // Collect existing data for index building
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    data_btree.scan(pager, |pk_key, v| {
        let row_values =
            deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
        let encoded =
            encode_index_key_from_row(&row_values, &col_indices, &table_def.columns, is_composite);
        if let Some(idx_key) = encoded {
            if ci.is_unique {
                entries.push((idx_key, pk_key.to_vec()));
            } else {
                // For non-unique indexes, append PK to make B-tree key unique
                let mut full_key = idx_key;
                full_key.extend_from_slice(pk_key);
                entries.push((full_key, pk_key.to_vec()));
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
        column_names: ci.column_names.clone(),
        index_type: IndexType::BTree,
        is_unique: ci.is_unique,
        btree_root: idx_btree_mut.root_page_id(),
        stats_distinct_keys: 0,
        stats_num_min: 0,
        stats_num_max: 0,
        stats_num_bounds_known: false,
        stats_num_hist_bins: Vec::new(),
        fts_stop_filter: false,
        fts_stop_df_ratio_ppm: 0,
    };
    catalog.create_index(pager, idx_def)?;

    Ok(ExecResult::Ok)
}

pub(super) fn exec_create_fulltext_index(
    fi: &CreateFulltextIndex,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, &fi.table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", fi.table_name)))?;

    if catalog.get_index(pager, &fi.index_name)?.is_some() {
        return Err(MuroError::Schema(format!(
            "Index '{}' already exists",
            fi.index_name
        )));
    }

    let col_idx = table_def.column_index(&fi.column_name).ok_or_else(|| {
        MuroError::Schema(format!(
            "Column '{}' not found in table '{}'",
            fi.column_name, fi.table_name
        ))
    })?;

    validate_fulltext_parser(fi)?;

    let col_ty = table_def.columns[col_idx].data_type;
    if !matches!(col_ty, DataType::Varchar(_) | DataType::Text) {
        return Err(MuroError::Schema(format!(
            "FULLTEXT index column '{}' must be VARCHAR or TEXT",
            fi.column_name
        )));
    }

    let mut fts_index = FtsIndex::create(pager, SQL_FTS_TERM_KEY)?;
    let fts_root = fts_index.root_page_id();

    let build_res: Result<()> = (|| {
        let data_btree = BTree::open(table_def.data_btree_root);
        let mut pending = Vec::new();
        let mut mappings = Vec::new();
        let mut next_doc_id = 1u64;

        data_btree.scan(pager, |pk_key, row| {
            let values =
                deserialize_row_versioned(row, &table_def.columns, table_def.row_format_version)?;
            let Some(text) = values.get(col_idx).and_then(value_to_fts_text) else {
                return Ok(true);
            };
            let doc_id = next_doc_id;
            next_doc_id = next_doc_id
                .checked_add(1)
                .ok_or_else(|| MuroError::Execution("FULLTEXT doc_id overflow".into()))?;
            pending.push(FtsPendingOp::Add { doc_id, text });
            mappings.push((pk_key.to_vec(), doc_id));
            Ok(true)
        })?;

        fts_index.apply_pending(pager, &pending)?;

        let mut meta_btree = BTree::open(fts_root);
        for (pk_key, doc_id) in &mappings {
            fts_put_doc_mapping(&mut meta_btree, pager, pk_key, *doc_id)?;
        }
        fts_set_next_doc_id(&mut meta_btree, pager, next_doc_id)?;
        Ok(())
    })();
    if let Err(e) = build_res {
        free_btree_pages(pager, fts_root);
        return Err(e);
    }

    let idx_def = IndexDef {
        name: fi.index_name.clone(),
        table_name: fi.table_name.clone(),
        column_names: vec![fi.column_name.clone()],
        index_type: IndexType::Fulltext,
        is_unique: false,
        btree_root: fts_root,
        stats_distinct_keys: 0,
        stats_num_min: 0,
        stats_num_max: 0,
        stats_num_bounds_known: false,
        stats_num_hist_bins: Vec::new(),
        fts_stop_filter: fi.stop_filter,
        fts_stop_df_ratio_ppm: fi.stop_df_ratio_ppm,
    };
    catalog.create_index(pager, idx_def)?;

    Ok(ExecResult::Ok)
}

pub(super) fn exec_analyze_table(
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    const NUM_HIST_BINS: usize = 16;
    let mut table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let data_btree = BTree::open(table_def.data_btree_root);
    let mut row_count: u64 = 0;
    data_btree.scan(pager, |_k, _v| {
        row_count += 1;
        Ok(true)
    })?;
    table_def.stats_row_count = row_count;
    catalog.update_table(pager, &table_def)?;

    let mut indexes = catalog.get_indexes_for_table(pager, table_name)?;
    let mut numeric_targets: HashMap<String, usize> = HashMap::new();
    for idx in &indexes {
        if idx.index_type != IndexType::BTree || idx.column_names.len() != 1 {
            continue;
        }
        let col_name = &idx.column_names[0];
        if let Some(col_idx) = table_def.column_index(col_name) {
            if matches!(
                table_def.columns[col_idx].data_type,
                DataType::TinyInt
                    | DataType::SmallInt
                    | DataType::Int
                    | DataType::BigInt
                    | DataType::Date
                    | DataType::DateTime
                    | DataType::Timestamp
            ) {
                numeric_targets.insert(idx.name.clone(), col_idx);
            }
        }
    }

    let mut numeric_bounds: HashMap<String, (i64, i64)> = HashMap::new();
    if !numeric_targets.is_empty() {
        let data_btree = BTree::open(table_def.data_btree_root);
        data_btree.scan(pager, |_k, row| {
            let values =
                deserialize_row_versioned(row, &table_def.columns, table_def.row_format_version)?;
            for (idx_name, col_idx) in &numeric_targets {
                if let Some(n) = values.get(*col_idx).and_then(value_as_i64_for_stats) {
                    numeric_bounds
                        .entry(idx_name.clone())
                        .and_modify(|(min_v, max_v)| {
                            if n < *min_v {
                                *min_v = n;
                            }
                            if n > *max_v {
                                *max_v = n;
                            }
                        })
                        .or_insert((n, n));
                }
            }
            Ok(true)
        })?;
    }
    let mut numeric_histograms: HashMap<String, Vec<u32>> = numeric_bounds
        .keys()
        .map(|k| (k.clone(), vec![0; NUM_HIST_BINS]))
        .collect();
    if !numeric_histograms.is_empty() {
        let data_btree = BTree::open(table_def.data_btree_root);
        data_btree.scan(pager, |_k, row| {
            let values =
                deserialize_row_versioned(row, &table_def.columns, table_def.row_format_version)?;
            for (idx_name, col_idx) in &numeric_targets {
                let Some((min_v, max_v)) = numeric_bounds.get(idx_name).copied() else {
                    continue;
                };
                let Some(n) = values.get(*col_idx).and_then(value_as_i64_for_stats) else {
                    continue;
                };
                if n < min_v || n > max_v {
                    continue;
                }
                let Some(bi) = numeric_hist_bin_index(n, min_v, max_v, NUM_HIST_BINS) else {
                    continue;
                };
                if let Some(bins) = numeric_histograms.get_mut(idx_name) {
                    if let Some(slot) = bins.get_mut(bi) {
                        *slot = slot.saturating_add(1);
                    }
                }
            }
            Ok(true)
        })?;
    }

    for idx in &mut indexes {
        if idx.index_type != IndexType::BTree {
            continue;
        }

        let idx_btree = BTree::open(idx.btree_root);
        let mut distinct_keys: u64 = 0;
        let mut seen_idx_parts: HashSet<Vec<u8>> = HashSet::new();
        idx_btree.scan(pager, |k, v| {
            if idx.is_unique {
                distinct_keys += 1;
                return Ok(true);
            }

            let idx_part = if k.len() >= v.len() {
                k[..k.len() - v.len()].to_vec()
            } else {
                return Err(MuroError::Corruption(
                    "invalid non-unique index entry: key shorter than value".into(),
                ));
            };
            if seen_idx_parts.insert(idx_part) {
                distinct_keys += 1;
            }
            Ok(true)
        })?;

        idx.stats_distinct_keys = distinct_keys;
        if let Some((min_v, max_v)) = numeric_bounds.get(&idx.name).copied() {
            idx.stats_num_bounds_known = true;
            idx.stats_num_min = min_v;
            idx.stats_num_max = max_v;
            idx.stats_num_hist_bins = numeric_histograms.remove(&idx.name).unwrap_or_default();
        } else {
            idx.stats_num_bounds_known = false;
            idx.stats_num_min = 0;
            idx.stats_num_max = 0;
            idx.stats_num_hist_bins.clear();
        }
        catalog.update_index(pager, idx)?;
    }

    Ok(ExecResult::Ok)
}

fn value_as_i64_for_stats(v: &Value) -> Option<i64> {
    match v {
        Value::Integer(n) => Some(*n),
        Value::Date(n) => Some(*n as i64),
        Value::DateTime(n) => Some(*n),
        Value::Timestamp(n) => Some(*n),
        _ => None,
    }
}

fn numeric_hist_bin_index(v: i64, min_v: i64, max_v: i64, bins: usize) -> Option<usize> {
    if bins == 0 || max_v < min_v || v < min_v || v > max_v {
        return None;
    }
    if max_v == min_v {
        return Some(0);
    }
    let span = (max_v as i128 - min_v as i128 + 1) as u128;
    let pos = (v as i128 - min_v as i128) as u128;
    let mut idx = (pos.saturating_mul(bins as u128) / span) as usize;
    if idx >= bins {
        idx = bins - 1;
    }
    Some(idx)
}

pub(super) fn exec_drop_table(
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

pub(super) fn exec_drop_index(
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
