use super::*;

pub(super) fn exec_select_without_table(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    if sel.table_name.is_some() {
        return Err(MuroError::Execution(
            "exec_select_without_table called with FROM clause".into(),
        ));
    }

    let has_subqueries = sel
        .where_clause
        .as_ref()
        .is_some_and(expr_contains_subquery)
        || select_columns_contain_subquery(&sel.columns)
        || sel.having.as_ref().is_some_and(expr_contains_subquery);

    if has_subqueries {
        let materialized = materialize_select_subqueries(sel, pager, catalog)?;
        return exec_select_without_table_inner(&materialized);
    }

    exec_select_without_table_inner(sel)
}

pub(super) fn exec_select_without_table_inner(sel: &Select) -> Result<ExecResult> {
    if sel.table_name.is_some() {
        return Err(MuroError::Execution(
            "exec_select_without_table_inner called with FROM clause".into(),
        ));
    }

    if sel.columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
        return Err(MuroError::Execution(
            "SELECT * requires a FROM clause".into(),
        ));
    }

    if !sel.joins.is_empty() {
        return Err(MuroError::Execution(
            "SELECT without FROM cannot include JOIN".into(),
        ));
    }

    let need_aggregation = has_aggregates(&sel.columns, &sel.having) || sel.group_by.is_some();

    let mut where_passed = true;
    if let Some(where_expr) = &sel.where_clause {
        let val = eval_expr(where_expr, &|_| None)?;
        where_passed = is_truthy(&val);
    }

    if !where_passed && !need_aggregation {
        return Ok(ExecResult::Rows(Vec::new()));
    }

    if need_aggregation {
        let table_def = TableDef {
            name: "<expr>".to_string(),
            columns: Vec::new(),
            pk_columns: Vec::new(),
            data_btree_root: PageId::default(),
            next_rowid: 0,
            row_format_version: 0,
            stats_row_count: 0,
        };
        let raw_rows = if where_passed {
            vec![vec![]]
        } else {
            Vec::new()
        };
        let mut rows = execute_aggregation(raw_rows, &table_def, sel)?;
        if let Some(order_items) = &sel.order_by {
            sort_rows(&mut rows, order_items);
        }
        if let Some(offset) = sel.offset {
            let offset = offset as usize;
            if offset >= rows.len() {
                rows.clear();
            } else {
                rows = rows.into_iter().skip(offset).collect();
            }
        }
        if let Some(limit) = sel.limit {
            rows.truncate(limit as usize);
        }
        return Ok(ExecResult::Rows(rows));
    }

    let mut rows = Vec::new();
    let mut row_values = Vec::new();
    for col in &sel.columns {
        if let SelectColumn::Expr(expr, alias) = col {
            let val = eval_expr(expr, &|_| None)?;
            let name = alias.clone().unwrap_or_else(|| match expr {
                Expr::ColumnRef(n) => n.clone(),
                _ => "?column?".to_string(),
            });
            row_values.push((name, val));
        }
    }
    rows.push(Row { values: row_values });

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

    if let Some(order_items) = &sel.order_by {
        sort_rows(&mut rows, order_items);
    }

    if let Some(offset) = sel.offset {
        let offset = offset as usize;
        if offset >= rows.len() {
            rows.clear();
        } else {
            rows = rows.into_iter().skip(offset).collect();
        }
    }

    if let Some(limit) = sel.limit {
        rows.truncate(limit as usize);
    }

    Ok(ExecResult::Rows(rows))
}

pub(super) fn exec_select(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    if sel.table_name.is_none() {
        return exec_select_without_table(sel, pager, catalog);
    }
    let table_name = sel.table_name.as_ref().unwrap();

    // Pre-materialize subqueries if any exist
    let has_subqueries = sel
        .where_clause
        .as_ref()
        .is_some_and(expr_contains_subquery)
        || select_columns_contain_subquery(&sel.columns)
        || sel.having.as_ref().is_some_and(expr_contains_subquery);

    if has_subqueries {
        let materialized = materialize_select_subqueries(sel, pager, catalog)?;
        return exec_select(&materialized, pager, catalog);
    }

    let table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    // If there are JOINs, use the join execution path
    if !sel.joins.is_empty() {
        return exec_select_join(sel, table_name, &table_def, pager, catalog);
    }

    let indexes = catalog.get_indexes_for_table(pager, table_name)?;
    let index_stats: Vec<IndexPlanStat> = indexes
        .iter()
        .filter(|idx| idx.index_type == IndexType::BTree)
        .map(|idx| IndexPlanStat {
            name: idx.name.clone(),
            column_names: idx.column_names.clone(),
            is_unique: idx.is_unique,
            stats_distinct_keys: idx.stats_distinct_keys,
        })
        .collect();

    let plan = plan_select(
        table_name,
        &table_def.pk_columns,
        &index_stats,
        &sel.where_clause,
        PlannerStats {
            table_rows: table_def.stats_row_count,
        },
    );

    let need_aggregation = has_aggregates(&sel.columns, &sel.having) || sel.group_by.is_some();
    let mut fts_ctx = build_fts_eval_context(
        &sel.columns,
        &sel.where_clause,
        &table_def.name,
        &indexes,
        pager,
    )?;
    let needs_fts_doc_ids = !fts_ctx.score_maps.is_empty();

    if need_aggregation {
        // Aggregation path: collect raw values first
        let mut raw_rows: Vec<Vec<Value>> = Vec::new();

        match plan {
            Plan::PkSeek { key_exprs, .. } => {
                let pk_key = eval_pk_seek_key(&table_def, &key_exprs)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if needs_fts_doc_ids {
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                    }
                    if matches_where_with_fts(
                        &sel.where_clause,
                        &table_def,
                        &values,
                        Some(&fts_ctx),
                    )? {
                        raw_rows.push(values);
                    }
                }
            }
            Plan::IndexSeek {
                index_name,
                column_names,
                key_exprs,
                ..
            } => {
                let idx_key = eval_index_seek_key(&table_def, &column_names, &key_exprs)?;
                let idx = indexes
                    .iter()
                    .find(|i| i.name == index_name)
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let pk_keys = index_seek_pk_keys(idx, &idx_key, pager)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                for pk_key in &pk_keys {
                    if let Some(data) = data_btree.search(pager, pk_key)? {
                        let values = deserialize_row_versioned(
                            &data,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if needs_fts_doc_ids {
                            populate_fts_row_doc_ids(
                                &mut fts_ctx,
                                pk_key,
                                &indexes,
                                &table_def.name,
                                pager,
                            )?;
                        }
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            raw_rows.push(values);
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
            } => {
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
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let pk_keys = index_seek_pk_keys_range(idx, lower_key, upper_key, pager)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                for pk_key in &pk_keys {
                    if let Some(data) = data_btree.search(pager, pk_key)? {
                        let values = deserialize_row_versioned(
                            &data,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if needs_fts_doc_ids {
                            populate_fts_row_doc_ids(
                                &mut fts_ctx,
                                pk_key,
                                &indexes,
                                &table_def.name,
                                pager,
                            )?;
                        }
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            raw_rows.push(values);
                        }
                    }
                }
            }
            Plan::FullScan { .. } => {
                let data_btree = BTree::open(table_def.data_btree_root);
                if needs_fts_doc_ids {
                    let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
                    data_btree.scan(pager, |pk_key, v| {
                        let values = deserialize_row_versioned(
                            v,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        entries.push((pk_key.to_vec(), values));
                        Ok(true)
                    })?;
                    for (pk_key, values) in entries {
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            raw_rows.push(values);
                        }
                    }
                } else {
                    data_btree.scan(pager, |_, v| {
                        let values = deserialize_row_versioned(
                            v,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            raw_rows.push(values);
                        }
                        Ok(true)
                    })?;
                }
            }
            Plan::FtsScan {
                column,
                query,
                mode,
                ..
            } => {
                let fts_rows =
                    execute_fts_scan_rows(&table_def, &indexes, &column, &query, mode, pager)?;
                for (_doc_id, values) in fts_rows {
                    if needs_fts_doc_ids {
                        let pk_key = encode_pk_key(&table_def, &values);
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                    }
                    if matches_where_with_fts(
                        &sel.where_clause,
                        &table_def,
                        &values,
                        Some(&fts_ctx),
                    )? {
                        raw_rows.push(values);
                    }
                }
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
            Plan::PkSeek { key_exprs, .. } => {
                let pk_key = eval_pk_seek_key(&table_def, &key_exprs)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                if let Some(data) = data_btree.search(pager, &pk_key)? {
                    let values = deserialize_row_versioned(
                        &data,
                        &table_def.columns,
                        table_def.row_format_version,
                    )?;
                    if needs_fts_doc_ids {
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                    }
                    if matches_where_with_fts(
                        &sel.where_clause,
                        &table_def,
                        &values,
                        Some(&fts_ctx),
                    )? {
                        let row =
                            build_row_with_fts(&table_def, &values, &sel.columns, Some(&fts_ctx))?;
                        rows.push(row);
                    }
                }
            }
            Plan::IndexSeek {
                index_name,
                column_names,
                key_exprs,
                ..
            } => {
                let idx_key = eval_index_seek_key(&table_def, &column_names, &key_exprs)?;
                let idx = indexes
                    .iter()
                    .find(|i| i.name == index_name)
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let pk_keys = index_seek_pk_keys(idx, &idx_key, pager)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                for pk_key in &pk_keys {
                    if let Some(data) = data_btree.search(pager, pk_key)? {
                        let values = deserialize_row_versioned(
                            &data,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if needs_fts_doc_ids {
                            populate_fts_row_doc_ids(
                                &mut fts_ctx,
                                pk_key,
                                &indexes,
                                &table_def.name,
                                pager,
                            )?;
                        }
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            let row = build_row_with_fts(
                                &table_def,
                                &values,
                                &sel.columns,
                                Some(&fts_ctx),
                            )?;
                            rows.push(row);
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
            } => {
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
                    .ok_or_else(|| {
                        MuroError::Execution(format!("Index '{}' not found", index_name))
                    })?;
                let pk_keys = index_seek_pk_keys_range(idx, lower_key, upper_key, pager)?;
                let data_btree = BTree::open(table_def.data_btree_root);
                for pk_key in &pk_keys {
                    if let Some(data) = data_btree.search(pager, pk_key)? {
                        let values = deserialize_row_versioned(
                            &data,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if needs_fts_doc_ids {
                            populate_fts_row_doc_ids(
                                &mut fts_ctx,
                                pk_key,
                                &indexes,
                                &table_def.name,
                                pager,
                            )?;
                        }
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            let row = build_row_with_fts(
                                &table_def,
                                &values,
                                &sel.columns,
                                Some(&fts_ctx),
                            )?;
                            rows.push(row);
                        }
                    }
                }
            }
            Plan::FullScan { .. } => {
                let data_btree = BTree::open(table_def.data_btree_root);
                if needs_fts_doc_ids {
                    let mut entries: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
                    data_btree.scan(pager, |pk_key, v| {
                        let values = deserialize_row_versioned(
                            v,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        entries.push((pk_key.to_vec(), values));
                        Ok(true)
                    })?;
                    for (pk_key, values) in entries {
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            let row = build_row_with_fts(
                                &table_def,
                                &values,
                                &sel.columns,
                                Some(&fts_ctx),
                            )?;
                            rows.push(row);
                        }
                    }
                } else {
                    data_btree.scan(pager, |_, v| {
                        let values = deserialize_row_versioned(
                            v,
                            &table_def.columns,
                            table_def.row_format_version,
                        )?;
                        if matches_where_with_fts(
                            &sel.where_clause,
                            &table_def,
                            &values,
                            Some(&fts_ctx),
                        )? {
                            let row = build_row_with_fts(
                                &table_def,
                                &values,
                                &sel.columns,
                                Some(&fts_ctx),
                            )?;
                            rows.push(row);
                        }
                        Ok(true)
                    })?;
                }
            }
            Plan::FtsScan {
                column,
                query,
                mode,
                ..
            } => {
                let fts_rows =
                    execute_fts_scan_rows(&table_def, &indexes, &column, &query, mode, pager)?;
                for (_doc_id, values) in fts_rows {
                    if needs_fts_doc_ids {
                        let pk_key = encode_pk_key(&table_def, &values);
                        populate_fts_row_doc_ids(
                            &mut fts_ctx,
                            &pk_key,
                            &indexes,
                            &table_def.name,
                            pager,
                        )?;
                    }
                    if matches_where_with_fts(
                        &sel.where_clause,
                        &table_def,
                        &values,
                        Some(&fts_ctx),
                    )? {
                        let row =
                            build_row_with_fts(&table_def, &values, &sel.columns, Some(&fts_ctx))?;
                        rows.push(row);
                    }
                }
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
pub(super) fn exec_select_returning_rows(
    sel: &Select,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<Vec<Row>> {
    match exec_select(sel, pager, catalog)? {
        ExecResult::Rows(rows) => Ok(rows),
        _ => Err(MuroError::Execution("Expected rows from SELECT".into())),
    }
}

pub(super) fn sort_rows(rows: &mut [Row], order_items: &[OrderByItem]) {
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

pub(super) fn matches_where(
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

pub(super) fn is_row_independent_expr(expr: &Expr) -> bool {
    match expr {
        Expr::ColumnRef(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            is_row_independent_expr(left) && is_row_independent_expr(right)
        }
        Expr::UnaryOp { operand, .. } => is_row_independent_expr(operand),
        Expr::Like { expr, pattern, .. } => {
            is_row_independent_expr(expr) && is_row_independent_expr(pattern)
        }
        Expr::InList { expr, list, .. } => {
            is_row_independent_expr(expr) && list.iter().all(is_row_independent_expr)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            is_row_independent_expr(expr)
                && is_row_independent_expr(low)
                && is_row_independent_expr(high)
        }
        Expr::IsNull { expr, .. } => is_row_independent_expr(expr),
        Expr::FunctionCall { args, .. } => args.iter().all(is_row_independent_expr),
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| is_row_independent_expr(e))
                .unwrap_or(true)
                && when_clauses.iter().all(|(cond, value)| {
                    is_row_independent_expr(cond) && is_row_independent_expr(value)
                })
                && else_clause
                    .as_ref()
                    .map(|e| is_row_independent_expr(e))
                    .unwrap_or(true)
        }
        Expr::Cast { expr, .. } => is_row_independent_expr(expr),
        Expr::AggregateFunc { arg, .. } => arg
            .as_ref()
            .map(|e| is_row_independent_expr(e))
            .unwrap_or(true),
        Expr::GreaterThanZero(expr) => is_row_independent_expr(expr),
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => false,
        Expr::MatchAgainst { .. } | Expr::FtsSnippet { .. } => false,
        Expr::IntLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BlobLiteral(_)
        | Expr::Null
        | Expr::DefaultValue => true,
    }
}

pub(super) fn matches_where_with_fts(
    where_clause: &Option<Expr>,
    table_def: &TableDef,
    values: &[Value],
    fts_ctx: Option<&FtsEvalContext>,
) -> Result<bool> {
    match where_clause {
        None => Ok(true),
        Some(expr) => {
            let expr = materialize_fts_expr(expr, table_def, values, fts_ctx);
            let result = eval_expr(&expr, &|name| {
                table_def
                    .column_index(name)
                    .and_then(|i| values.get(i).cloned())
            })?;
            Ok(is_truthy(&result))
        }
    }
}

pub(super) fn build_row_with_fts(
    table_def: &TableDef,
    values: &[Value],
    select_columns: &[SelectColumn],
    fts_ctx: Option<&FtsEvalContext>,
) -> Result<Row> {
    let mut row_values = Vec::with_capacity(select_columns.len().max(table_def.columns.len()));

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
                let expr = materialize_fts_expr(expr, table_def, values, fts_ctx);
                let val = eval_expr(&expr, &|name| {
                    table_def
                        .column_index(name)
                        .and_then(|i| values.get(i).cloned())
                })?;
                let name = alias.clone().unwrap_or_else(|| match expr {
                    Expr::ColumnRef(ref n) => n.clone(),
                    _ => "?column?".to_string(),
                });
                row_values.push((name, val));
            }
        }
    }

    Ok(Row { values: row_values })
}
