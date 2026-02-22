use super::*;

pub(super) fn scan_table_qualified(
    table_name: &str,
    alias: Option<&str>,
    table_def: &TableDef,
    pager: &mut impl PageStore,
) -> Result<Vec<Vec<(String, Value)>>> {
    let qualifier = alias.unwrap_or(table_name);
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut result = Vec::new();
    data_btree.scan(pager, |_k, v| {
        let values =
            deserialize_row_versioned(v, &table_def.columns, table_def.row_format_version)?;
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

/// Make a null row for LEFT/RIGHT JOIN when there's no match on the other side.
pub(super) fn null_row_qualified(qualifier: &str, table_def: &TableDef) -> Vec<(String, Value)> {
    table_def
        .columns
        .iter()
        .map(|col| (format!("{}.{}", qualifier, col.name), Value::Null))
        .collect()
}

/// Resolve a column name against a joined row.
/// Supports "table.column" qualified names and unqualified "column" names.
pub(super) fn resolve_join_column<'a>(
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
pub(super) fn eval_join_expr(expr: &Expr, row: &[(String, Value)]) -> Result<Value> {
    eval_expr(expr, &|name| {
        resolve_join_column(name, row).ok().flatten().cloned()
    })
}

pub(super) fn exec_select_join(
    sel: &Select,
    base_table_name: &str,
    base_table_def: &TableDef,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Collect hidden qualified column names for Star expansion filtering
    let base_qualifier = sel.table_alias.as_deref().unwrap_or(base_table_name);
    let mut hidden_columns: Vec<String> = base_table_def
        .columns
        .iter()
        .filter(|c| c.is_hidden)
        .map(|c| format!("{}.{}", base_qualifier, c.name))
        .collect();

    // 1. Scan the base (FROM) table
    let mut joined_rows = scan_table_qualified(
        base_table_name,
        sel.table_alias.as_deref(),
        base_table_def,
        pager,
    )?;

    // Track accumulated left-side column qualifiers and table defs for RIGHT JOIN null generation
    let mut left_qualifiers_and_defs: Vec<(String, TableDef)> =
        vec![(base_qualifier.to_string(), base_table_def.clone())];

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
            JoinType::Right => {
                // Build a null row for the left side columns from accumulated table defs
                let null_left: Vec<(String, Value)> = left_qualifiers_and_defs
                    .iter()
                    .flat_map(|(q, td)| null_row_qualified(q, td))
                    .collect();

                for right in &right_rows {
                    let mut matched = false;
                    for left in &joined_rows {
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
                        let mut combined: Vec<(String, Value)> = null_left.clone();
                        combined.extend(right.iter().cloned());
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

        left_qualifiers_and_defs.push((right_qualifier.to_string(), right_table_def));
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

pub(super) fn build_join_row(
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
