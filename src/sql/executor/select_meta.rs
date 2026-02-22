use super::*;

pub(super) fn select_col_count(sel: &Select) -> Option<usize> {
    // Star expands to all table columns, so we can't determine the count statically
    if sel.columns.iter().any(|c| matches!(c, SelectColumn::Star)) {
        None
    } else {
        Some(sel.columns.len())
    }
}

pub(super) fn exec_explain(
    stmt: &Statement,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let (table_name, where_clause, select_type) = match stmt {
        Statement::Select(sel) => {
            let table_name = sel.table_name.as_ref().ok_or_else(|| {
                MuroError::Execution("EXPLAIN requires SELECT to have a FROM clause".into())
            })?;
            (table_name.clone(), &sel.where_clause, "SIMPLE")
        }
        Statement::Update(upd) => (upd.table_name.clone(), &upd.where_clause, "UPDATE"),
        Statement::Delete(del) => (del.table_name.clone(), &del.where_clause, "DELETE"),
        _ => {
            return Err(MuroError::Execution(
                "EXPLAIN supports SELECT, UPDATE, and DELETE statements".into(),
            ));
        }
    };

    let table_def = catalog
        .get_table(pager, &table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let indexes = catalog.get_indexes_for_table(pager, &table_name)?;
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
    let planner_stats = PlannerStats {
        table_rows: table_def.stats_row_count,
    };

    let plan = plan_select(
        &table_name,
        &table_def.pk_columns,
        &index_stats,
        where_clause,
        planner_stats,
    );
    // Keep EXPLAIN row cardinality informative even before ANALYZE TABLE
    // by falling back to observed table rows for display only.
    let display_stats = PlannerStats {
        table_rows: estimate_table_rows(&table_def, pager)?,
    };
    let estimated_rows = estimate_plan_rows_hint(&plan, &display_stats, &index_stats);
    let estimated_cost = plan_cost_hint_with_stats(&plan, &planner_stats, &index_stats) as i64;

    let (access_type, key_name, extra) = match &plan {
        Plan::PkSeek { .. } => ("const", "PRIMARY".to_string(), "Using where".to_string()),
        Plan::IndexSeek { index_name, .. } => (
            "ref",
            index_name.clone(),
            "Using where; Using index".to_string(),
        ),
        Plan::IndexRangeSeek { index_name, .. } => (
            "range",
            index_name.clone(),
            "Using where; Using index".to_string(),
        ),
        Plan::FullScan { .. } => {
            let extra = if where_clause.is_some() {
                "Using where"
            } else {
                ""
            };
            ("ALL", String::new(), extra.to_string())
        }
        Plan::FtsScan { column, .. } => {
            let key_name = indexes
                .iter()
                .find(|idx| {
                    idx.index_type == IndexType::Fulltext
                        && idx.column_names.first().map(|c| c.as_str()) == Some(column.as_str())
                })
                .map(|idx| idx.name.clone())
                .unwrap_or_else(|| format!("fts_{}", column));
            (
                "fulltext",
                key_name,
                "Using where; Using fulltext".to_string(),
            )
        }
    };

    let row = Row {
        values: vec![
            ("id".to_string(), Value::Integer(1)),
            (
                "select_type".to_string(),
                Value::Varchar(select_type.to_string()),
            ),
            ("table".to_string(), Value::Varchar(table_name)),
            ("type".to_string(), Value::Varchar(access_type.to_string())),
            (
                "key".to_string(),
                if key_name.is_empty() {
                    Value::Null
                } else {
                    Value::Varchar(key_name)
                },
            ),
            ("rows".to_string(), Value::Integer(estimated_rows as i64)),
            ("cost".to_string(), Value::Integer(estimated_cost)),
            (
                "Extra".to_string(),
                if extra.is_empty() {
                    Value::Null
                } else {
                    Value::Varchar(extra)
                },
            ),
        ],
    };

    Ok(ExecResult::Rows(vec![row]))
}

fn estimate_table_rows(table_def: &TableDef, pager: &mut impl PageStore) -> Result<u64> {
    if table_def.stats_row_count > 0 {
        return Ok(table_def.stats_row_count);
    }
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut cnt: u64 = 0;
    data_btree.scan(pager, |_k, _v| {
        cnt += 1;
        Ok(true)
    })?;
    Ok(cnt.max(1))
}

pub(super) fn exec_set_query(
    sq: &SetQuery,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    // Determine expected column count from AST when possible
    let mut expected_col_count: Option<usize> = select_col_count(&sq.left);

    // Execute the first SELECT
    let first_result = exec_select_returning_rows(&sq.left, pager, catalog)?;

    // If we couldn't determine from AST (e.g. SELECT *), use result rows
    if expected_col_count.is_none() {
        if let Some(first_row) = first_result.first() {
            expected_col_count = Some(first_row.values.len());
        }
    }

    let col_names: Vec<String> = first_result
        .first()
        .map(|r| r.values.iter().map(|(n, _)| n.clone()).collect())
        .unwrap_or_default();

    let mut rows = first_result;

    for (op, sel) in &sq.ops {
        // Check column count from AST before executing
        if let (Some(expected), Some(actual)) = (expected_col_count, select_col_count(sel)) {
            if actual != expected {
                return Err(MuroError::Execution(format!(
                    "UNION queries must have the same number of columns (expected {}, got {})",
                    expected, actual
                )));
            }
        }

        let sel_rows = exec_select_returning_rows(sel, pager, catalog)?;

        // Validate column count from result rows (handles SELECT * cases)
        if let Some(first_row) = sel_rows.first() {
            match expected_col_count {
                Some(expected) if first_row.values.len() != expected => {
                    return Err(MuroError::Execution(format!(
                        "UNION queries must have the same number of columns (expected {}, got {})",
                        expected,
                        first_row.values.len()
                    )));
                }
                None => {
                    expected_col_count = Some(first_row.values.len());
                }
                _ => {}
            }
        }

        // Unify column names to match the first SELECT
        for mut row in sel_rows {
            if !col_names.is_empty() {
                for (i, (name, _)) in row.values.iter_mut().enumerate() {
                    if i < col_names.len() {
                        *name = col_names[i].clone();
                    }
                }
            }
            rows.push(row);
        }

        // UNION (without ALL) removes duplicates
        if *op == SetOp::Union {
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
    }

    // Apply ORDER BY
    if let Some(order_items) = &sq.order_by {
        sort_rows(&mut rows, order_items);
    }

    // Apply OFFSET
    if let Some(offset) = sq.offset {
        let offset = offset as usize;
        if offset >= rows.len() {
            rows.clear();
        } else {
            rows = rows.into_iter().skip(offset).collect();
        }
    }

    // Apply LIMIT
    if let Some(limit) = sq.limit {
        rows.truncate(limit as usize);
    }

    Ok(ExecResult::Rows(rows))
}
