use super::*;

pub(super) fn has_aggregates(columns: &[SelectColumn], having: &Option<Expr>) -> bool {
    for col in columns {
        if let SelectColumn::Expr(expr, _) = col {
            if expr_contains_aggregate(expr) {
                return true;
            }
        }
    }
    if let Some(h) = having {
        if expr_contains_aggregate(h) {
            return true;
        }
    }
    false
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::AggregateFunc { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::UnaryOp { operand, .. } => expr_contains_aggregate(operand),
        Expr::Like { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_aggregate),
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                if expr_contains_aggregate(op) {
                    return true;
                }
            }
            for (cond, then) in when_clauses {
                if expr_contains_aggregate(cond) || expr_contains_aggregate(then) {
                    return true;
                }
            }
            if let Some(e) = else_clause {
                if expr_contains_aggregate(e) {
                    return true;
                }
            }
            false
        }
        Expr::Cast { expr, .. } => expr_contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::GreaterThanZero(inner) => expr_contains_aggregate(inner),
        // Subquery variants: internal aggregation is self-contained
        Expr::InSubquery { expr, .. } => expr_contains_aggregate(expr),
        Expr::Exists { .. } => false,
        Expr::ScalarSubquery(_) => false,
        _ => false,
    }
}

/// Accumulator for aggregate functions.
enum Accumulator {
    Count {
        count: i64,
    },
    CountDistinct {
        values: HashSet<ValueKey>,
    },
    Sum {
        total: Option<Value>,
    },
    Min {
        val: Option<Value>,
    },
    Max {
        val: Option<Value>,
    },
    Avg {
        int_sum: i128,
        float_sum: f64,
        count: i64,
        has_float: bool,
    },
}

impl Accumulator {
    fn new(name: &str, distinct: bool) -> Self {
        match name {
            "COUNT" if distinct => Accumulator::CountDistinct {
                values: HashSet::new(),
            },
            "COUNT" => Accumulator::Count { count: 0 },
            "SUM" => Accumulator::Sum { total: None },
            "MIN" => Accumulator::Min { val: None },
            "MAX" => Accumulator::Max { val: None },
            "AVG" => Accumulator::Avg {
                int_sum: 0,
                float_sum: 0.0,
                count: 0,
                has_float: false,
            },
            _ => Accumulator::Count { count: 0 },
        }
    }

    fn feed(&mut self, val: &Value) {
        match self {
            Accumulator::Count { count } => {
                // COUNT(col) skips NULLs; COUNT(*) uses arg=None so this won't be called for NULLs
                if !val.is_null() {
                    *count += 1;
                }
            }
            Accumulator::CountDistinct { values } => {
                if !val.is_null() {
                    values.insert(ValueKey(val.clone()));
                }
            }
            Accumulator::Sum { total } => match val {
                Value::Integer(n) => {
                    *total = Some(match total.take() {
                        None => Value::Integer(*n),
                        Some(Value::Integer(cur)) => Value::Integer(cur + *n),
                        Some(Value::Float(cur)) => Value::Float(cur + (*n as f64)),
                        Some(other) => other,
                    });
                }
                Value::Float(n) => {
                    *total = Some(match total.take() {
                        None => Value::Float(*n),
                        Some(Value::Integer(cur)) => Value::Float((cur as f64) + *n),
                        Some(Value::Float(cur)) => Value::Float(cur + *n),
                        Some(other) => other,
                    });
                }
                _ => {}
            },
            Accumulator::Min { val: current } => {
                if val.is_null() {
                    return;
                }
                match current {
                    None => *current = Some(val.clone()),
                    Some(cur) => {
                        if cmp_values(Some(val), Some(cur)) == std::cmp::Ordering::Less {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            Accumulator::Max { val: current } => {
                if val.is_null() {
                    return;
                }
                match current {
                    None => *current = Some(val.clone()),
                    Some(cur) => {
                        if cmp_values(Some(val), Some(cur)) == std::cmp::Ordering::Greater {
                            *current = Some(val.clone());
                        }
                    }
                }
            }
            Accumulator::Avg {
                int_sum,
                float_sum,
                count,
                has_float,
            } => match val {
                Value::Integer(n) => {
                    *int_sum += *n as i128;
                    *float_sum += *n as f64;
                    *count += 1;
                }
                Value::Float(n) => {
                    *float_sum += *n;
                    *count += 1;
                    *has_float = true;
                }
                _ => {}
            },
        }
    }

    fn feed_count_star(&mut self) {
        if let Accumulator::Count { count } = self {
            *count += 1;
        }
    }

    fn finalize(&self) -> Value {
        match self {
            Accumulator::Count { count } => Value::Integer(*count),
            Accumulator::CountDistinct { values } => Value::Integer(values.len() as i64),
            Accumulator::Sum { total } => total.clone().unwrap_or(Value::Null),
            Accumulator::Min { val } => val.clone().unwrap_or(Value::Null),
            Accumulator::Max { val } => val.clone().unwrap_or(Value::Null),
            Accumulator::Avg {
                int_sum,
                float_sum,
                count,
                has_float,
            } => {
                if *count == 0 {
                    Value::Null
                } else if *has_float {
                    Value::Float(*float_sum / (*count as f64))
                } else {
                    let avg = *int_sum / (*count as i128);
                    Value::Integer(avg as i64)
                }
            }
        }
    }
}

/// Collect all AggregateFunc expressions from a list of SelectColumns and an optional HAVING clause.
/// Returns a list of (index, name, arg, distinct) for each aggregate found.
struct AggregateInfo {
    name: String,
    arg: Option<Expr>,
    distinct: bool,
}

fn collect_aggregates(columns: &[SelectColumn], having: &Option<Expr>) -> Vec<AggregateInfo> {
    let mut aggs = Vec::new();
    for col in columns {
        if let SelectColumn::Expr(expr, _) = col {
            collect_aggregates_from_expr(expr, &mut aggs);
        }
    }
    if let Some(h) = having {
        collect_aggregates_from_expr(h, &mut aggs);
    }
    aggs
}

fn collect_aggregates_from_expr(expr: &Expr, aggs: &mut Vec<AggregateInfo>) {
    match expr {
        Expr::AggregateFunc {
            name,
            arg,
            distinct,
        } => {
            // Check if we already have an identical aggregate
            let already_exists = aggs.iter().any(|a| {
                a.name == *name
                    && a.distinct == *distinct
                    && format!("{:?}", a.arg) == format!("{:?}", arg.as_deref().cloned())
            });
            if !already_exists {
                aggs.push(AggregateInfo {
                    name: name.clone(),
                    arg: arg.as_deref().cloned(),
                    distinct: *distinct,
                });
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_aggregates_from_expr(left, aggs);
            collect_aggregates_from_expr(right, aggs);
        }
        Expr::UnaryOp { operand, .. } => {
            collect_aggregates_from_expr(operand, aggs);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_aggregates_from_expr(arg, aggs);
            }
        }
        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(op) = operand {
                collect_aggregates_from_expr(op, aggs);
            }
            for (cond, then) in when_clauses {
                collect_aggregates_from_expr(cond, aggs);
                collect_aggregates_from_expr(then, aggs);
            }
            if let Some(e) = else_clause {
                collect_aggregates_from_expr(e, aggs);
            }
        }
        Expr::Cast { expr, .. } => collect_aggregates_from_expr(expr, aggs),
        Expr::Like { expr, pattern, .. } => {
            collect_aggregates_from_expr(expr, aggs);
            collect_aggregates_from_expr(pattern, aggs);
        }
        Expr::IsNull { expr, .. } => collect_aggregates_from_expr(expr, aggs),
        Expr::InList { expr, list, .. } => {
            collect_aggregates_from_expr(expr, aggs);
            for item in list {
                collect_aggregates_from_expr(item, aggs);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_aggregates_from_expr(expr, aggs);
            collect_aggregates_from_expr(low, aggs);
            collect_aggregates_from_expr(high, aggs);
        }
        Expr::GreaterThanZero(inner) => collect_aggregates_from_expr(inner, aggs),
        _ => {}
    }
}

/// Find the index of an aggregate in the aggs list that matches a given AggregateFunc expression.
fn find_aggregate_index(
    aggs: &[AggregateInfo],
    name: &str,
    arg: &Option<Box<Expr>>,
    distinct: bool,
) -> Option<usize> {
    aggs.iter().position(|a| {
        a.name == name
            && a.distinct == distinct
            && format!("{:?}", a.arg) == format!("{:?}", arg.as_deref().cloned())
    })
}

/// Substitute aggregate expressions in an Expr with their computed values.
/// Returns a new Expr with aggregates replaced by their finalized values.
fn substitute_aggregates(expr: &Expr, aggs: &[AggregateInfo], agg_values: &[Value]) -> Expr {
    match expr {
        Expr::AggregateFunc {
            name,
            arg,
            distinct,
        } => {
            if let Some(idx) = find_aggregate_index(aggs, name, arg, *distinct) {
                value_to_expr(&agg_values[idx])
            } else {
                Expr::Null
            }
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_aggregates(left, aggs, agg_values)),
            op: *op,
            right: Box::new(substitute_aggregates(right, aggs, agg_values)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op: *op,
            operand: Box::new(substitute_aggregates(operand, aggs, agg_values)),
        },
        _ => expr.clone(),
    }
}

/// Execute the aggregation pipeline for non-join queries.
/// Takes raw rows (Vec<Vec<Value>>), groups them, computes aggregates,
/// applies HAVING, and returns projected Rows.
pub(super) fn execute_aggregation(
    raw_rows: Vec<Vec<Value>>,
    table_def: &TableDef,
    sel: &Select,
) -> Result<Vec<Row>> {
    let aggs = collect_aggregates(&sel.columns, &sel.having);
    let has_group_by = sel.group_by.is_some();

    // Build groups: group_key -> list of raw rows
    let mut groups: Vec<(Vec<ValueKey>, Vec<Vec<Value>>)> = Vec::new();
    let mut group_index: HashMap<Vec<ValueKey>, usize> = HashMap::new();

    for raw_row in &raw_rows {
        let group_key = if let Some(group_exprs) = &sel.group_by {
            let mut key = Vec::with_capacity(group_exprs.len());
            for gexpr in group_exprs {
                let val = eval_expr(gexpr, &|name| {
                    table_def
                        .column_index(name)
                        .and_then(|i| raw_row.get(i).cloned())
                })?;
                key.push(ValueKey(val));
            }
            key
        } else {
            // No GROUP BY: all rows in one group
            vec![]
        };

        if let Some(&idx) = group_index.get(&group_key) {
            groups[idx].1.push(raw_row.clone());
        } else {
            let idx = groups.len();
            group_index.insert(group_key.clone(), idx);
            groups.push((group_key, vec![raw_row.clone()]));
        }
    }

    // If no rows and no GROUP BY, produce a single group (for SELECT COUNT(*) FROM empty_table)
    if groups.is_empty() && !has_group_by {
        groups.push((vec![], vec![]));
    }

    let mut result_rows = Vec::new();

    for (_group_key, group_rows) in &groups {
        // Create accumulators for each aggregate
        let mut accumulators: Vec<Accumulator> = aggs
            .iter()
            .map(|a| Accumulator::new(&a.name, a.distinct))
            .collect();

        // Feed rows into accumulators
        for raw_row in group_rows {
            for (i, agg_info) in aggs.iter().enumerate() {
                if let Some(arg_expr) = &agg_info.arg {
                    let val = eval_expr(arg_expr, &|name| {
                        table_def
                            .column_index(name)
                            .and_then(|j| raw_row.get(j).cloned())
                    })?;
                    accumulators[i].feed(&val);
                } else {
                    // COUNT(*)
                    accumulators[i].feed_count_star();
                }
            }
        }

        // Finalize aggregates
        let agg_values: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();

        // Apply HAVING filter
        if let Some(having_expr) = &sel.having {
            let substituted = substitute_aggregates(having_expr, &aggs, &agg_values);
            // Use a representative row from the group for column references
            let rep_row = group_rows.first();
            let result = eval_expr(&substituted, &|name| {
                if let Some(row) = rep_row {
                    table_def
                        .column_index(name)
                        .and_then(|i| row.get(i).cloned())
                } else {
                    None
                }
            })?;
            if !is_truthy(&result) {
                continue;
            }
        }

        // Project SELECT columns
        let rep_row = group_rows.first();
        let mut row_values = Vec::new();

        for sel_col in &sel.columns {
            match sel_col {
                SelectColumn::Star => {
                    if let Some(raw) = rep_row {
                        for (i, col) in table_def.columns.iter().enumerate() {
                            if col.is_hidden {
                                continue;
                            }
                            let val = raw.get(i).cloned().unwrap_or(Value::Null);
                            row_values.push((col.name.clone(), val));
                        }
                    }
                }
                SelectColumn::Expr(expr, alias) => {
                    let substituted = substitute_aggregates(expr, &aggs, &agg_values);
                    let val = eval_expr(&substituted, &|name| {
                        if let Some(row) = rep_row {
                            table_def
                                .column_index(name)
                                .and_then(|i| row.get(i).cloned())
                        } else {
                            None
                        }
                    })?;
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::ColumnRef(n) => n.clone(),
                        Expr::AggregateFunc {
                            name,
                            arg,
                            distinct,
                        } => {
                            let arg_str = match arg {
                                None => "*".to_string(),
                                Some(a) => {
                                    if *distinct {
                                        format!("DISTINCT {:?}", a)
                                    } else {
                                        format!("{:?}", a)
                                    }
                                }
                            };
                            format!("{}({})", name, arg_str)
                        }
                        _ => "?column?".to_string(),
                    });
                    row_values.push((name, val));
                }
            }
        }

        result_rows.push(Row { values: row_values });
    }

    Ok(result_rows)
}

/// Execute the aggregation pipeline for join queries.
pub(super) fn execute_aggregation_join(
    joined_rows: &[Vec<(String, Value)>],
    sel: &Select,
    hidden_columns: &[String],
) -> Result<Vec<Row>> {
    let aggs = collect_aggregates(&sel.columns, &sel.having);
    let has_group_by = sel.group_by.is_some();

    // Build groups
    #[allow(clippy::type_complexity)]
    let mut groups: Vec<(Vec<ValueKey>, Vec<&Vec<(String, Value)>>)> = Vec::new();
    let mut group_index: HashMap<Vec<ValueKey>, usize> = HashMap::new();

    for jrow in joined_rows {
        let group_key = if let Some(group_exprs) = &sel.group_by {
            let mut key = Vec::with_capacity(group_exprs.len());
            for gexpr in group_exprs {
                let val = eval_join_expr(gexpr, jrow)?;
                key.push(ValueKey(val));
            }
            key
        } else {
            vec![]
        };

        if let Some(&idx) = group_index.get(&group_key) {
            groups[idx].1.push(jrow);
        } else {
            let idx = groups.len();
            group_index.insert(group_key.clone(), idx);
            groups.push((group_key, vec![jrow]));
        }
    }

    if groups.is_empty() && !has_group_by {
        groups.push((vec![], vec![]));
    }

    let mut result_rows = Vec::new();

    for (_group_key, group_rows) in &groups {
        let mut accumulators: Vec<Accumulator> = aggs
            .iter()
            .map(|a| Accumulator::new(&a.name, a.distinct))
            .collect();

        for jrow in group_rows {
            for (i, agg_info) in aggs.iter().enumerate() {
                if let Some(arg_expr) = &agg_info.arg {
                    let val = eval_join_expr(arg_expr, jrow)?;
                    accumulators[i].feed(&val);
                } else {
                    accumulators[i].feed_count_star();
                }
            }
        }

        let agg_values: Vec<Value> = accumulators.iter().map(|a| a.finalize()).collect();

        if let Some(having_expr) = &sel.having {
            let substituted = substitute_aggregates(having_expr, &aggs, &agg_values);
            let rep_row = group_rows.first().map(|r| r.as_slice()).unwrap_or(&[]);
            let result = eval_join_expr(&substituted, rep_row)?;
            if !is_truthy(&result) {
                continue;
            }
        }

        let rep_row = group_rows.first().map(|r| r.as_slice()).unwrap_or(&[]);
        let mut row_values = Vec::new();

        for sel_col in &sel.columns {
            match sel_col {
                SelectColumn::Star => {
                    for (qualified_name, val) in rep_row {
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
                    let substituted = substitute_aggregates(expr, &aggs, &agg_values);
                    let val = eval_join_expr(&substituted, rep_row)?;
                    let name = alias.clone().unwrap_or_else(|| match expr {
                        Expr::ColumnRef(n) => n.clone(),
                        Expr::AggregateFunc {
                            name,
                            arg,
                            distinct,
                        } => {
                            let arg_str = match arg {
                                None => "*".to_string(),
                                Some(a) => {
                                    if *distinct {
                                        format!("DISTINCT {:?}", a)
                                    } else {
                                        format!("{:?}", a)
                                    }
                                }
                            };
                            format!("{}({})", name, arg_str)
                        }
                        _ => "?column?".to_string(),
                    });
                    row_values.push((name, val));
                }
            }
        }

        result_rows.push(Row { values: row_values });
    }

    Ok(result_rows)
}

pub(super) fn cmp_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    fn cmp_i64_f64(i: i64, f: f64) -> std::cmp::Ordering {
        if f.is_nan() {
            return std::cmp::Ordering::Equal;
        }
        if f >= i64::MAX as f64 {
            return std::cmp::Ordering::Less;
        }
        if f < i64::MIN as f64 {
            return std::cmp::Ordering::Greater;
        }

        let t = f.trunc() as i64;
        if i < t {
            return std::cmp::Ordering::Less;
        }
        if i > t {
            return std::cmp::Ordering::Greater;
        }

        let frac = f.fract();
        if frac > 0.0 {
            std::cmp::Ordering::Less
        } else if frac < 0.0 {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    }

    match (a, b) {
        (Some(Value::Integer(a)), Some(Value::Integer(b))) => a.cmp(b),
        (Some(Value::Float(a)), Some(Value::Float(b))) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Some(Value::Integer(a)), Some(Value::Float(b))) => cmp_i64_f64(*a, *b),
        (Some(Value::Float(a)), Some(Value::Integer(b))) => cmp_i64_f64(*b, *a).reverse(),
        (Some(Value::Date(a)), Some(Value::Date(b))) => a.cmp(b),
        (Some(Value::DateTime(a)), Some(Value::DateTime(b))) => a.cmp(b),
        (Some(Value::Timestamp(a)), Some(Value::Timestamp(b))) => a.cmp(b),
        (Some(Value::Date(a)), Some(Value::DateTime(b))) => ((*a as i64) * 1_000_000).cmp(b),
        (Some(Value::DateTime(a)), Some(Value::Date(b))) => a.cmp(&((*b as i64) * 1_000_000)),
        (Some(Value::Date(a)), Some(Value::Timestamp(b))) => ((*a as i64) * 1_000_000).cmp(b),
        (Some(Value::Timestamp(a)), Some(Value::Date(b))) => a.cmp(&((*b as i64) * 1_000_000)),
        (Some(Value::DateTime(a)), Some(Value::Timestamp(b))) => a.cmp(b),
        (Some(Value::Timestamp(a)), Some(Value::DateTime(b))) => a.cmp(b),
        (Some(Value::Varchar(a)), Some(Value::Varchar(b))) => a.cmp(b),
        (Some(Value::Varbinary(a)), Some(Value::Varbinary(b))) => a.cmp(b),
        (Some(Value::Null), _) | (None, _) => std::cmp::Ordering::Less,
        (_, Some(Value::Null)) | (_, None) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}
