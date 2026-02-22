/// Rule-based query planner.
///
/// Plan types:
///   PkSeek(key)       - Primary key lookup
///   IndexSeek(idx, key) - Secondary index lookup
///   FullScan          - Full table scan
///   FtsScan(col, query, mode) - FTS search
use crate::sql::ast::*;
use crate::sql::eval::eval_expr;

#[derive(Debug, Clone)]
pub struct IndexPlanStat {
    pub name: String,
    pub column_names: Vec<String>,
    pub is_unique: bool,
    pub stats_distinct_keys: u64,
    pub stats_num_min: Option<i64>,
    pub stats_num_max: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PlannerStats {
    /// 0 means unknown/not analyzed.
    pub table_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinLoopOrder {
    LeftOuter,
    RightOuter,
}

#[derive(Debug)]
pub enum Plan {
    PkSeek {
        table_name: String,
        key_exprs: Vec<(String, Expr)>, // (column_name, value_expr)
    },
    IndexSeek {
        table_name: String,
        index_name: String,
        column_names: Vec<String>,
        key_exprs: Vec<Expr>,
    },
    IndexRangeSeek {
        table_name: String,
        index_name: String,
        column_names: Vec<String>,
        prefix_key_exprs: Vec<Expr>,
        lower: Option<(Box<Expr>, bool)>, // (expr, inclusive)
        upper: Option<(Box<Expr>, bool)>, // (expr, inclusive)
    },
    FullScan {
        table_name: String,
    },
    FtsScan {
        table_name: String,
        column: String,
        query: String,
        mode: MatchMode,
    },
}

/// Choose nested-loop order from estimated cardinalities.
pub fn choose_nested_loop_order(left_rows_est: u64, right_rows_est: u64) -> JoinLoopOrder {
    if right_rows_est < left_rows_est {
        JoinLoopOrder::RightOuter
    } else {
        JoinLoopOrder::LeftOuter
    }
}

/// Stable heuristic cost used by the planner for deterministic plan selection.
pub fn plan_cost_hint(plan: &Plan) -> u64 {
    plan_cost_hint_with_stats(plan, &PlannerStats::default(), &[])
}

/// Cost hint that incorporates persisted stats when available.
pub fn plan_cost_hint_with_stats(
    plan: &Plan,
    planner_stats: &PlannerStats,
    index_stats: &[IndexPlanStat],
) -> u64 {
    let est_rows = estimate_plan_rows_hint(plan, planner_stats, index_stats);
    match plan {
        Plan::PkSeek { .. } => 100u64.saturating_add(est_rows),
        Plan::IndexSeek { key_exprs, .. } => 1_500u64
            .saturating_sub((key_exprs.len() as u64).saturating_mul(300))
            .saturating_add(est_rows.saturating_mul(3)),
        Plan::IndexRangeSeek {
            prefix_key_exprs,
            lower,
            upper,
            ..
        } => {
            let bound_terms = (lower.is_some() as u64) + (upper.is_some() as u64);
            1_400u64
                .saturating_sub((prefix_key_exprs.len() as u64).saturating_mul(250))
                .saturating_sub(bound_terms.saturating_mul(250))
                .saturating_add(est_rows.saturating_mul(3))
        }
        Plan::FtsScan { .. } => 2_000u64.saturating_add(est_rows.saturating_mul(2)),
        Plan::FullScan { .. } => 3_000u64.saturating_add(est_rows.saturating_mul(5)),
    }
}

/// Heuristic row estimate used for both planning and EXPLAIN.
pub fn estimate_plan_rows_hint(
    plan: &Plan,
    planner_stats: &PlannerStats,
    index_stats: &[IndexPlanStat],
) -> u64 {
    let table_rows = table_rows_hint(planner_stats);
    match plan {
        Plan::PkSeek { .. } => 1,
        Plan::IndexSeek {
            index_name,
            key_exprs,
            ..
        } => {
            let index = index_stats.iter().find(|idx| idx.name == *index_name);
            let full_key_equality = index
                .map(|idx| idx.is_unique && key_exprs.len() == idx.column_names.len())
                .unwrap_or(false);
            estimate_index_seek_rows(table_rows, key_exprs.len(), index, full_key_equality)
        }
        Plan::IndexRangeSeek {
            index_name,
            prefix_key_exprs,
            lower,
            upper,
            ..
        } => {
            let index = index_stats.iter().find(|idx| idx.name == *index_name);
            let prefix_rows =
                estimate_index_seek_rows(table_rows, prefix_key_exprs.len(), index, false);
            let ranged_rows = if prefix_key_exprs.is_empty() {
                estimate_numeric_range_rows(prefix_rows, lower, upper, index).unwrap_or_else(|| {
                    match (lower.is_some(), upper.is_some()) {
                        (true, true) => div_ceil(prefix_rows, 5),
                        (true, false) | (false, true) => div_ceil(prefix_rows, 2),
                        (false, false) => prefix_rows,
                    }
                })
            } else {
                match (lower.is_some(), upper.is_some()) {
                    (true, true) => div_ceil(prefix_rows, 5),
                    (true, false) | (false, true) => div_ceil(prefix_rows, 2),
                    (false, false) => prefix_rows,
                }
            };
            ranged_rows.max(1).min(table_rows)
        }
        Plan::FullScan { .. } => table_rows,
        Plan::FtsScan { .. } => div_ceil(table_rows.saturating_mul(3), 10).max(1),
    }
}

/// Analyze a WHERE clause and determine the access plan.
pub fn plan_select(
    table_name: &str,
    pk_columns: &[String],
    index_stats: &[IndexPlanStat],
    where_clause: &Option<Expr>,
    planner_stats: PlannerStats,
) -> Plan {
    let mut best_candidate: Option<(u64, String, Plan)> = None;
    let consider = |best: &mut Option<(u64, String, Plan)>, plan: Plan, tie_key: String| {
        let cost = plan_cost_hint_with_stats(&plan, &planner_stats, index_stats);
        match best {
            Some((best_cost, best_tie, _)) if *best_cost < cost => {}
            Some((best_cost, best_tie, _)) if *best_cost == cost && *best_tie <= tie_key => {}
            _ => *best = Some((cost, tie_key, plan)),
        }
    };

    if let Some(expr) = where_clause {
        // Check for FTS MATCH...AGAINST
        if let Some((column, query, mode)) = extract_fts_match(expr) {
            return Plan::FtsScan {
                table_name: table_name.to_string(),
                column,
                query,
                mode,
            };
        }

        // Check for PK equality (single or composite)
        if !pk_columns.is_empty() {
            let equalities = extract_equalities(expr);
            if pk_columns.len() == 1 {
                // Single PK: simple equality
                if let Some(key_expr) = equalities.iter().find_map(|(col, e)| {
                    if col == &pk_columns[0] {
                        Some(e.clone())
                    } else {
                        None
                    }
                }) {
                    return Plan::PkSeek {
                        table_name: table_name.to_string(),
                        key_exprs: vec![(pk_columns[0].clone(), key_expr)],
                    };
                }
            } else {
                // Composite PK: need all columns matched
                let mut key_exprs = Vec::new();
                let mut all_found = true;
                for pk_col in pk_columns {
                    if let Some((_, e)) = equalities.iter().find(|(col, _)| col == pk_col) {
                        key_exprs.push((pk_col.clone(), e.clone()));
                    } else {
                        all_found = false;
                        break;
                    }
                }
                if all_found {
                    return Plan::PkSeek {
                        table_name: table_name.to_string(),
                        key_exprs,
                    };
                }
            }
        }

        // Check for index equality (single or composite)
        let equalities = extract_equalities(expr);
        let ranges = extract_ranges(expr);
        for idx in index_stats {
            let idx_name = &idx.name;
            let col_names = &idx.column_names;
            if col_names.len() == 1 {
                if let Some(key_expr) = equalities.iter().find_map(|(col, e)| {
                    if col == &col_names[0] {
                        Some(e.clone())
                    } else {
                        None
                    }
                }) {
                    if is_row_independent_expr(&key_expr) {
                        consider(
                            &mut best_candidate,
                            Plan::IndexSeek {
                                table_name: table_name.to_string(),
                                index_name: idx_name.clone(),
                                column_names: col_names.clone(),
                                key_exprs: vec![key_expr],
                            },
                            format!("0:{}", idx_name),
                        );
                    }
                }
                if let Some(range) = ranges.get(&col_names[0]) {
                    consider(
                        &mut best_candidate,
                        Plan::IndexRangeSeek {
                            table_name: table_name.to_string(),
                            index_name: idx_name.clone(),
                            column_names: col_names.clone(),
                            prefix_key_exprs: Vec::new(),
                            lower: range.lower.clone().map(|(e, i)| (Box::new(e), i)),
                            upper: range.upper.clone().map(|(e, i)| (Box::new(e), i)),
                        },
                        format!("1:{}", idx_name),
                    );
                }
            } else {
                // Composite index:
                // 1) exact seek if all columns have equality
                // 2) range seek if prefix equalities exist and the next (last) column has a range predicate
                let mut key_exprs = Vec::new();
                let mut prefix_key_exprs = Vec::new();
                let mut prefix_len = 0usize;
                for col_name in col_names {
                    if let Some((_, e)) = equalities.iter().find(|(col, _)| col == col_name) {
                        key_exprs.push(e.clone());
                        prefix_key_exprs.push(e.clone());
                        prefix_len += 1;
                    } else {
                        break;
                    }
                }
                if prefix_len == col_names.len() {
                    if key_exprs.iter().all(is_row_independent_expr) {
                        consider(
                            &mut best_candidate,
                            Plan::IndexSeek {
                                table_name: table_name.to_string(),
                                index_name: idx_name.clone(),
                                column_names: col_names.clone(),
                                key_exprs,
                            },
                            format!("0:{}", idx_name),
                        );
                    }
                    continue;
                }

                if prefix_len < col_names.len() && prefix_len + 1 == col_names.len() {
                    let range_col = &col_names[prefix_len];
                    if let Some(range) = ranges.get(range_col) {
                        if prefix_key_exprs.iter().all(is_row_independent_expr) {
                            consider(
                                &mut best_candidate,
                                Plan::IndexRangeSeek {
                                    table_name: table_name.to_string(),
                                    index_name: idx_name.clone(),
                                    column_names: col_names.clone(),
                                    prefix_key_exprs,
                                    lower: range.lower.clone().map(|(e, i)| (Box::new(e), i)),
                                    upper: range.upper.clone().map(|(e, i)| (Box::new(e), i)),
                                },
                                format!("1:{}", idx_name),
                            );
                        }
                    }
                }
            }
        }
    }

    if let Some((_, _, plan)) = best_candidate {
        return plan;
    }

    Plan::FullScan {
        table_name: table_name.to_string(),
    }
}

fn table_rows_hint(planner_stats: &PlannerStats) -> u64 {
    if planner_stats.table_rows > 0 {
        planner_stats.table_rows
    } else {
        // Conservative fallback before ANALYZE TABLE is run.
        10_000
    }
}

fn estimate_index_seek_rows(
    table_rows: u64,
    key_parts: usize,
    index: Option<&IndexPlanStat>,
    full_key_equality: bool,
) -> u64 {
    if key_parts == 0 {
        return table_rows.max(1);
    }

    if full_key_equality {
        return 1;
    }

    let mut rows = if let Some(idx) = index {
        if idx.stats_distinct_keys > 0 {
            div_ceil(table_rows.max(1), idx.stats_distinct_keys)
        } else {
            div_ceil(table_rows.max(1), 4)
        }
    } else {
        div_ceil(table_rows.max(1), 4)
    };

    for _ in 1..key_parts {
        rows = div_ceil(rows, 2);
    }
    rows.max(1).min(table_rows.max(1))
}

fn div_ceil(num: u64, den: u64) -> u64 {
    if den == 0 {
        return num;
    }
    num.saturating_add(den - 1) / den
}

fn estimate_numeric_range_rows(
    prefix_rows: u64,
    lower: &Option<(Box<Expr>, bool)>,
    upper: &Option<(Box<Expr>, bool)>,
    index: Option<&IndexPlanStat>,
) -> Option<u64> {
    let idx = index?;
    let min_v = idx.stats_num_min?;
    let max_v = idx.stats_num_max?;
    if min_v > max_v {
        return None;
    }

    let mut lo = min_v;
    let mut hi = max_v;
    if let Some((expr, inclusive)) = lower {
        let v = const_i64(expr)?;
        lo = if *inclusive { v } else { v.saturating_add(1) };
    }
    if let Some((expr, inclusive)) = upper {
        let v = const_i64(expr)?;
        hi = if *inclusive { v } else { v.saturating_sub(1) };
    }

    if hi < lo {
        return Some(1);
    }

    let clamped_lo = lo.max(min_v);
    let clamped_hi = hi.min(max_v);
    if clamped_hi < clamped_lo {
        return Some(1);
    }

    let covered = span_inclusive_u128(clamped_lo, clamped_hi)?;
    let total = span_inclusive_u128(min_v, max_v)?;
    let scaled = div_ceil_u128((prefix_rows as u128).saturating_mul(covered), total);
    Some((scaled as u64).max(1))
}

fn const_i64(expr: &Expr) -> Option<i64> {
    match eval_expr(expr, &|_| None).ok()? {
        crate::types::Value::Integer(n) => Some(n),
        crate::types::Value::Date(n) => Some(n as i64),
        crate::types::Value::DateTime(n) => Some(n),
        crate::types::Value::Timestamp(n) => Some(n),
        _ => None,
    }
}

fn span_inclusive_u128(lo: i64, hi: i64) -> Option<u128> {
    if hi < lo {
        return None;
    }
    Some((hi as i128 - lo as i128 + 1) as u128)
}

fn div_ceil_u128(num: u128, den: u128) -> u128 {
    if den == 0 {
        return num;
    }
    num.saturating_add(den - 1) / den
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_range_estimator_handles_full_i64_span() {
        let idx = IndexPlanStat {
            name: "idx_a".to_string(),
            column_names: vec!["a".to_string()],
            is_unique: false,
            stats_distinct_keys: 0,
            stats_num_min: Some(i64::MIN),
            stats_num_max: Some(i64::MAX),
        };
        let lower = Some((Box::new(Expr::IntLiteral(0)), true));
        let rows = estimate_numeric_range_rows(1000, &lower, &None, Some(&idx)).unwrap();
        assert_eq!(rows, 500);
    }

    #[test]
    fn test_choose_nested_loop_order_prefers_smaller_outer() {
        assert_eq!(choose_nested_loop_order(10, 9), JoinLoopOrder::RightOuter);
        assert_eq!(choose_nested_loop_order(9, 10), JoinLoopOrder::LeftOuter);
        assert_eq!(choose_nested_loop_order(10, 10), JoinLoopOrder::LeftOuter);
    }
}

/// Extract FTS match from expression tree.
fn extract_fts_match(expr: &Expr) -> Option<(String, String, MatchMode)> {
    match expr {
        Expr::MatchAgainst {
            column,
            query,
            mode,
        } => Some((column.clone(), query.clone(), *mode)),
        Expr::BinaryOp {
            left,
            op: BinaryOp::Gt,
            right,
        } => {
            // MATCH(...) AGAINST(...) > 0
            if let (
                Expr::MatchAgainst {
                    column,
                    query,
                    mode,
                },
                Expr::IntLiteral(0),
            ) = (left.as_ref(), right.as_ref())
            {
                Some((column.clone(), query.clone(), *mode))
            } else {
                None
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => extract_fts_match(left).or_else(|| extract_fts_match(right)),
        _ => None,
    }
}

/// Extract all equality conditions from an AND-connected expression.
/// Returns vec of (column_name, value_expr).
fn extract_equalities(expr: &Expr) -> Vec<(String, Expr)> {
    let mut result = Vec::new();
    collect_equalities(expr, &mut result);
    result
}

fn collect_equalities(expr: &Expr, result: &mut Vec<(String, Expr)>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let Expr::ColumnRef(ref name) = **left {
                result.push((name.clone(), *right.clone()));
            } else if let Expr::ColumnRef(ref name) = **right {
                result.push((name.clone(), *left.clone()));
            }
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            collect_equalities(left, result);
            collect_equalities(right, result);
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Default)]
struct ColumnRange {
    lower: Option<(Expr, bool)>,
    upper: Option<(Expr, bool)>,
}

fn extract_ranges(expr: &Expr) -> std::collections::HashMap<String, ColumnRange> {
    let mut result = std::collections::HashMap::new();
    collect_ranges(expr, &mut result);
    result
}

fn collect_ranges(expr: &Expr, result: &mut std::collections::HashMap<String, ColumnRange>) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOp::And => {
                collect_ranges(left, result);
                collect_ranges(right, result);
            }
            BinaryOp::Gt | BinaryOp::Ge | BinaryOp::Lt | BinaryOp::Le => {
                if let Expr::ColumnRef(col) = left.as_ref() {
                    if !is_row_independent_expr(right) {
                        return;
                    }
                    let entry = result.entry(col.clone()).or_default();
                    match op {
                        BinaryOp::Gt => merge_lower(entry, *right.clone(), false),
                        BinaryOp::Ge => merge_lower(entry, *right.clone(), true),
                        BinaryOp::Lt => merge_upper(entry, *right.clone(), false),
                        BinaryOp::Le => merge_upper(entry, *right.clone(), true),
                        _ => {}
                    }
                } else if let Expr::ColumnRef(col) = right.as_ref() {
                    if !is_row_independent_expr(left) {
                        return;
                    }
                    let entry = result.entry(col.clone()).or_default();
                    match op {
                        BinaryOp::Gt => merge_upper(entry, *left.clone(), false),
                        BinaryOp::Ge => merge_upper(entry, *left.clone(), true),
                        BinaryOp::Lt => merge_lower(entry, *left.clone(), false),
                        BinaryOp::Le => merge_lower(entry, *left.clone(), true),
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if let Expr::ColumnRef(col) = expr.as_ref() {
                if is_row_independent_expr(low) && is_row_independent_expr(high) {
                    let entry = result.entry(col.clone()).or_default();
                    merge_lower(entry, *low.clone(), true);
                    merge_upper(entry, *high.clone(), true);
                }
            }
        }
        _ => {}
    }
}

fn merge_lower(entry: &mut ColumnRange, expr: Expr, inclusive: bool) {
    match &entry.lower {
        None => entry.lower = Some((expr, inclusive)),
        Some((cur_expr, cur_inclusive)) => match compare_const_expr(cur_expr, &expr) {
            Some(std::cmp::Ordering::Less) => entry.lower = Some((expr, inclusive)),
            Some(std::cmp::Ordering::Equal) => {
                entry.lower = Some((cur_expr.clone(), *cur_inclusive && inclusive))
            }
            Some(std::cmp::Ordering::Greater) | None => {}
        },
    }
}

fn merge_upper(entry: &mut ColumnRange, expr: Expr, inclusive: bool) {
    match &entry.upper {
        None => entry.upper = Some((expr, inclusive)),
        Some((cur_expr, cur_inclusive)) => match compare_const_expr(cur_expr, &expr) {
            Some(std::cmp::Ordering::Greater) => entry.upper = Some((expr, inclusive)),
            Some(std::cmp::Ordering::Equal) => {
                entry.upper = Some((cur_expr.clone(), *cur_inclusive && inclusive))
            }
            Some(std::cmp::Ordering::Less) | None => {}
        },
    }
}

fn compare_const_expr(left: &Expr, right: &Expr) -> Option<std::cmp::Ordering> {
    let lt = Expr::BinaryOp {
        left: Box::new(left.clone()),
        op: BinaryOp::Lt,
        right: Box::new(right.clone()),
    };
    if eval_to_true(&lt)? {
        return Some(std::cmp::Ordering::Less);
    }

    let gt = Expr::BinaryOp {
        left: Box::new(left.clone()),
        op: BinaryOp::Gt,
        right: Box::new(right.clone()),
    };
    if eval_to_true(&gt)? {
        return Some(std::cmp::Ordering::Greater);
    }

    let eq = Expr::BinaryOp {
        left: Box::new(left.clone()),
        op: BinaryOp::Eq,
        right: Box::new(right.clone()),
    };
    if eval_to_true(&eq)? {
        return Some(std::cmp::Ordering::Equal);
    }

    None
}

fn eval_to_true(expr: &Expr) -> Option<bool> {
    match eval_expr(expr, &|_| None).ok()? {
        crate::types::Value::Integer(n) => Some(n != 0),
        _ => None,
    }
}

fn is_row_independent_expr(expr: &Expr) -> bool {
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
