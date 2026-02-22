/// Rule-based query planner.
///
/// Plan types:
///   PkSeek(key)       - Primary key lookup
///   IndexSeek(idx, key) - Secondary index lookup
///   FullScan          - Full table scan
///   FtsScan(col, query, mode) - FTS search
use crate::sql::ast::*;

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

/// Stable heuristic cost used by the planner for deterministic plan selection.
pub fn plan_cost_hint(plan: &Plan) -> u64 {
    match plan {
        Plan::PkSeek { .. } => 100,
        Plan::IndexSeek { key_exprs, .. } => 3000u64.saturating_sub((key_exprs.len() as u64) * 900),
        Plan::IndexRangeSeek {
            prefix_key_exprs, ..
        } => 3200u64.saturating_sub(((prefix_key_exprs.len() as u64) + 1) * 800),
        Plan::FtsScan { .. } => 2000,
        Plan::FullScan { .. } => 10_000,
    }
}

/// Analyze a WHERE clause and determine the access plan.
pub fn plan_select(
    table_name: &str,
    pk_columns: &[String],
    index_columns: &[(String, Vec<String>)], // (index_name, column_names)
    where_clause: &Option<Expr>,
) -> Plan {
    let mut best_candidate: Option<(u64, String, Plan)> = None;
    let consider = |best: &mut Option<(u64, String, Plan)>, plan: Plan, tie_key: String| {
        let cost = plan_cost_hint(&plan);
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
        for (idx_name, col_names) in index_columns {
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
                        BinaryOp::Gt => entry.lower = Some((*right.clone(), false)),
                        BinaryOp::Ge => entry.lower = Some((*right.clone(), true)),
                        BinaryOp::Lt => entry.upper = Some((*right.clone(), false)),
                        BinaryOp::Le => entry.upper = Some((*right.clone(), true)),
                        _ => {}
                    }
                } else if let Expr::ColumnRef(col) = right.as_ref() {
                    if !is_row_independent_expr(left) {
                        return;
                    }
                    let entry = result.entry(col.clone()).or_default();
                    match op {
                        BinaryOp::Gt => entry.upper = Some((*left.clone(), false)),
                        BinaryOp::Ge => entry.upper = Some((*left.clone(), true)),
                        BinaryOp::Lt => entry.lower = Some((*left.clone(), false)),
                        BinaryOp::Le => entry.lower = Some((*left.clone(), true)),
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
                    entry.lower = Some((*low.clone(), true));
                    entry.upper = Some((*high.clone(), true));
                }
            }
        }
        _ => {}
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
        Expr::MatchAgainst { .. } | Expr::FtsSnippet { .. } => true,
        Expr::IntLiteral(_)
        | Expr::FloatLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BlobLiteral(_)
        | Expr::Null
        | Expr::DefaultValue => true,
    }
}
