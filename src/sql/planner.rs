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
        key_expr: Expr,
    },
    IndexSeek {
        table_name: String,
        index_name: String,
        column_name: String,
        key_expr: Expr,
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

/// Analyze a WHERE clause and determine the access plan.
pub fn plan_select(
    table_name: &str,
    pk_column: Option<&str>,
    index_columns: &[(String, String)], // (index_name, column_name)
    where_clause: &Option<Expr>,
) -> Plan {
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

        // Check for PK equality: WHERE pk_col = value
        if let Some(pk) = pk_column {
            if let Some(key_expr) = extract_equality(expr, pk) {
                return Plan::PkSeek {
                    table_name: table_name.to_string(),
                    key_expr,
                };
            }
        }

        // Check for index equality
        for (idx_name, col_name) in index_columns {
            if let Some(key_expr) = extract_equality(expr, col_name) {
                return Plan::IndexSeek {
                    table_name: table_name.to_string(),
                    index_name: idx_name.clone(),
                    column_name: col_name.clone(),
                    key_expr,
                };
            }
        }
    }

    Plan::FullScan {
        table_name: table_name.to_string(),
    }
}

/// Extract FTS match from expression tree.
fn extract_fts_match(expr: &Expr) -> Option<(String, String, MatchMode)> {
    match expr {
        Expr::MatchAgainst { column, query, mode } => {
            Some((column.clone(), query.clone(), *mode))
        }
        Expr::BinaryOp { left, op: BinaryOp::Gt, right } => {
            // MATCH(...) AGAINST(...) > 0
            if let (Expr::MatchAgainst { column, query, mode }, Expr::IntLiteral(0)) =
                (left.as_ref(), right.as_ref())
            {
                Some((column.clone(), query.clone(), *mode))
            } else {
                None
            }
        }
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            extract_fts_match(left).or_else(|| extract_fts_match(right))
        }
        _ => None,
    }
}

/// Extract equality condition: column = value.
fn extract_equality(expr: &Expr, column_name: &str) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let Expr::ColumnRef(ref name) = **left {
                if name == column_name {
                    return Some(*right.clone());
                }
            }
            if let Expr::ColumnRef(ref name) = **right {
                if name == column_name {
                    return Some(*left.clone());
                }
            }
            None
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            ..
        } => extract_equality(left, column_name),
        _ => None,
    }
}
