/// Expression evaluator for WHERE clauses.

use crate::error::{MuroError, Result};
use crate::sql::ast::{BinaryOp, Expr};
use crate::types::Value;

/// Evaluate an expression given a row's column values.
/// `columns` maps column name -> Value.
pub fn eval_expr(
    expr: &Expr,
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Value> {
    match expr {
        Expr::IntLiteral(n) => Ok(Value::Int64(*n)),
        Expr::StringLiteral(s) => Ok(Value::Varchar(s.clone())),
        Expr::BlobLiteral(b) => Ok(Value::Varbinary(b.clone())),
        Expr::Null => Ok(Value::Null),

        Expr::ColumnRef(name) => {
            columns(name).ok_or_else(|| {
                MuroError::Execution(format!("Unknown column: {}", name))
            })
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, columns)?;
            let rval = eval_expr(right, columns)?;
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::MatchAgainst { .. } => {
            // FTS scoring - returns a float-like value, represented as Int64 for MVP
            // Actual FTS evaluation happens in the executor
            Ok(Value::Int64(0))
        }

        Expr::FtsSnippet { .. } => {
            // FTS snippet - handled in executor
            Ok(Value::Varchar(String::new()))
        }

        Expr::GreaterThanZero(inner) => {
            let val = eval_expr(inner, columns)?;
            match val {
                Value::Int64(n) => Ok(Value::Int64(if n > 0 { 1 } else { 0 })),
                _ => Ok(Value::Int64(0)),
            }
        }
    }
}

fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    // Handle NULL comparisons
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::And => {
                // NULL AND FALSE = FALSE, NULL AND TRUE = NULL
                if matches!(left, Value::Int64(0)) || matches!(right, Value::Int64(0)) {
                    Ok(Value::Int64(0))
                } else {
                    Ok(Value::Null)
                }
            }
            BinaryOp::Or => {
                // NULL OR TRUE = TRUE
                if matches!(left, Value::Int64(n) if *n != 0)
                    || matches!(right, Value::Int64(n) if *n != 0)
                {
                    Ok(Value::Int64(1))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null), // NULL comparison always NULL
        };
    }

    match op {
        BinaryOp::Eq => Ok(Value::Int64(if value_cmp(left, right) == Some(std::cmp::Ordering::Equal) { 1 } else { 0 })),
        BinaryOp::Ne => Ok(Value::Int64(if value_cmp(left, right) != Some(std::cmp::Ordering::Equal) { 1 } else { 0 })),
        BinaryOp::Lt => Ok(Value::Int64(if value_cmp(left, right) == Some(std::cmp::Ordering::Less) { 1 } else { 0 })),
        BinaryOp::Gt => Ok(Value::Int64(if value_cmp(left, right) == Some(std::cmp::Ordering::Greater) { 1 } else { 0 })),
        BinaryOp::Le => Ok(Value::Int64(if matches!(value_cmp(left, right), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)) { 1 } else { 0 })),
        BinaryOp::Ge => Ok(Value::Int64(if matches!(value_cmp(left, right), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)) { 1 } else { 0 })),
        BinaryOp::And => {
            let l = is_truthy(left);
            let r = is_truthy(right);
            Ok(Value::Int64(if l && r { 1 } else { 0 }))
        }
        BinaryOp::Or => {
            let l = is_truthy(left);
            let r = is_truthy(right);
            Ok(Value::Int64(if l || r { 1 } else { 0 }))
        }
    }
}

fn value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Int64(a), Value::Int64(b)) => Some(a.cmp(b)),
        (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
        (Value::Varbinary(a), Value::Varbinary(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Int64(n) => *n != 0,
        Value::Varchar(s) => !s.is_empty(),
        Value::Varbinary(b) => !b.is_empty(),
        Value::Null => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_literals() {
        let lookup = |_: &str| -> Option<Value> { None };
        assert_eq!(eval_expr(&Expr::IntLiteral(42), &lookup).unwrap(), Value::Int64(42));
        assert_eq!(
            eval_expr(&Expr::StringLiteral("hello".into()), &lookup).unwrap(),
            Value::Varchar("hello".into())
        );
    }

    #[test]
    fn test_eval_comparison() {
        let lookup = |name: &str| -> Option<Value> {
            match name {
                "id" => Some(Value::Int64(5)),
                _ => None,
            }
        };

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::IntLiteral(5)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Int64(1));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef("id".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::IntLiteral(10)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Int64(0));
    }
}
