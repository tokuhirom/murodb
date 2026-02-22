/// Expression evaluator for WHERE clauses.
use crate::error::{MuroError, Result};
use crate::sql::ast::Expr;
use crate::types::Value;

mod cast;
mod compare;
mod functions;
mod ops;
mod pattern;

use cast::eval_cast;
pub use compare::is_truthy;
use compare::value_cmp;
use functions::{eval_case_when, eval_function_call};
use ops::{eval_binary_op, eval_unary_op};
use pattern::like_match;

/// Evaluate an expression given a row's column values.
/// `columns` maps column name -> Value.
pub fn eval_expr(expr: &Expr, columns: &dyn Fn(&str) -> Option<Value>) -> Result<Value> {
    match expr {
        Expr::IntLiteral(n) => Ok(Value::Integer(*n)),
        Expr::FloatLiteral(n) => Ok(Value::Float(*n)),
        Expr::StringLiteral(s) => Ok(Value::Varchar(s.clone())),
        Expr::BlobLiteral(b) => Ok(Value::Varbinary(b.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::DefaultValue => Ok(Value::Null), // handled by executor before eval

        Expr::ColumnRef(name) => {
            columns(name).ok_or_else(|| MuroError::Execution(format!("Unknown column: {}", name)))
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, columns)?;
            let rval = eval_expr(right, columns)?;
            eval_binary_op(&lval, *op, &rval)
        }

        Expr::UnaryOp { op, operand } => {
            let val = eval_expr(operand, columns)?;
            eval_unary_op(*op, &val)
        }

        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let val = eval_expr(expr, columns)?;
            let pat = eval_expr(pattern, columns)?;
            match (&val, &pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Varchar(s), Value::Varchar(p)) => {
                    let matches = like_match(s, p);
                    let result = if *negated { !matches } else { matches };
                    Ok(Value::Integer(if result { 1 } else { 0 }))
                }
                _ => Ok(Value::Integer(0)),
            }
        }

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let val = eval_expr(expr, columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            let mut has_null = false;
            for item in list {
                let item_val = eval_expr(item, columns)?;
                if item_val.is_null() {
                    has_null = true;
                    continue;
                }
                if value_cmp(&val, &item_val) == Some(std::cmp::Ordering::Equal) {
                    found = true;
                    break;
                }
            }
            if found {
                // IN → TRUE, NOT IN → FALSE
                Ok(Value::Integer(if *negated { 0 } else { 1 }))
            } else if has_null {
                // No match but NULL in list → UNKNOWN (NULL)
                // SQL standard: IN → NULL, NOT IN → NULL
                Ok(Value::Null)
            } else {
                // No match, no NULLs → IN → FALSE, NOT IN → TRUE
                Ok(Value::Integer(if *negated { 1 } else { 0 }))
            }
        }

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let val = eval_expr(expr, columns)?;
            let low_val = eval_expr(low, columns)?;
            let high_val = eval_expr(high, columns)?;
            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Value::Null);
            }
            let ge_low = matches!(
                value_cmp(&val, &low_val),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            );
            let le_high = matches!(
                value_cmp(&val, &high_val),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            );
            let in_range = ge_low && le_high;
            let result = if *negated { !in_range } else { in_range };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }

        Expr::IsNull { expr, negated } => {
            let val = eval_expr(expr, columns)?;
            let is_null = val.is_null();
            let result = if *negated { !is_null } else { is_null };
            Ok(Value::Integer(if result { 1 } else { 0 }))
        }

        Expr::FunctionCall { name, args } => eval_function_call(name, args, columns),

        Expr::CaseWhen {
            operand,
            when_clauses,
            else_clause,
        } => eval_case_when(operand, when_clauses, else_clause, columns),

        Expr::Cast { expr, target_type } => {
            let val = eval_expr(expr, columns)?;
            eval_cast(&val, target_type)
        }

        Expr::AggregateFunc { .. } => {
            // Aggregate functions are evaluated by the executor's aggregation pipeline,
            // not by eval_expr. If we reach here, the aggregate value should have been
            // substituted by the executor already.
            Err(MuroError::Execution(
                "Aggregate function used outside of aggregation context".into(),
            ))
        }

        Expr::MatchAgainst { .. } => {
            // FTS scoring - actual evaluation happens in the executor
            Ok(Value::Integer(0))
        }

        Expr::FtsSnippet { .. } => {
            // FTS snippet - handled in executor
            Ok(Value::Varchar(String::new()))
        }

        Expr::GreaterThanZero(inner) => {
            let val = eval_expr(inner, columns)?;
            match val {
                Value::Integer(n) => Ok(Value::Integer(if n > 0 { 1 } else { 0 })),
                Value::Float(n) => Ok(Value::Integer(if n > 0.0 { 1 } else { 0 })),
                _ => Ok(Value::Integer(0)),
            }
        }

        // Subquery variants should be materialized before eval_expr is called
        Expr::InSubquery { .. } | Expr::Exists { .. } | Expr::ScalarSubquery(_) => Err(
            MuroError::Execution("Subquery not materialized before evaluation".into()),
        ),
    }
}

/// Evaluate an expression with optional collation-aware comparison/LIKE semantics.
/// `resolve_collation(left, right)` may return a collation name for binary comparisons and LIKE.
pub fn eval_expr_with_collation(
    expr: &Expr,
    columns: &dyn Fn(&str) -> Option<Value>,
    resolve_collation: &dyn Fn(&Expr, &Expr) -> Result<Option<String>>,
) -> Result<Value> {
    match expr {
        Expr::IntLiteral(n) => Ok(Value::Integer(*n)),
        Expr::FloatLiteral(n) => Ok(Value::Float(*n)),
        Expr::StringLiteral(s) => Ok(Value::Varchar(s.clone())),
        Expr::BlobLiteral(b) => Ok(Value::Varbinary(b.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::DefaultValue => Ok(Value::Null),

        Expr::ColumnRef(name) => {
            columns(name).ok_or_else(|| MuroError::Execution(format!("Unknown column: {}", name)))
        }

        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr_with_collation(left, columns, resolve_collation)?;
            let rval = eval_expr_with_collation(right, columns, resolve_collation)?;
            match op {
                crate::sql::ast::BinaryOp::Eq
                | crate::sql::ast::BinaryOp::Ne
                | crate::sql::ast::BinaryOp::Lt
                | crate::sql::ast::BinaryOp::Gt
                | crate::sql::ast::BinaryOp::Le
                | crate::sql::ast::BinaryOp::Ge => {
                    if lval.is_null() || rval.is_null() {
                        return Ok(Value::Null);
                    }
                    let collation = resolve_collation(left, right)?;
                    let ord = value_cmp_with_collation(&lval, &rval, collation.as_deref())?;
                    let out = match op {
                        crate::sql::ast::BinaryOp::Eq => ord == Some(std::cmp::Ordering::Equal),
                        crate::sql::ast::BinaryOp::Ne => ord != Some(std::cmp::Ordering::Equal),
                        crate::sql::ast::BinaryOp::Lt => ord == Some(std::cmp::Ordering::Less),
                        crate::sql::ast::BinaryOp::Gt => ord == Some(std::cmp::Ordering::Greater),
                        crate::sql::ast::BinaryOp::Le => {
                            matches!(
                                ord,
                                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                            )
                        }
                        crate::sql::ast::BinaryOp::Ge => matches!(
                            ord,
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                        ),
                        _ => unreachable!(),
                    };
                    Ok(Value::Integer(if out { 1 } else { 0 }))
                }
                _ => eval_binary_op(&lval, *op, &rval),
            }
        }

        Expr::UnaryOp { op, operand } => {
            let val = eval_expr_with_collation(operand, columns, resolve_collation)?;
            eval_unary_op(*op, &val)
        }

        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let val = eval_expr_with_collation(expr, columns, resolve_collation)?;
            let pat = eval_expr_with_collation(pattern, columns, resolve_collation)?;
            match (&val, &pat) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Varchar(s), Value::Varchar(p)) => {
                    let collation = resolve_collation(expr, pattern)?;
                    let matches = match collation.as_deref() {
                        None => like_match(s, p),
                        Some(name) if name.eq_ignore_ascii_case("binary") => like_match(s, p),
                        Some(name) if name.eq_ignore_ascii_case("nocase") => {
                            like_match(&ascii_fold_nocase(s), &ascii_fold_nocase(p))
                        }
                        Some(other) => {
                            return Err(MuroError::Execution(format!(
                                "Unsupported collation '{}' in LIKE: currently only binary and nocase are supported",
                                other
                            )));
                        }
                    };
                    let result = if *negated { !matches } else { matches };
                    Ok(Value::Integer(if result { 1 } else { 0 }))
                }
                _ => Ok(Value::Integer(0)),
            }
        }

        Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::IsNull { .. }
        | Expr::FunctionCall { .. }
        | Expr::CaseWhen { .. }
        | Expr::Cast { .. }
        | Expr::AggregateFunc { .. }
        | Expr::MatchAgainst { .. }
        | Expr::FtsSnippet { .. }
        | Expr::GreaterThanZero(_)
        | Expr::InSubquery { .. }
        | Expr::Exists { .. }
        | Expr::ScalarSubquery(_) => eval_expr(expr, columns),
    }
}

fn ascii_fold_nocase(s: &str) -> String {
    let folded: Vec<u8> = s.bytes().map(|b| b.to_ascii_lowercase()).collect();
    String::from_utf8(folded).expect("ASCII lowercasing preserves UTF-8 validity")
}

fn value_cmp_with_collation(
    left: &Value,
    right: &Value,
    collation: Option<&str>,
) -> Result<Option<std::cmp::Ordering>> {
    match collation {
        None => Ok(value_cmp(left, right)),
        Some(name) if name.eq_ignore_ascii_case("binary") => Ok(value_cmp(left, right)),
        Some(name) if name.eq_ignore_ascii_case("nocase") => match (left, right) {
            (Value::Varchar(a), Value::Varchar(b)) => {
                let a = ascii_fold_nocase(a);
                let b = ascii_fold_nocase(b);
                Ok(Some(a.cmp(&b)))
            }
            _ => Ok(value_cmp(left, right)),
        },
        Some(other) => Err(MuroError::Execution(format!(
            "Unsupported collation '{}' in comparison: currently only binary and nocase are supported",
            other
        ))),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{BinaryOp, UnaryOp};
    use crate::types::DataType;

    #[test]
    fn test_eval_literals() {
        let lookup = |_: &str| -> Option<Value> { None };
        assert_eq!(
            eval_expr(&Expr::IntLiteral(42), &lookup).unwrap(),
            Value::Integer(42)
        );
        assert_eq!(
            eval_expr(&Expr::StringLiteral("hello".into()), &lookup).unwrap(),
            Value::Varchar("hello".into())
        );
    }

    #[test]
    fn test_eval_comparison() {
        let lookup = |name: &str| -> Option<Value> {
            match name {
                "id" => Some(Value::Integer(5)),
                _ => None,
            }
        };

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::IntLiteral(5)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::ColumnRef("id".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::IntLiteral(10)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_cast_non_finite_float_to_integer_is_error() {
        let err = eval_cast(&Value::Float(f64::NAN), &DataType::BigInt).unwrap_err();
        assert!(format!("{err}").contains("non-finite"));
    }

    #[test]
    fn test_cast_out_of_range_float_to_integer_is_error() {
        let err = eval_cast(
            &Value::Float(9_223_372_036_854_775_808.0),
            &DataType::BigInt,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("out of range"));
    }

    #[test]
    fn test_value_cmp_large_int_vs_float_not_equal() {
        let i = Value::Integer(9_007_199_254_740_993);
        let f = Value::Float(9_007_199_254_740_992.0);
        assert_eq!(value_cmp(&i, &f), Some(std::cmp::Ordering::Greater));
        assert_eq!(value_cmp(&f, &i), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn test_eval_like() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::Like {
            expr: Box::new(Expr::StringLiteral("hello world".into())),
            pattern: Box::new(Expr::StringLiteral("%world".into())),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::Like {
            expr: Box::new(Expr::StringLiteral("hello".into())),
            pattern: Box::new(Expr::StringLiteral("h_llo".into())),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::Like {
            expr: Box::new(Expr::StringLiteral("hello".into())),
            pattern: Box::new(Expr::StringLiteral("world".into())),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_eval_in_list() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::InList {
            expr: Box::new(Expr::IntLiteral(2)),
            list: vec![
                Expr::IntLiteral(1),
                Expr::IntLiteral(2),
                Expr::IntLiteral(3),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::InList {
            expr: Box::new(Expr::IntLiteral(5)),
            list: vec![
                Expr::IntLiteral(1),
                Expr::IntLiteral(2),
                Expr::IntLiteral(3),
            ],
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_eval_between() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::Between {
            expr: Box::new(Expr::IntLiteral(5)),
            low: Box::new(Expr::IntLiteral(1)),
            high: Box::new(Expr::IntLiteral(10)),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::Between {
            expr: Box::new(Expr::IntLiteral(15)),
            low: Box::new(Expr::IntLiteral(1)),
            high: Box::new(Expr::IntLiteral(10)),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_eval_is_null() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::IsNull {
            expr: Box::new(Expr::Null),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));

        let expr = Expr::IsNull {
            expr: Box::new(Expr::IntLiteral(5)),
            negated: false,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));

        let expr = Expr::IsNull {
            expr: Box::new(Expr::IntLiteral(5)),
            negated: true,
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_eval_not() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expr::IntLiteral(1)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(0));

        let expr = Expr::UnaryOp {
            op: UnaryOp::Not,
            operand: Box::new(Expr::IntLiteral(0)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_eval_arithmetic() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Add,
            right: Box::new(Expr::IntLiteral(3)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(13));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Sub,
            right: Box::new(Expr::IntLiteral(3)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(7));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Mul,
            right: Box::new(Expr::IntLiteral(3)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(30));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Div,
            right: Box::new(Expr::IntLiteral(3)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(3));

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Mod,
            right: Box::new(Expr::IntLiteral(3)),
        };
        assert_eq!(eval_expr(&expr, &lookup).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_eval_division_by_zero() {
        let lookup = |_: &str| -> Option<Value> { None };

        let expr = Expr::BinaryOp {
            left: Box::new(Expr::IntLiteral(10)),
            op: BinaryOp::Div,
            right: Box::new(Expr::IntLiteral(0)),
        };
        assert!(eval_expr(&expr, &lookup).is_err());
    }

    #[test]
    fn test_like_patterns() {
        assert!(like_match("hello", "hello"));
        assert!(like_match("hello", "%"));
        assert!(like_match("hello", "h%"));
        assert!(like_match("hello", "%o"));
        assert!(like_match("hello", "%ll%"));
        assert!(like_match("hello", "h_llo"));
        assert!(like_match("hello", "_____"));
        assert!(!like_match("hello", "______"));
        assert!(!like_match("hello", "world"));
        assert!(like_match("", ""));
        assert!(like_match("", "%"));
        assert!(!like_match("", "_"));
    }
}
