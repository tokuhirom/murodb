/// Expression evaluator for WHERE clauses.
use crate::error::{MuroError, Result};
use crate::sql::ast::{BinaryOp, Expr, UnaryOp};
use crate::types::Value;

/// Evaluate an expression given a row's column values.
/// `columns` maps column name -> Value.
pub fn eval_expr(expr: &Expr, columns: &dyn Fn(&str) -> Option<Value>) -> Result<Value> {
    match expr {
        Expr::IntLiteral(n) => Ok(Value::Integer(*n)),
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
            for item in list {
                let item_val = eval_expr(item, columns)?;
                if !item_val.is_null()
                    && value_cmp(&val, &item_val) == Some(std::cmp::Ordering::Equal)
                {
                    found = true;
                    break;
                }
            }
            let result = if *negated { !found } else { found };
            Ok(Value::Integer(if result { 1 } else { 0 }))
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
                _ => Ok(Value::Integer(0)),
            }
        }
    }
}

fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
    match op {
        UnaryOp::Not => {
            if val.is_null() {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(if is_truthy(val) { 0 } else { 1 }))
            }
        }
        UnaryOp::Neg => match val {
            Value::Integer(n) => Ok(Value::Integer(-n)),
            Value::Null => Ok(Value::Null),
            _ => Err(MuroError::Execution(
                "Cannot negate non-numeric value".into(),
            )),
        },
    }
}

fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    // Handle arithmetic operators
    match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            return eval_arithmetic(left, op, right);
        }
        _ => {}
    }

    // Handle NULL comparisons
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::And => {
                // NULL AND FALSE = FALSE, NULL AND TRUE = NULL
                if matches!(left, Value::Integer(0)) || matches!(right, Value::Integer(0)) {
                    Ok(Value::Integer(0))
                } else {
                    Ok(Value::Null)
                }
            }
            BinaryOp::Or => {
                // NULL OR TRUE = TRUE
                if matches!(left, Value::Integer(n) if *n != 0)
                    || matches!(right, Value::Integer(n) if *n != 0)
                {
                    Ok(Value::Integer(1))
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Ok(Value::Null), // NULL comparison always NULL
        };
    }

    match op {
        BinaryOp::Eq => Ok(Value::Integer(
            if value_cmp(left, right) == Some(std::cmp::Ordering::Equal) {
                1
            } else {
                0
            },
        )),
        BinaryOp::Ne => Ok(Value::Integer(
            if value_cmp(left, right) != Some(std::cmp::Ordering::Equal) {
                1
            } else {
                0
            },
        )),
        BinaryOp::Lt => Ok(Value::Integer(
            if value_cmp(left, right) == Some(std::cmp::Ordering::Less) {
                1
            } else {
                0
            },
        )),
        BinaryOp::Gt => Ok(Value::Integer(
            if value_cmp(left, right) == Some(std::cmp::Ordering::Greater) {
                1
            } else {
                0
            },
        )),
        BinaryOp::Le => Ok(Value::Integer(
            if matches!(
                value_cmp(left, right),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) {
                1
            } else {
                0
            },
        )),
        BinaryOp::Ge => Ok(Value::Integer(
            if matches!(
                value_cmp(left, right),
                Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
            ) {
                1
            } else {
                0
            },
        )),
        BinaryOp::And => {
            let l = is_truthy(left);
            let r = is_truthy(right);
            Ok(Value::Integer(if l && r { 1 } else { 0 }))
        }
        BinaryOp::Or => {
            let l = is_truthy(left);
            let r = is_truthy(right);
            Ok(Value::Integer(if l || r { 1 } else { 0 }))
        }
        _ => unreachable!(),
    }
}

fn eval_arithmetic(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            let result = match op {
                BinaryOp::Add => a
                    .checked_add(*b)
                    .ok_or_else(|| MuroError::Execution("Integer overflow in addition".into()))?,
                BinaryOp::Sub => a.checked_sub(*b).ok_or_else(|| {
                    MuroError::Execution("Integer overflow in subtraction".into())
                })?,
                BinaryOp::Mul => a.checked_mul(*b).ok_or_else(|| {
                    MuroError::Execution("Integer overflow in multiplication".into())
                })?,
                BinaryOp::Div => {
                    if *b == 0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    a / b
                }
                BinaryOp::Mod => {
                    if *b == 0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    a % b
                }
                _ => unreachable!(),
            };
            Ok(Value::Integer(result))
        }
        _ => Err(MuroError::Execution(
            "Arithmetic operations require integer operands".into(),
        )),
    }
}

/// SQL LIKE pattern matching with % and _ wildcards.
fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&s_chars, &p_chars)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }

    match p[0] {
        '%' => {
            // % matches zero or more characters
            // Try matching the rest of the pattern at every position
            for i in 0..=s.len() {
                if like_match_inner(&s[i..], &p[1..]) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            if s.is_empty() {
                false
            } else {
                like_match_inner(&s[1..], &p[1..])
            }
        }
        c => {
            if s.is_empty() {
                false
            } else if s[0] == c {
                like_match_inner(&s[1..], &p[1..])
            } else {
                false
            }
        }
    }
}

fn value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
        (Value::Varbinary(a), Value::Varbinary(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Integer(n) => *n != 0,
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
