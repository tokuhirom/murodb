use crate::error::{MuroError, Result};
use crate::sql::ast::{BinaryOp, UnaryOp};
use crate::types::Value;

use super::compare::{is_truthy, value_cmp};

pub(super) fn eval_unary_op(op: UnaryOp, val: &Value) -> Result<Value> {
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
            Value::Float(n) => Ok(Value::Float(-n)),
            Value::Null => Ok(Value::Null),
            _ => Err(MuroError::Execution(
                "Cannot negate non-numeric value".into(),
            )),
        },
    }
}

pub(super) fn eval_binary_op(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
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
    if let (Some(a), Some(b)) = (left.as_f64(), right.as_f64()) {
        if matches!(left, Value::Float(_)) || matches!(right, Value::Float(_)) {
            let result = match op {
                BinaryOp::Add => a + b,
                BinaryOp::Sub => a - b,
                BinaryOp::Mul => a * b,
                BinaryOp::Div => {
                    if b == 0.0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    a / b
                }
                BinaryOp::Mod => {
                    if b == 0.0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    a % b
                }
                _ => unreachable!(),
            };
            return Ok(Value::Float(result));
        }
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
