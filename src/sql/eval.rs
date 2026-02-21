/// Expression evaluator for WHERE clauses.
use crate::error::{MuroError, Result};
use crate::sql::ast::{BinaryOp, Expr, UnaryOp};
use crate::types::{
    format_date, format_datetime, parse_date_string, parse_datetime_string, parse_timestamp_string,
    DataType, Value,
};

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
            Value::Float(n) => Ok(Value::Float(-n)),
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

fn eval_function_call(
    name: &str,
    args: &[Expr],
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Value> {
    match name {
        // NULL handling & conditional (these have special NULL semantics)
        "COALESCE" => {
            for arg in args {
                let val = eval_expr(arg, columns)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            check_args(name, args, 2)?;
            let a = eval_expr(&args[0], columns)?;
            if !a.is_null() {
                Ok(a)
            } else {
                eval_expr(&args[1], columns)
            }
        }
        "NULLIF" => {
            check_args(name, args, 2)?;
            let a = eval_expr(&args[0], columns)?;
            let b = eval_expr(&args[1], columns)?;
            if !a.is_null() && !b.is_null() && value_cmp(&a, &b) == Some(std::cmp::Ordering::Equal)
            {
                Ok(Value::Null)
            } else {
                Ok(a)
            }
        }
        "IF" => {
            check_args(name, args, 3)?;
            let cond = eval_expr(&args[0], columns)?;
            if is_truthy(&cond) {
                eval_expr(&args[1], columns)
            } else {
                eval_expr(&args[2], columns)
            }
        }

        // String functions (basic)
        "LENGTH" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            match &val {
                Value::Varchar(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Varbinary(b) => Ok(Value::Integer(b.len() as i64)),
                _ => Ok(Value::Null),
            }
        }
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            match &val {
                Value::Varchar(s) => Ok(Value::Integer(s.chars().count() as i64)),
                _ => Ok(Value::Null),
            }
        }
        "CONCAT" => {
            if args.is_empty() {
                return Err(MuroError::Execution(
                    "CONCAT requires at least 1 argument".into(),
                ));
            }
            let mut result = String::new();
            for arg in args {
                let val = eval_expr(arg, columns)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
                result.push_str(&val.to_string());
            }
            Ok(Value::Varchar(result))
        }
        "SUBSTRING" | "SUBSTR" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(MuroError::Execution(format!(
                    "{} requires 2 or 3 arguments",
                    name
                )));
            }
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let pos = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("SUBSTRING pos must be integer".into()))?;
            let chars: Vec<char> = s.chars().collect();
            // MySQL 1-based, pos can be negative
            let start = if pos > 0 {
                (pos - 1) as usize
            } else if pos < 0 {
                let from_end = (-pos) as usize;
                if from_end > chars.len() {
                    0
                } else {
                    chars.len() - from_end
                }
            } else {
                return Ok(Value::Varchar(String::new()));
            };
            if start >= chars.len() {
                return Ok(Value::Varchar(String::new()));
            }
            let len = if args.len() == 3 {
                vals[2]
                    .as_i64()
                    .ok_or_else(|| MuroError::Execution("SUBSTRING len must be integer".into()))?;
                let l = vals[2].as_i64().unwrap();
                if l < 0 {
                    return Ok(Value::Varchar(String::new()));
                }
                l as usize
            } else {
                chars.len() - start
            };
            let end = (start + len).min(chars.len());
            Ok(Value::Varchar(chars[start..end].iter().collect()))
        }
        "UPPER" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().to_uppercase()))
        }
        "LOWER" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().to_lowercase()))
        }

        // String functions (extended)
        "TRIM" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim().to_string()))
        }
        "LTRIM" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim_start().to_string()))
        }
        "RTRIM" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim_end().to_string()))
        }
        "REPLACE" => {
            check_args(name, args, 3)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let from = vals[1].to_string();
            let to = vals[2].to_string();
            Ok(Value::Varchar(s.replace(&from, &to)))
        }
        "REVERSE" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().chars().rev().collect()))
        }
        "REPEAT" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let n = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("REPEAT count must be integer".into()))?;
            if n < 0 {
                return Ok(Value::Varchar(String::new()));
            }
            Ok(Value::Varchar(s.repeat(n as usize)))
        }
        "LEFT" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let n = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("LEFT count must be integer".into()))?;
            if n < 0 {
                return Ok(Value::Varchar(String::new()));
            }
            Ok(Value::Varchar(s.chars().take(n as usize).collect()))
        }
        "RIGHT" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let n = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("RIGHT count must be integer".into()))?;
            if n < 0 {
                return Ok(Value::Varchar(String::new()));
            }
            let chars: Vec<char> = s.chars().collect();
            let skip = chars.len().saturating_sub(n as usize);
            Ok(Value::Varchar(chars[skip..].iter().collect()))
        }
        "LPAD" => {
            check_args(name, args, 3)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let len = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("LPAD len must be integer".into()))?
                as usize;
            let pad = vals[2].to_string();
            if s.chars().count() >= len {
                Ok(Value::Varchar(s.chars().take(len).collect()))
            } else if pad.is_empty() {
                Ok(Value::Varchar(s))
            } else {
                let need = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().collect();
                let mut prefix = String::new();
                for i in 0..need {
                    prefix.push(pad_chars[i % pad_chars.len()]);
                }
                prefix.push_str(&s);
                Ok(Value::Varchar(prefix))
            }
        }
        "RPAD" => {
            check_args(name, args, 3)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let len = vals[1]
                .as_i64()
                .ok_or_else(|| MuroError::Execution("RPAD len must be integer".into()))?
                as usize;
            let pad = vals[2].to_string();
            if s.chars().count() >= len {
                Ok(Value::Varchar(s.chars().take(len).collect()))
            } else if pad.is_empty() {
                Ok(Value::Varchar(s))
            } else {
                let need = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().collect();
                let mut result = s;
                for i in 0..need {
                    result.push(pad_chars[i % pad_chars.len()]);
                }
                Ok(Value::Varchar(result))
            }
        }
        "INSTR" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let sub = vals[1].to_string();
            match s.find(&sub) {
                Some(byte_pos) => {
                    let char_pos = s[..byte_pos].chars().count() + 1; // 1-based
                    Ok(Value::Integer(char_pos as i64))
                }
                None => Ok(Value::Integer(0)),
            }
        }
        "LOCATE" => {
            if args.len() < 2 || args.len() > 3 {
                return Err(MuroError::Execution(
                    "LOCATE requires 2 or 3 arguments".into(),
                ));
            }
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let sub = vals[0].to_string();
            let s = vals[1].to_string();
            let start_pos = if args.len() == 3 {
                vals[2]
                    .as_i64()
                    .ok_or_else(|| MuroError::Execution("LOCATE pos must be integer".into()))?
                    as usize
            } else {
                1
            };
            // Convert char position to byte position for searching
            let chars: Vec<char> = s.chars().collect();
            if start_pos < 1 || start_pos > chars.len() + 1 {
                return Ok(Value::Integer(0));
            }
            let byte_offset: usize = chars[..start_pos - 1].iter().map(|c| c.len_utf8()).sum();
            match s[byte_offset..].find(&sub) {
                Some(byte_pos) => {
                    let char_pos = s[..byte_offset + byte_pos].chars().count() + 1;
                    Ok(Value::Integer(char_pos as i64))
                }
                None => Ok(Value::Integer(0)),
            }
        }

        // REGEXP
        "REGEXP" | "REGEXP_LIKE" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let s = vals[0].to_string();
            let pattern = vals[1].to_string();
            let re = regex::Regex::new(&pattern)
                .map_err(|e| MuroError::Execution(format!("Invalid regex: {}", e)))?;
            Ok(Value::Integer(if re.is_match(&s) { 1 } else { 0 }))
        }

        // Numeric functions
        "ABS" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            match val {
                Value::Integer(n) => Ok(Value::Integer(n.abs())),
                Value::Float(n) => Ok(Value::Float(n.abs())),
                _ => Err(MuroError::Execution("ABS requires numeric argument".into())),
            }
        }
        "CEIL" | "CEILING" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            // Integer type: CEIL is identity
            match val {
                Value::Integer(n) => Ok(Value::Integer(n)),
                Value::Float(n) => Ok(Value::Float(n.ceil())),
                _ => Err(MuroError::Execution(
                    "CEIL requires numeric argument".into(),
                )),
            }
        }
        "FLOOR" => {
            check_args(name, args, 1)?;
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            match val {
                Value::Integer(n) => Ok(Value::Integer(n)),
                Value::Float(n) => Ok(Value::Float(n.floor())),
                _ => Err(MuroError::Execution(
                    "FLOOR requires numeric argument".into(),
                )),
            }
        }
        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return Err(MuroError::Execution(
                    "ROUND requires 1 or 2 arguments".into(),
                ));
            }
            let val = eval_expr(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            // Integer-only: ROUND is identity
            match val {
                Value::Integer(n) => Ok(Value::Integer(n)),
                Value::Float(n) => {
                    let scale = if args.len() == 2 {
                        eval_expr(&args[1], columns)?.as_i64().ok_or_else(|| {
                            MuroError::Execution("ROUND scale must be integer".into())
                        })?
                    } else {
                        0
                    };
                    let factor = 10f64.powi(scale as i32);
                    Ok(Value::Float((n * factor).round() / factor))
                }
                _ => Err(MuroError::Execution(
                    "ROUND requires numeric argument".into(),
                )),
            }
        }
        "MOD" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            match (&vals[0], &vals[1]) {
                (Value::Integer(a), Value::Integer(b)) => {
                    if *b == 0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    Ok(Value::Integer(a % b))
                }
                (a, b) if a.as_f64().is_some() && b.as_f64().is_some() => {
                    let a = a.as_f64().unwrap();
                    let b = b.as_f64().unwrap();
                    if b == 0.0 {
                        return Err(MuroError::Execution("Division by zero".into()));
                    }
                    Ok(Value::Float(a % b))
                }
                _ => Err(MuroError::Execution(
                    "MOD requires numeric arguments".into(),
                )),
            }
        }
        "POWER" | "POW" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            match (&vals[0], &vals[1]) {
                (Value::Integer(base), Value::Integer(exp)) => {
                    if *exp < 0 {
                        // Integer power with negative exponent → 0 (truncation)
                        Ok(Value::Integer(0))
                    } else {
                        let result = base.checked_pow(*exp as u32).ok_or_else(|| {
                            MuroError::Execution("Integer overflow in POWER".into())
                        })?;
                        Ok(Value::Integer(result))
                    }
                }
                (base, exp) if base.as_f64().is_some() && exp.as_f64().is_some() => Ok(
                    Value::Float(base.as_f64().unwrap().powf(exp.as_f64().unwrap())),
                ),
                _ => Err(MuroError::Execution(
                    "POWER requires numeric arguments".into(),
                )),
            }
        }

        _ => Err(MuroError::Execution(format!("Unknown function: {}", name))),
    }
}

fn check_args(name: &str, args: &[Expr], expected: usize) -> Result<()> {
    if args.len() != expected {
        Err(MuroError::Execution(format!(
            "{} requires {} argument(s), got {}",
            name,
            expected,
            args.len()
        )))
    } else {
        Ok(())
    }
}

/// Evaluate all args and return None if any is NULL.
fn eval_args_null_check(
    args: &[Expr],
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Option<Vec<Value>>> {
    let mut vals = Vec::with_capacity(args.len());
    for arg in args {
        let val = eval_expr(arg, columns)?;
        if val.is_null() {
            return Ok(None);
        }
        vals.push(val);
    }
    Ok(Some(vals))
}

fn eval_case_when(
    operand: &Option<Box<Expr>>,
    when_clauses: &[(Expr, Expr)],
    else_clause: &Option<Box<Expr>>,
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Value> {
    match operand {
        Some(op_expr) => {
            // Simple CASE: CASE expr WHEN val THEN result ...
            let op_val = eval_expr(op_expr, columns)?;
            for (when_expr, then_expr) in when_clauses {
                let when_val = eval_expr(when_expr, columns)?;
                if !op_val.is_null()
                    && !when_val.is_null()
                    && value_cmp(&op_val, &when_val) == Some(std::cmp::Ordering::Equal)
                {
                    return eval_expr(then_expr, columns);
                }
            }
        }
        None => {
            // Searched CASE: CASE WHEN condition THEN result ...
            for (cond_expr, then_expr) in when_clauses {
                let cond_val = eval_expr(cond_expr, columns)?;
                if is_truthy(&cond_val) {
                    return eval_expr(then_expr, columns);
                }
            }
        }
    }
    match else_clause {
        Some(else_expr) => eval_expr(else_expr, columns),
        None => Ok(Value::Null),
    }
}

fn eval_cast(val: &Value, target_type: &DataType) -> Result<Value> {
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0; // -2^63
    const I64_UPPER_EXCLUSIVE_F64: f64 = 9_223_372_036_854_775_808.0; // 2^63

    fn float_to_i64_checked(n: f64) -> Result<i64> {
        if !n.is_finite() {
            return Err(MuroError::Execution(format!(
                "Cannot cast non-finite float '{}' to integer",
                n
            )));
        }
        if !(I64_MIN_F64..I64_UPPER_EXCLUSIVE_F64).contains(&n) {
            return Err(MuroError::Execution(format!(
                "Float '{}' out of range for integer cast",
                n
            )));
        }
        Ok(n as i64)
    }

    fn float_checked(n: f64, target_type: &DataType) -> Result<f64> {
        if !n.is_finite() {
            return Err(MuroError::Execution(format!(
                "Cannot cast non-finite float '{}' to {}",
                n, target_type
            )));
        }
        if *target_type == DataType::Float && (n < f32::MIN as f64 || n > f32::MAX as f64) {
            return Err(MuroError::Execution(format!(
                "Float '{}' out of range for FLOAT",
                n
            )));
        }
        Ok(n)
    }

    if val.is_null() {
        return Ok(Value::Null);
    }
    match target_type {
        DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => match val {
            Value::Integer(n) => Ok(Value::Integer(*n)),
            Value::Float(n) => Ok(Value::Integer(float_to_i64_checked(*n)?)),
            Value::Varchar(s) => {
                let n: i64 = s
                    .trim()
                    .parse()
                    .map_err(|_| MuroError::Execution(format!("Cannot cast '{}' to integer", s)))?;
                Ok(Value::Integer(n))
            }
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to integer",
                val
            ))),
        },
        DataType::Float | DataType::Double => match val {
            Value::Integer(n) => Ok(Value::Float(float_checked(*n as f64, target_type)?)),
            Value::Float(n) => Ok(Value::Float(float_checked(*n, target_type)?)),
            Value::Varchar(s) => {
                let n: f64 = s
                    .trim()
                    .parse()
                    .map_err(|_| MuroError::Execution(format!("Cannot cast '{}' to float", s)))?;
                Ok(Value::Float(float_checked(n, target_type)?))
            }
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to float",
                val
            ))),
        },
        DataType::Date => match val {
            Value::Date(d) => Ok(Value::Date(*d)),
            Value::DateTime(dt) => Ok(Value::Date((*dt / 1_000_000) as i32)),
            Value::Timestamp(ts) => Ok(Value::Date((*ts / 1_000_000) as i32)),
            Value::Varchar(s) => parse_date_string(s)
                .map(Value::Date)
                .ok_or_else(|| MuroError::Execution(format!("Cannot cast '{}' to DATE", s))),
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to DATE",
                val
            ))),
        },
        DataType::DateTime => match val {
            Value::Date(d) => Ok(Value::DateTime((*d as i64) * 1_000_000)),
            Value::DateTime(dt) => Ok(Value::DateTime(*dt)),
            Value::Timestamp(ts) => Ok(Value::DateTime(*ts)),
            Value::Varchar(s) => parse_datetime_string(s)
                .map(Value::DateTime)
                .ok_or_else(|| MuroError::Execution(format!("Cannot cast '{}' to DATETIME", s))),
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to DATETIME",
                val
            ))),
        },
        DataType::Timestamp => match val {
            Value::Date(d) => Ok(Value::Timestamp((*d as i64) * 1_000_000)),
            Value::DateTime(dt) => Ok(Value::Timestamp(*dt)),
            Value::Timestamp(ts) => Ok(Value::Timestamp(*ts)),
            Value::Varchar(s) => parse_timestamp_string(s)
                .map(Value::Timestamp)
                .ok_or_else(|| MuroError::Execution(format!("Cannot cast '{}' to TIMESTAMP", s))),
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to TIMESTAMP",
                val
            ))),
        },
        DataType::Varchar(_) | DataType::Text => {
            let s = match val {
                Value::Date(d) => format_date(*d),
                Value::DateTime(dt) => format_datetime(*dt),
                Value::Timestamp(ts) => format_datetime(*ts),
                _ => val.to_string(),
            };
            Ok(Value::Varchar(s))
        }
        DataType::Varbinary(_) => match val {
            Value::Varbinary(b) => Ok(Value::Varbinary(b.clone())),
            Value::Varchar(s) => Ok(Value::Varbinary(s.as_bytes().to_vec())),
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to varbinary",
                val
            ))),
        },
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
    fn cmp_i64_f64(i: i64, f: f64) -> Option<std::cmp::Ordering> {
        if f.is_nan() {
            return None;
        }
        if f >= i64::MAX as f64 {
            return Some(std::cmp::Ordering::Less);
        }
        if f < i64::MIN as f64 {
            return Some(std::cmp::Ordering::Greater);
        }

        let t = f.trunc() as i64;
        if i < t {
            return Some(std::cmp::Ordering::Less);
        }
        if i > t {
            return Some(std::cmp::Ordering::Greater);
        }

        let frac = f.fract();
        if frac > 0.0 {
            Some(std::cmp::Ordering::Less)
        } else if frac < 0.0 {
            Some(std::cmp::Ordering::Greater)
        } else {
            Some(std::cmp::Ordering::Equal)
        }
    }

    match (a, b) {
        (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Float(b)) => cmp_i64_f64(*a, *b),
        (Value::Float(a), Value::Integer(b)) => cmp_i64_f64(*b, *a).map(|o| o.reverse()),
        (Value::Varchar(a), Value::Varchar(b)) => Some(a.cmp(b)),
        (Value::Varbinary(a), Value::Varbinary(b)) => Some(a.cmp(b)),
        (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
        (Value::DateTime(a), Value::DateTime(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Date(a), Value::DateTime(b)) => Some(((*a as i64) * 1_000_000).cmp(b)),
        (Value::DateTime(a), Value::Date(b)) => Some(a.cmp(&((*b as i64) * 1_000_000))),
        (Value::Date(a), Value::Timestamp(b)) => Some(((*a as i64) * 1_000_000).cmp(b)),
        (Value::Timestamp(a), Value::Date(b)) => Some(a.cmp(&((*b as i64) * 1_000_000))),
        (Value::DateTime(a), Value::Timestamp(b)) => Some(a.cmp(b)),
        (Value::Timestamp(a), Value::DateTime(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Integer(n) => *n != 0,
        Value::Float(n) => *n != 0.0,
        Value::Varchar(s) => !s.is_empty(),
        Value::Varbinary(b) => !b.is_empty(),
        Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) => true,
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
