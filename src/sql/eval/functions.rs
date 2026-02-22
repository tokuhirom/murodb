use crate::error::{MuroError, Result};
use crate::sql::ast::Expr;
use crate::types::{parse_date_string, parse_timestamp_string, Value};
use std::time::{SystemTime, UNIX_EPOCH};

use super::compare::{is_truthy, value_cmp};
use super::eval_expr;

type EvalFn<'a> = dyn Fn(&Expr, &dyn Fn(&str) -> Option<Value>) -> Result<Value> + 'a;

pub(super) fn eval_function_call(
    name: &str,
    args: &[Expr],
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Value> {
    eval_function_call_with(name, args, columns, &|expr, cols| eval_expr(expr, cols))
}

pub(super) fn eval_function_call_with(
    name: &str,
    args: &[Expr],
    columns: &dyn Fn(&str) -> Option<Value>,
    eval_fn: &EvalFn<'_>,
) -> Result<Value> {
    match name {
        // Date/time functions
        "NOW" | "CURRENT_TIMESTAMP" => {
            check_args(name, args, 0)?;
            Ok(Value::DateTime(current_utc_datetime_packed()?))
        }
        "DATE_FORMAT" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns, eval_fn)?;
            let vals = match vals {
                Some(v) => v,
                None => return Ok(Value::Null),
            };
            let format = vals[1].to_string();
            match format_datetime_with_mysql_spec(&vals[0], &format) {
                Some(s) => Ok(Value::Varchar(s)),
                None => Ok(Value::Null),
            }
        }

        // NULL handling & conditional (these have special NULL semantics)
        "COALESCE" => {
            for arg in args {
                let val = eval_fn(arg, columns)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            check_args(name, args, 2)?;
            let a = eval_fn(&args[0], columns)?;
            if !a.is_null() {
                Ok(a)
            } else {
                eval_fn(&args[1], columns)
            }
        }
        "NULLIF" => {
            check_args(name, args, 2)?;
            let a = eval_fn(&args[0], columns)?;
            let b = eval_fn(&args[1], columns)?;
            if !a.is_null() && !b.is_null() && value_cmp(&a, &b) == Some(std::cmp::Ordering::Equal)
            {
                Ok(Value::Null)
            } else {
                Ok(a)
            }
        }
        "IF" => {
            check_args(name, args, 3)?;
            let cond = eval_fn(&args[0], columns)?;
            if is_truthy(&cond) {
                eval_fn(&args[1], columns)
            } else {
                eval_fn(&args[2], columns)
            }
        }

        // String functions (basic)
        "LENGTH" => {
            check_args(name, args, 1)?;
            let val = eval_fn(&args[0], columns)?;
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
            let val = eval_fn(&args[0], columns)?;
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
                let val = eval_fn(arg, columns)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().to_uppercase()))
        }
        "LOWER" => {
            check_args(name, args, 1)?;
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().to_lowercase()))
        }

        // String functions (extended)
        "TRIM" => {
            check_args(name, args, 1)?;
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim().to_string()))
        }
        "LTRIM" => {
            check_args(name, args, 1)?;
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim_start().to_string()))
        }
        "RTRIM" => {
            check_args(name, args, 1)?;
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().trim_end().to_string()))
        }
        "REPLACE" => {
            check_args(name, args, 3)?;
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            Ok(Value::Varchar(val.to_string().chars().rev().collect()))
        }
        "REPEAT" => {
            check_args(name, args, 2)?;
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let val = eval_fn(&args[0], columns)?;
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
            let val = eval_fn(&args[0], columns)?;
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
            let val = eval_fn(&args[0], columns)?;
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
            let val = eval_fn(&args[0], columns)?;
            if val.is_null() {
                return Ok(Value::Null);
            }
            // Integer-only: ROUND is identity
            match val {
                Value::Integer(n) => Ok(Value::Integer(n)),
                Value::Float(n) => {
                    let scale = if args.len() == 2 {
                        eval_fn(&args[1], columns)?.as_i64().ok_or_else(|| {
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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
            let vals = eval_args_null_check(args, columns, eval_fn)?;
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

fn current_utc_datetime_packed() -> Result<i64> {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MuroError::Execution("System time is before UNIX_EPOCH".into()))?
        .as_secs() as i64;
    unix_to_datetime(unix).ok_or_else(|| {
        MuroError::Execution("Current system time is outside supported DATETIME range".into())
    })
}

fn format_datetime_with_mysql_spec(value: &Value, format: &str) -> Option<String> {
    let (y, m, d, hh, mm, ss) = extract_datetime_parts(value)?;

    let month_names = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    let month_abbr = [
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let weekday_names = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    let weekday_abbr = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let weekday = weekday_sunday0(y, m, d) as usize;
    let day_of_year = day_of_year(y, m, d);
    let hour12 = match hh % 12 {
        0 => 12,
        v => v,
    };
    let ampm = if hh < 12 { "AM" } else { "PM" };

    let mut out = String::with_capacity(format.len() + 16);
    let mut chars = format.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            out.push('%');
            break;
        };
        match spec {
            'Y' => out.push_str(&format!("{:04}", y)),
            'y' => out.push_str(&format!("{:02}", y % 100)),
            'm' => out.push_str(&format!("{:02}", m)),
            'c' => out.push_str(&m.to_string()),
            'M' => out.push_str(month_names[m as usize]),
            'b' => out.push_str(month_abbr[m as usize]),
            'd' => out.push_str(&format!("{:02}", d)),
            'e' => out.push_str(&d.to_string()),
            'H' => out.push_str(&format!("{:02}", hh)),
            'k' => out.push_str(&hh.to_string()),
            'h' | 'I' => out.push_str(&format!("{:02}", hour12)),
            'l' => out.push_str(&hour12.to_string()),
            'i' => out.push_str(&format!("{:02}", mm)),
            's' | 'S' => out.push_str(&format!("{:02}", ss)),
            'T' => out.push_str(&format!("{:02}:{:02}:{:02}", hh, mm, ss)),
            'r' => out.push_str(&format!("{:02}:{:02}:{:02} {}", hour12, mm, ss, ampm)),
            'p' => out.push_str(ampm),
            'W' => out.push_str(weekday_names[weekday]),
            'a' => out.push_str(weekday_abbr[weekday]),
            'w' => out.push_str(&weekday.to_string()),
            'j' => out.push_str(&format!("{:03}", day_of_year)),
            'f' => out.push_str("000000"), // microseconds are not stored
            '%' => out.push('%'),
            other => out.push(other),
        }
    }
    Some(out)
}

fn extract_datetime_parts(value: &Value) -> Option<(i32, i32, i32, i32, i32, i32)> {
    match value {
        Value::Date(d) => Some(unpack_date(*d)),
        Value::DateTime(dt) | Value::Timestamp(dt) => Some(unpack_datetime(*dt)),
        Value::Varchar(s) => {
            if let Some(dt) = parse_timestamp_string(s) {
                Some(unpack_datetime(dt))
            } else {
                parse_date_string(s).map(unpack_date)
            }
        }
        _ => None,
    }
}

fn unpack_date(packed: i32) -> (i32, i32, i32, i32, i32, i32) {
    let y = packed / 10000;
    let m = (packed / 100) % 100;
    let d = packed % 100;
    (y, m, d, 0, 0, 0)
}

fn unpack_datetime(packed: i64) -> (i32, i32, i32, i32, i32, i32) {
    let y = (packed / 10000000000) as i32;
    let m = ((packed / 100000000) % 100) as i32;
    let d = ((packed / 1000000) % 100) as i32;
    let hh = ((packed / 10000) % 100) as i32;
    let mm = ((packed / 100) % 100) as i32;
    let ss = (packed % 100) as i32;
    (y, m, d, hh, mm, ss)
}

fn day_of_year(y: i32, m: i32, d: i32) -> i32 {
    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut total = 0;
    for month in 1..m {
        total += month_days[(month - 1) as usize];
        if month == 2 && is_leap_year(y) {
            total += 1;
        }
    }
    total + d
}

fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn weekday_sunday0(y: i32, m: i32, d: i32) -> i32 {
    let days = days_from_civil(y, m, d);
    (((days + 4) % 7 + 7) % 7) as i32
}

fn unix_to_datetime(unix: i64) -> Option<i64> {
    let days = unix.div_euclid(86_400);
    let sod = unix.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    if !(1..=9999).contains(&y) {
        return None;
    }
    let hh = sod / 3_600;
    let mm = (sod % 3_600) / 60;
    let ss = sod % 60;
    Some(
        (y as i64) * 10000000000
            + (m as i64) * 100000000
            + (d as i64) * 1000000
            + hh * 10000
            + mm * 100
            + ss,
    )
}

fn days_from_civil(y: i32, m: i32, d: i32) -> i64 {
    let y = y as i64 - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = m as i64;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i32, i32, i32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y as i32, m as i32, d as i32)
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
    eval_fn: &EvalFn<'_>,
) -> Result<Option<Vec<Value>>> {
    let mut vals = Vec::with_capacity(args.len());
    for arg in args {
        let val = eval_fn(arg, columns)?;
        if val.is_null() {
            return Ok(None);
        }
        vals.push(val);
    }
    Ok(Some(vals))
}

pub(super) fn eval_case_when(
    operand: &Option<Box<Expr>>,
    when_clauses: &[(Expr, Expr)],
    else_clause: &Option<Box<Expr>>,
    columns: &dyn Fn(&str) -> Option<Value>,
) -> Result<Value> {
    eval_case_when_with(
        operand,
        when_clauses,
        else_clause,
        columns,
        &|expr, cols| eval_expr(expr, cols),
    )
}

pub(super) fn eval_case_when_with(
    operand: &Option<Box<Expr>>,
    when_clauses: &[(Expr, Expr)],
    else_clause: &Option<Box<Expr>>,
    columns: &dyn Fn(&str) -> Option<Value>,
    eval_fn: &EvalFn<'_>,
) -> Result<Value> {
    match operand {
        Some(op_expr) => {
            // Simple CASE: CASE expr WHEN val THEN result ...
            for (when_expr, then_expr) in when_clauses {
                let cmp_expr = Expr::BinaryOp {
                    left: op_expr.clone(),
                    op: crate::sql::ast::BinaryOp::Eq,
                    right: Box::new(when_expr.clone()),
                };
                let cond_val = eval_fn(&cmp_expr, columns)?;
                if is_truthy(&cond_val) {
                    return eval_fn(then_expr, columns);
                }
            }
        }
        None => {
            // Searched CASE: CASE WHEN condition THEN result ...
            for (cond_expr, then_expr) in when_clauses {
                let cond_val = eval_fn(cond_expr, columns)?;
                if is_truthy(&cond_val) {
                    return eval_fn(then_expr, columns);
                }
            }
        }
    }
    match else_clause {
        Some(else_expr) => eval_fn(else_expr, columns),
        None => Ok(Value::Null),
    }
}
