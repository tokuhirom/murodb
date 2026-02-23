use crate::error::{MuroError, Result};
use crate::types::{
    format_date, format_datetime, format_uuid, parse_date_string, parse_datetime_string,
    parse_timestamp_string, parse_uuid_string, DataType, Value,
};
use rust_decimal::prelude::ToPrimitive;
use serde_json::Value as JsonValue;

pub(super) fn eval_cast(val: &Value, target_type: &DataType) -> Result<Value> {
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
            Value::Decimal(d) => {
                let truncated = d.trunc();
                truncated.to_i64().map(Value::Integer).ok_or_else(|| {
                    MuroError::Execution(format!("Decimal '{}' out of range for integer cast", d))
                })
            }
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
            Value::Decimal(d) => {
                let n = d.to_f64().ok_or_else(|| {
                    MuroError::Execution(format!("Cannot cast Decimal '{}' to float", d))
                })?;
                Ok(Value::Float(float_checked(n, target_type)?))
            }
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
        DataType::Decimal(p, s) => {
            let d = match val {
                Value::Integer(n) => rust_decimal::Decimal::from(*n),
                Value::Float(n) => {
                    use std::str::FromStr;
                    rust_decimal::Decimal::from_str(&n.to_string()).map_err(|_| {
                        MuroError::Execution(format!("Cannot cast float '{}' to DECIMAL", n))
                    })?
                }
                Value::Decimal(d) => *d,
                Value::Varchar(sv) => {
                    use std::str::FromStr;
                    rust_decimal::Decimal::from_str(sv.trim()).map_err(|_| {
                        MuroError::Execution(format!("Cannot cast '{}' to DECIMAL", sv))
                    })?
                }
                _ => {
                    return Err(MuroError::Execution(format!(
                        "Cannot cast {:?} to DECIMAL",
                        val
                    )))
                }
            };
            // Round to declared scale, set exact scale, and validate precision
            let mut rounded = d.round_dp(*s);
            rounded.rescale(*s);
            let max_int_digits = p - s;
            let int_part = rounded.trunc().abs();
            let int_digits = if int_part.is_zero() {
                0u32
            } else {
                int_part.to_string().len() as u32
            };
            if int_digits > max_int_digits {
                return Err(MuroError::Execution(format!(
                    "Value '{}' out of range for DECIMAL({},{})",
                    d, p, s
                )));
            }
            Ok(Value::Decimal(rounded))
        }
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
                Value::Uuid(b) => format_uuid(b),
                _ => val.to_string(),
            };
            Ok(Value::Varchar(s))
        }
        DataType::Jsonb => match val {
            Value::Varchar(s) => Ok(Value::Varchar(canonicalize_json_text(s)?)),
            Value::Varbinary(b) => {
                let s = std::str::from_utf8(b).map_err(|_| {
                    MuroError::Execution("Cannot cast non-UTF8 VARBINARY to JSONB".into())
                })?;
                Ok(Value::Varchar(canonicalize_json_text(s)?))
            }
            Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) | Value::Uuid(_) => {
                Ok(Value::Varchar(json_string_literal(&val.to_string())?))
            }
            _ => Ok(Value::Varchar(canonicalize_json_text(&val.to_string())?)),
        },
        DataType::Varbinary(_) => match val {
            Value::Varbinary(b) => Ok(Value::Varbinary(b.clone())),
            Value::Varchar(s) => Ok(Value::Varbinary(s.as_bytes().to_vec())),
            Value::Uuid(b) => Ok(Value::Varbinary(b.to_vec())),
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to varbinary",
                val
            ))),
        },
        DataType::Uuid => match val {
            Value::Uuid(b) => Ok(Value::Uuid(*b)),
            Value::Varchar(s) => {
                let bytes = parse_uuid_string(s)
                    .ok_or_else(|| MuroError::Execution(format!("Cannot cast '{}' to UUID", s)))?;
                Ok(Value::Uuid(bytes))
            }
            Value::Varbinary(b) => {
                if b.len() != 16 {
                    return Err(MuroError::Execution(format!(
                        "VARBINARY must be 16 bytes to cast to UUID, got {}",
                        b.len()
                    )));
                }
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(b);
                Ok(Value::Uuid(bytes))
            }
            _ => Err(MuroError::Execution(format!(
                "Cannot cast {:?} to UUID",
                val
            ))),
        },
    }
}

fn canonicalize_json_text(s: &str) -> Result<String> {
    let parsed: JsonValue = serde_json::from_str(s)
        .map_err(|e| MuroError::Execution(format!("Invalid JSON: {}", e)))?;
    serde_json::to_string(&parsed)
        .map_err(|e| MuroError::Execution(format!("Failed to canonicalize JSON: {}", e)))
}

fn json_string_literal(s: &str) -> Result<String> {
    serde_json::to_string(s)
        .map_err(|e| MuroError::Execution(format!("Failed to encode JSON string: {}", e)))
}
