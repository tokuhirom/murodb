use crate::error::{MuroError, Result};
use crate::types::{
    format_date, format_datetime, parse_date_string, parse_datetime_string, parse_timestamp_string,
    DataType, Value,
};

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
