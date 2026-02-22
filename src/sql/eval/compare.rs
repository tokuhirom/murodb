use crate::types::Value;

pub(super) fn value_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
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
