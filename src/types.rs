use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Integer(i64),
    Float(f64),
    Date(i32),      // YYYYMMDD
    DateTime(i64),  // YYYYMMDDhhmmss
    Timestamp(i64), // YYYYMMDDhhmmss
    Varchar(String),
    Varbinary(Vec<u8>),
    Null,
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(v) => Some(*v as f64),
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Varchar(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Varbinary(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", format_date(*v)),
            Value::DateTime(v) => write!(f, "{}", format_datetime(*v)),
            Value::Timestamp(v) => write!(f, "{}", format_datetime(*v)),
            Value::Varchar(v) => write!(f, "{}", v),
            Value::Varbinary(v) => write!(f, "<binary {} bytes>", v.len()),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Wrapper for Value that implements Eq + Hash, for use in HashMap/HashSet (GROUP BY, COUNT DISTINCT).
#[derive(Debug, Clone)]
pub struct ValueKey(pub Value);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                match (float_to_exact_i64_key(*a), float_to_exact_i64_key(*b)) {
                    (Some(x), Some(y)) => x == y,
                    _ => canonical_f64_bits(*a) == canonical_f64_bits(*b),
                }
            }
            (Value::Integer(a), Value::Float(b)) => int_float_equal(*a, *b),
            (Value::Float(a), Value::Integer(b)) => int_float_equal(*b, *a),
            (a @ Value::Date(_), b @ Value::Date(_))
            | (a @ Value::Date(_), b @ Value::DateTime(_))
            | (a @ Value::Date(_), b @ Value::Timestamp(_))
            | (a @ Value::DateTime(_), b @ Value::Date(_))
            | (a @ Value::DateTime(_), b @ Value::DateTime(_))
            | (a @ Value::DateTime(_), b @ Value::Timestamp(_))
            | (a @ Value::Timestamp(_), b @ Value::Date(_))
            | (a @ Value::Timestamp(_), b @ Value::DateTime(_))
            | (a @ Value::Timestamp(_), b @ Value::Timestamp(_)) => {
                temporal_key(a) == temporal_key(b)
            }
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Varbinary(a), Value::Varbinary(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ValueKey {}

impl Hash for ValueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            Value::Integer(n) => {
                if in_exact_f64_int_range(*n) {
                    // Numeric-equivalent domain for int/float (e.g. 1 and 1.0).
                    0u8.hash(state);
                    n.hash(state);
                } else {
                    1u8.hash(state);
                    n.hash(state);
                }
            }
            Value::Float(n) => {
                if let Some(i) = float_to_exact_i64_key(*n) {
                    0u8.hash(state);
                    i.hash(state);
                } else {
                    2u8.hash(state);
                    canonical_f64_bits(*n).hash(state);
                }
            }
            Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) => {
                3u8.hash(state);
                temporal_key(&self.0).hash(state);
            }
            Value::Varchar(s) => {
                6u8.hash(state);
                s.hash(state);
            }
            Value::Varbinary(b) => {
                7u8.hash(state);
                b.hash(state);
            }
            Value::Null => {
                8u8.hash(state);
            }
        }
    }
}

const MAX_EXACT_F64_INT: i64 = 1_i64 << 53;

fn in_exact_f64_int_range(i: i64) -> bool {
    (-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&i)
}

fn canonical_f64_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else {
        n.to_bits()
    }
}

fn float_to_exact_i64_key(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let f = if f == 0.0 { 0.0 } else { f };
    if f.fract() != 0.0 {
        return None;
    }
    if !((-MAX_EXACT_F64_INT as f64)..=(MAX_EXACT_F64_INT as f64)).contains(&f) {
        return None;
    }
    Some(f as i64)
}

fn int_float_equal(i: i64, f: f64) -> bool {
    in_exact_f64_int_range(i) && float_to_exact_i64_key(f) == Some(i)
}

fn temporal_key(v: &Value) -> i64 {
    match v {
        Value::Date(d) => (*d as i64) * 1_000_000,
        Value::DateTime(dt) | Value::Timestamp(dt) => *dt,
        _ => unreachable!("temporal_key called for non-temporal value"),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_timestamp_string, Value, ValueKey};
    use std::collections::HashSet;

    #[test]
    fn test_value_key_large_int_float_no_non_transitive_equality() {
        let a = ValueKey(Value::Integer(9_007_199_254_740_992)); // 2^53
        let b = ValueKey(Value::Float(9_007_199_254_740_992.0));
        let c = ValueKey(Value::Integer(9_007_199_254_740_993)); // 2^53 + 1

        assert_eq!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn test_value_key_hash_for_numeric_equivalent_values() {
        let mut set = HashSet::new();
        set.insert(ValueKey(Value::Integer(1)));
        set.insert(ValueKey(Value::Float(1.0)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_as_i64_is_strict_integer_only() {
        assert_eq!(Value::Integer(42).as_i64(), Some(42));
        assert_eq!(Value::Float(42.0).as_i64(), None);
        assert_eq!(Value::Float(f64::NAN).as_i64(), None);
    }

    #[test]
    fn test_value_key_temporal_cross_type_equality_and_hash() {
        let date = ValueKey(Value::Date(20260221));
        let dt = ValueKey(Value::DateTime(20260221000000));
        let ts = ValueKey(Value::Timestamp(20260221000000));
        assert_eq!(date, dt);
        assert_eq!(dt, ts);

        let mut set = HashSet::new();
        set.insert(date);
        set.insert(dt);
        set.insert(ts);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_parse_timestamp_string_with_timezone() {
        assert_eq!(
            parse_timestamp_string("2026-02-22 09:30:00+09:00"),
            Some(20260222003000)
        );
        assert_eq!(
            parse_timestamp_string("2026-02-21T15:00:00-02:30"),
            Some(20260221173000)
        );
        assert_eq!(
            parse_timestamp_string("2026-02-21 00:00:00Z"),
            Some(20260221000000)
        );
    }

    #[test]
    fn test_temporal_parsers_reject_non_ascii_without_panic() {
        assert_eq!(super::parse_date_string("123é-45-67"), None);
        assert_eq!(super::parse_datetime_string("2026-0é-21 00:00:00"), None);
        assert_eq!(
            super::parse_timestamp_string("2026-02-21 00:00:00+0é:00"),
            None
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Date,
    DateTime,
    Timestamp,
    Varchar(Option<u32>),
    Varbinary(Option<u32>),
    Text,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::TinyInt => write!(f, "TINYINT"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Int => write!(f, "INT"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Date => write!(f, "DATE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Varchar(None) => write!(f, "VARCHAR"),
            DataType::Varchar(Some(n)) => write!(f, "VARCHAR({})", n),
            DataType::Varbinary(None) => write!(f, "VARBINARY"),
            DataType::Varbinary(Some(n)) => write!(f, "VARBINARY({})", n),
            DataType::Text => write!(f, "TEXT"),
        }
    }
}

pub fn parse_date_string(s: &str) -> Option<i32> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() != 10 || b.get(4) != Some(&b'-') || b.get(7) != Some(&b'-') {
        return None;
    }
    let y: i32 = parse_ascii_i32(b.get(0..4)?)?;
    let m: i32 = parse_ascii_i32(b.get(5..7)?)?;
    let d: i32 = parse_ascii_i32(b.get(8..10)?)?;
    if !valid_date(y, m, d) {
        return None;
    }
    Some(y * 10000 + m * 100 + d)
}

pub fn parse_datetime_string(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() != 19 {
        return None;
    }
    let sep = *b.get(10)?;
    if sep != b' ' && sep != b'T' {
        return None;
    }
    if b.get(4) != Some(&b'-')
        || b.get(7) != Some(&b'-')
        || b.get(13) != Some(&b':')
        || b.get(16) != Some(&b':')
    {
        return None;
    }
    let y: i64 = parse_ascii_i64(b.get(0..4)?)?;
    let m: i64 = parse_ascii_i64(b.get(5..7)?)?;
    let d: i64 = parse_ascii_i64(b.get(8..10)?)?;
    let hh: i64 = parse_ascii_i64(b.get(11..13)?)?;
    let mm: i64 = parse_ascii_i64(b.get(14..16)?)?;
    let ss: i64 = parse_ascii_i64(b.get(17..19)?)?;
    if !valid_date(y as i32, m as i32, d as i32) || hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    Some(y * 10000000000 + m * 100000000 + d * 1000000 + hh * 10000 + mm * 100 + ss)
}

pub fn parse_timestamp_string(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();

    if let Some(stripped) = s.strip_suffix('Z') {
        return parse_datetime_string(stripped);
    }

    // Support explicit timezone offsets: YYYY-MM-DD[ T]HH:MM:SS+09:00 / -05:30
    if b.len() >= 25 {
        let tz_pos = b.len() - 6;
        let tz_sign = *b.get(tz_pos)?;
        if (tz_sign == b'+' || tz_sign == b'-') && b.get(tz_pos + 3) == Some(&b':') {
            let base = std::str::from_utf8(b.get(..tz_pos)?).ok()?;
            let base_packed = parse_datetime_string(base)?;
            let offset_h: i64 = parse_ascii_i64(b.get(tz_pos + 1..tz_pos + 3)?)?;
            let offset_m: i64 = parse_ascii_i64(b.get(tz_pos + 4..tz_pos + 6)?)?;
            if offset_h > 23 || offset_m > 59 {
                return None;
            }
            let offset_sec =
                (offset_h * 3600 + offset_m * 60) * if tz_sign == b'+' { 1 } else { -1 };
            let (y, mon, day, hh, mm, ss) = unpack_datetime(base_packed);
            let unix = datetime_to_unix(y, mon, day, hh, mm, ss)?;
            return unix_to_datetime(unix - offset_sec);
        }
    }

    // No timezone suffix: treat as UTC.
    parse_datetime_string(s)
}

pub fn format_date(packed: i32) -> String {
    let y = packed / 10000;
    let m = (packed / 100) % 100;
    let d = packed % 100;
    format!("{:04}-{:02}-{:02}", y, m, d)
}

pub fn format_datetime(packed: i64) -> String {
    let y = packed / 10000000000;
    let m = (packed / 100000000) % 100;
    let d = (packed / 1000000) % 100;
    let hh = (packed / 10000) % 100;
    let mm = (packed / 100) % 100;
    let ss = packed % 100;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss)
}

fn valid_date(y: i32, m: i32, d: i32) -> bool {
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d < 1 {
        return false;
    }
    let max_d = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(y) => 29,
        2 => 28,
        _ => return false,
    };
    d <= max_d
}

fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

fn parse_ascii_i32(bytes: &[u8]) -> Option<i32> {
    if bytes.is_empty() || bytes.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_ascii_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || bytes.iter().any(|b| !b.is_ascii_digit()) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
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

fn datetime_to_unix(y: i32, mon: i32, day: i32, hh: i32, mm: i32, ss: i32) -> Option<i64> {
    if !valid_date(y, mon, day)
        || !(0..=23).contains(&hh)
        || !(0..=59).contains(&mm)
        || !(0..=59).contains(&ss)
    {
        return None;
    }
    let days = days_from_civil(y, mon, day);
    Some(days * 86_400 + (hh as i64) * 3_600 + (mm as i64) * 60 + (ss as i64))
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

// Howard Hinnant's civil date algorithms.
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
