use super::*;

pub fn serialize_row(values: &[Value], columns: &[ColumnDef]) -> Vec<u8> {
    let mut buf = Vec::new();

    // Stored column count (u16) — allows deserialize_row to handle short rows
    // after ALTER TABLE ADD COLUMN without rewriting existing data.
    buf.extend_from_slice(&(columns.len() as u16).to_le_bytes());

    // Null bitmap (1 bit per column, packed into bytes)
    let bitmap_bytes = columns.len().div_ceil(8);
    let mut bitmap = vec![0u8; bitmap_bytes];
    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend_from_slice(&bitmap);

    // Values
    for (i, val) in values.iter().enumerate() {
        if val.is_null() {
            continue;
        }
        match val {
            Value::Integer(n) => match columns[i].data_type {
                DataType::TinyInt => buf.extend_from_slice(&(*n as i8).to_le_bytes()),
                DataType::SmallInt => buf.extend_from_slice(&(*n as i16).to_le_bytes()),
                DataType::Int => buf.extend_from_slice(&(*n as i32).to_le_bytes()),
                DataType::BigInt => buf.extend_from_slice(&n.to_le_bytes()),
                DataType::Float => buf.extend_from_slice(&(*n as f32).to_le_bytes()),
                DataType::Double => buf.extend_from_slice(&(*n as f64).to_le_bytes()),
                DataType::Date => buf.extend_from_slice(&(*n as i32).to_le_bytes()),
                DataType::DateTime | DataType::Timestamp => buf.extend_from_slice(&n.to_le_bytes()),
                DataType::Varchar(_) | DataType::Text => {
                    let bytes = n.to_string().as_bytes().to_vec();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&bytes);
                }
                DataType::Varbinary(_) => {
                    let b = n.to_le_bytes();
                    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&b);
                }
            },
            Value::Float(n) => match columns[i].data_type {
                DataType::TinyInt => buf.extend_from_slice(&(*n as i8).to_le_bytes()),
                DataType::SmallInt => buf.extend_from_slice(&(*n as i16).to_le_bytes()),
                DataType::Int => buf.extend_from_slice(&(*n as i32).to_le_bytes()),
                DataType::BigInt => buf.extend_from_slice(&(*n as i64).to_le_bytes()),
                DataType::Float => buf.extend_from_slice(&(*n as f32).to_le_bytes()),
                DataType::Double => buf.extend_from_slice(&n.to_le_bytes()),
                DataType::Date | DataType::DateTime | DataType::Timestamp => {
                    // Coercion should reject this path before serialize_row.
                    panic!("float value reached date/time serializer")
                }
                DataType::Varchar(_) | DataType::Text => {
                    let bytes = n.to_string().as_bytes().to_vec();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&bytes);
                }
                DataType::Varbinary(_) => {
                    let b = n.to_le_bytes();
                    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&b);
                }
            },
            Value::Date(d) => match columns[i].data_type {
                DataType::Date => buf.extend_from_slice(&d.to_le_bytes()),
                DataType::DateTime | DataType::Timestamp => {
                    let v = (*d as i64) * 1_000_000;
                    buf.extend_from_slice(&v.to_le_bytes());
                }
                DataType::Varchar(_) | DataType::Text => {
                    let s = format_date(*d);
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                _ => panic!("date value reached incompatible serializer"),
            },
            Value::DateTime(dt) => match columns[i].data_type {
                DataType::Date => {
                    let d = (*dt / 1_000_000) as i32;
                    buf.extend_from_slice(&d.to_le_bytes());
                }
                DataType::DateTime | DataType::Timestamp => {
                    buf.extend_from_slice(&dt.to_le_bytes());
                }
                DataType::Varchar(_) | DataType::Text => {
                    let s = format_datetime(*dt);
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                _ => panic!("datetime value reached incompatible serializer"),
            },
            Value::Timestamp(ts) => match columns[i].data_type {
                DataType::Date => {
                    let d = (*ts / 1_000_000) as i32;
                    buf.extend_from_slice(&d.to_le_bytes());
                }
                DataType::DateTime | DataType::Timestamp => {
                    buf.extend_from_slice(&ts.to_le_bytes());
                }
                DataType::Varchar(_) | DataType::Text => {
                    let s = format_datetime(*ts);
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                _ => panic!("timestamp value reached incompatible serializer"),
            },
            Value::Varchar(s) => {
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Varbinary(b) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Null => {} // already skipped
        }
    }

    buf
}

pub fn deserialize_row(data: &[u8], columns: &[ColumnDef]) -> Result<Vec<Value>> {
    deserialize_row_versioned(data, columns, 1)
}

/// Deserialize a row with explicit row format version.
/// version 0: legacy format (no prefix, stored_col_count == columns.len())
/// version 1: u16 column count prefix
pub fn deserialize_row_versioned(
    data: &[u8],
    columns: &[ColumnDef],
    row_format_version: u8,
) -> Result<Vec<Value>> {
    let (stored_col_count, data) = if row_format_version >= 1 {
        // New format: u16 column count prefix
        if data.len() < 2 {
            return Err(MuroError::InvalidPage);
        }
        let count = u16::from_le_bytes(data[0..2].try_into().unwrap()) as usize;
        (count, &data[2..])
    } else {
        // Legacy format: no prefix, assume all current columns are stored
        (columns.len(), data)
    };

    let bitmap_bytes = stored_col_count.div_ceil(8);
    if data.len() < bitmap_bytes {
        return Err(MuroError::InvalidPage);
    }

    let bitmap = &data[..bitmap_bytes];
    let mut offset = bitmap_bytes;
    let mut values = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        // Columns beyond what was stored get default/NULL
        if i >= stored_col_count {
            values.push(default_value_for_column(col));
            continue;
        }

        let is_null = bitmap[i / 8] & (1 << (i % 8)) != 0;
        if is_null {
            values.push(Value::Null);
            continue;
        }

        match col.data_type {
            DataType::TinyInt => {
                if offset + 1 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = data[offset] as i8;
                values.push(Value::Integer(n as i64));
                offset += 1;
            }
            DataType::SmallInt => {
                if offset + 2 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
                values.push(Value::Integer(n as i64));
                offset += 2;
            }
            DataType::Int => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                values.push(Value::Integer(n as i64));
                offset += 4;
            }
            DataType::BigInt => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::Integer(n));
                offset += 8;
            }
            DataType::Float => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                values.push(Value::Float(n as f64));
                offset += 4;
            }
            DataType::Double => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::Float(n));
                offset += 8;
            }
            DataType::Date => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                values.push(Value::Date(n));
                offset += 4;
            }
            DataType::DateTime => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::DateTime(n));
                offset += 8;
            }
            DataType::Timestamp => {
                if offset + 8 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                values.push(Value::Timestamp(n));
                offset += 8;
            }
            DataType::Varchar(_) | DataType::Text => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let s = String::from_utf8(data[offset..offset + len].to_vec())
                    .map_err(|_| MuroError::InvalidPage)?;
                values.push(Value::Varchar(s));
                offset += len;
            }
            DataType::Varbinary(_) => {
                if offset + 4 > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err(MuroError::InvalidPage);
                }
                values.push(Value::Varbinary(data[offset..offset + len].to_vec()));
                offset += len;
            }
        }
    }

    Ok(values)
}

/// Get the default value for a newly-added column.
pub(super) fn default_value_for_column(col: &ColumnDef) -> Value {
    match &col.default_value {
        Some(DefaultValue::Integer(n)) => Value::Integer(*n),
        Some(DefaultValue::Float(n)) => Value::Float(*n),
        Some(DefaultValue::String(s)) => Value::Varchar(s.clone()),
        Some(DefaultValue::Null) | None => Value::Null,
    }
}

/// Encode a Value for use as a B-tree key.
/// For integer types, the encoding width depends on the DataType.
pub fn encode_value(value: &Value, data_type: &DataType) -> Vec<u8> {
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0; // -2^63
    const I64_UPPER_EXCLUSIVE_F64: f64 = 9_223_372_036_854_775_808.0; // 2^63

    fn float_as_integral_i64(n: f64) -> Option<i64> {
        if !n.is_finite()
            || n.fract() != 0.0
            || !(I64_MIN_F64..I64_UPPER_EXCLUSIVE_F64).contains(&n)
        {
            None
        } else {
            Some(n as i64)
        }
    }

    fn impossible_int_seek_key() -> Vec<u8> {
        // Integer keys are fixed-width 1/2/4/8 bytes in this engine.
        // A 9-byte key cannot match any integer key.
        vec![0xff; 9]
    }

    match (value, data_type) {
        (Value::Integer(n), DataType::TinyInt) => encode_i8(*n as i8).to_vec(),
        (Value::Integer(n), DataType::SmallInt) => encode_i16(*n as i16).to_vec(),
        (Value::Integer(n), DataType::Int) => encode_i32(*n as i32).to_vec(),
        (Value::Integer(n), DataType::BigInt) => encode_i64(*n).to_vec(),
        (Value::Integer(n), DataType::Float) => encode_f32(*n as f32).to_vec(),
        (Value::Integer(n), DataType::Double) => encode_f64(*n as f64).to_vec(),
        (Value::Integer(n), DataType::Date) => encode_i32(*n as i32).to_vec(),
        (Value::Integer(n), DataType::DateTime | DataType::Timestamp) => encode_i64(*n).to_vec(),
        (Value::Integer(n), _) => encode_i64(*n).to_vec(),
        (Value::Float(n), DataType::TinyInt) => {
            float_as_integral_i64(*n).map_or_else(impossible_int_seek_key, |v| {
                if (i8::MIN as i64..=i8::MAX as i64).contains(&v) {
                    encode_i8(v as i8).to_vec()
                } else {
                    impossible_int_seek_key()
                }
            })
        }
        (Value::Float(n), DataType::SmallInt) => {
            float_as_integral_i64(*n).map_or_else(impossible_int_seek_key, |v| {
                if (i16::MIN as i64..=i16::MAX as i64).contains(&v) {
                    encode_i16(v as i16).to_vec()
                } else {
                    impossible_int_seek_key()
                }
            })
        }
        (Value::Float(n), DataType::Int) => {
            float_as_integral_i64(*n).map_or_else(impossible_int_seek_key, |v| {
                if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
                    encode_i32(v as i32).to_vec()
                } else {
                    impossible_int_seek_key()
                }
            })
        }
        (Value::Float(n), DataType::BigInt) => float_as_integral_i64(*n)
            .map_or_else(impossible_int_seek_key, |v| encode_i64(v).to_vec()),
        (Value::Float(n), DataType::Float) => encode_f32(*n as f32).to_vec(),
        (Value::Float(n), DataType::Double) => encode_f64(*n).to_vec(),
        (Value::Float(_), DataType::Date | DataType::DateTime | DataType::Timestamp) => {
            impossible_int_seek_key()
        }
        (Value::Float(n), _) => encode_f64(*n).to_vec(),
        (Value::Date(n), DataType::Date) => encode_i32(*n).to_vec(),
        (Value::Date(n), DataType::DateTime | DataType::Timestamp) => {
            encode_i64((*n as i64) * 1_000_000).to_vec()
        }
        (Value::Date(n), _) => encode_i32(*n).to_vec(),
        (Value::DateTime(n), DataType::Date) => encode_i32((*n / 1_000_000) as i32).to_vec(),
        (Value::DateTime(n), DataType::DateTime | DataType::Timestamp) => encode_i64(*n).to_vec(),
        (Value::DateTime(n), _) => encode_i64(*n).to_vec(),
        (Value::Timestamp(n), DataType::Date) => encode_i32((*n / 1_000_000) as i32).to_vec(),
        (Value::Timestamp(n), DataType::DateTime | DataType::Timestamp) => encode_i64(*n).to_vec(),
        (Value::Timestamp(n), _) => encode_i64(*n).to_vec(),
        (Value::Varchar(s), _) => s.as_bytes().to_vec(),
        (Value::Varbinary(b), _) => b.clone(),
        (Value::Null, _) => Vec::new(),
    }
}
