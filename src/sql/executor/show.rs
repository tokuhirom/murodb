use super::*;

pub(super) fn exec_show_tables(
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let tables = catalog.list_tables(pager)?;
    let rows = tables
        .into_iter()
        .map(|name| Row {
            values: vec![("Table".to_string(), Value::Varchar(name))],
        })
        .collect();
    Ok(ExecResult::Rows(rows))
}

pub(super) fn exec_show_create_table(
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let mut sql = format!("CREATE TABLE {} (\n", table_name);
    let visible_columns: Vec<&ColumnDef> =
        table_def.columns.iter().filter(|c| !c.is_hidden).collect();
    let is_composite_pk = table_def.is_composite_pk();

    // Collect table-level constraints to append after columns
    let mut table_constraints = Vec::new();
    if is_composite_pk {
        table_constraints.push(format!(
            "  PRIMARY KEY ({})",
            table_def.pk_columns.join(", ")
        ));
    }

    // Collect composite UNIQUE indexes
    let indexes = catalog.get_indexes_for_table(pager, table_name)?;
    for idx in &indexes {
        if idx.is_unique && idx.column_names.len() > 1 {
            table_constraints.push(format!("  UNIQUE ({})", idx.column_names.join(", ")));
        }
    }

    let total_items = visible_columns.len() + table_constraints.len();
    for (i, col) in visible_columns.iter().enumerate() {
        sql.push_str(&format!("  {} {}", col.name, col.data_type));
        if let Some(collation) = &col.collation {
            sql.push_str(&format!(" COLLATE {}", collation));
        }
        if col.is_primary_key && !is_composite_pk {
            sql.push_str(" PRIMARY KEY");
        }
        if col.auto_increment {
            sql.push_str(" AUTO_INCREMENT");
        }
        if col.is_unique && !col.is_primary_key {
            sql.push_str(" UNIQUE");
        }
        if !col.is_nullable && !col.is_primary_key {
            sql.push_str(" NOT NULL");
        }
        if let Some(default) = &col.default_value {
            match default {
                DefaultValue::Integer(n) => sql.push_str(&format!(" DEFAULT {}", n)),
                DefaultValue::Float(n) => sql.push_str(&format!(" DEFAULT {}", n)),
                DefaultValue::String(s) => sql.push_str(&format!(" DEFAULT '{}'", s)),
                DefaultValue::Null => sql.push_str(" DEFAULT NULL"),
            }
        }
        if let Some(check) = &col.check_expr {
            sql.push_str(&format!(" CHECK ({})", check));
        }
        if i < total_items - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }
    for (i, constraint) in table_constraints.iter().enumerate() {
        sql.push_str(constraint);
        if i < table_constraints.len() - 1 {
            sql.push(',');
        }
        sql.push('\n');
    }
    sql.push(')');

    let rows = vec![Row {
        values: vec![
            ("Table".to_string(), Value::Varchar(table_name.to_string())),
            ("Create Table".to_string(), Value::Varchar(sql)),
        ],
    }];
    Ok(ExecResult::Rows(rows))
}

pub(super) fn exec_describe(
    table_name: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let table_def = catalog
        .get_table(pager, table_name)?
        .ok_or_else(|| MuroError::Schema(format!("Table '{}' not found", table_name)))?;

    let mut rows = Vec::new();
    for col in &table_def.columns {
        if col.is_hidden {
            continue;
        }
        let null_str = if col.is_nullable { "YES" } else { "NO" };
        let key_str = if col.is_primary_key {
            "PRI"
        } else if col.is_unique {
            "UNI"
        } else {
            ""
        };
        let default_str = match &col.default_value {
            Some(DefaultValue::Integer(n)) => n.to_string(),
            Some(DefaultValue::Float(n)) => n.to_string(),
            Some(DefaultValue::String(s)) => s.clone(),
            Some(DefaultValue::Null) => "NULL".to_string(),
            None => "NULL".to_string(),
        };
        let extra_str = match (col.auto_increment, col.collation.as_deref()) {
            (true, Some(collation)) => format!("auto_increment,collation={}", collation),
            (true, None) => "auto_increment".to_string(),
            (false, Some(collation)) => format!("collation={}", collation),
            (false, None) => "".to_string(),
        };

        rows.push(Row {
            values: vec![
                ("Field".to_string(), Value::Varchar(col.name.clone())),
                (
                    "Type".to_string(),
                    Value::Varchar(col.data_type.to_string()),
                ),
                ("Null".to_string(), Value::Varchar(null_str.to_string())),
                ("Key".to_string(), Value::Varchar(key_str.to_string())),
                ("Default".to_string(), Value::Varchar(default_str)),
                ("Extra".to_string(), Value::Varchar(extra_str)),
            ],
        });
    }
    Ok(ExecResult::Rows(rows))
}

// --- Row serialization ---
// Format: [null_bitmap][value1][value2]...
// Each value: for integers: 1/2/4/8 bytes by type; for VARCHAR/TEXT/VARBINARY: u32 len + bytes
