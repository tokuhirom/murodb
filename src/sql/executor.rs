/// SQL executor: executes statements against the B-tree storage.
use std::collections::HashMap;
use std::collections::HashSet;

use crate::btree::key_encoding::{
    encode_composite_key, encode_f32, encode_f64, encode_i16, encode_i32, encode_i64, encode_i8,
};
use crate::btree::ops::BTree;
use crate::error::{MuroError, Result};
use crate::fts::index::{FtsIndex, FtsPendingOp};
use crate::fts::query::{query_boolean, query_natural, FtsResult};
use crate::fts::snippet::fts_snippet;
use crate::schema::catalog::{SystemCatalog, TableDef};
use crate::schema::column::{ColumnDef, DefaultValue};
use crate::schema::index::{IndexDef, IndexType};
use crate::sql::ast::*;
use crate::sql::eval::{eval_expr, is_truthy};
use crate::sql::parser::parse_sql;
use crate::sql::planner::{plan_select, Plan};
use crate::storage::page::PageId;
use crate::storage::page_store::PageStore;
use crate::types::{
    format_date, format_datetime, parse_date_string, parse_datetime_string, parse_timestamp_string,
    DataType, Value, ValueKey,
};

mod aggregation;
mod alter;
mod codec;
mod ddl;
mod fts;
mod indexing;
mod insert;
mod mutation;
mod row_format;
mod select_join;
mod select_meta;
mod select_query;
mod show;
mod subquery;

pub use codec::{deserialize_row, deserialize_row_versioned, encode_value, serialize_row};

use aggregation::{cmp_values, execute_aggregation, execute_aggregation_join, has_aggregates};
use alter::*;
use codec::default_value_for_column;
use ddl::*;
use fts::{
    build_fts_eval_context, execute_fts_scan_rows, free_btree_pages, fts_allocate_doc_id,
    fts_delete_doc_mapping, fts_get_doc_id, fts_put_doc_mapping, fts_set_next_doc_id,
    materialize_fts_expr, populate_fts_row_doc_ids, validate_fulltext_parser, validate_value,
    value_to_fts_text, FtsEvalContext, SQL_FTS_TERM_KEY,
};
use indexing::{
    check_unique_index_constraints, check_unique_index_constraints_excluding,
    delete_from_secondary_indexes, encode_index_key_from_row, encode_pk_key, eval_index_seek_key,
    eval_pk_seek_key, find_unique_index_conflict, index_seek_pk_keys,
    insert_into_secondary_indexes, persist_indexes, replace_delete_unique_conflicts,
};
use insert::*;
use mutation::*;
use row_format::*;
use select_join::*;
use select_meta::*;
use select_query::*;
use show::*;
use subquery::*;

/// A result row.
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<(String, Value)>,
}

impl Row {
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.values.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }
}

/// Execution result.
#[derive(Debug)]
pub enum ExecResult {
    Rows(Vec<Row>),
    RowsAffected(u64),
    Ok,
}
pub fn execute(
    sql: &str,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
    execute_statement(&stmt, pager, catalog)
}

pub fn execute_statement(
    stmt: &Statement,
    pager: &mut impl PageStore,
    catalog: &mut SystemCatalog,
) -> Result<ExecResult> {
    match stmt {
        Statement::CreateTable(ct) => exec_create_table(ct, pager, catalog),
        Statement::CreateIndex(ci) => exec_create_index(ci, pager, catalog),
        Statement::CreateFulltextIndex(fi) => exec_create_fulltext_index(fi, pager, catalog),
        Statement::DropTable(dt) => exec_drop_table(dt, pager, catalog),
        Statement::DropIndex(di) => exec_drop_index(di, pager, catalog),
        Statement::AlterTable(at) => exec_alter_table(at, pager, catalog),
        Statement::RenameTable(rt) => exec_rename_table(rt, pager, catalog),
        Statement::Insert(ins) => exec_insert(ins, pager, catalog),
        Statement::Select(sel) => exec_select(sel, pager, catalog),
        Statement::Explain(inner) => exec_explain(inner, pager, catalog),
        Statement::SetQuery(sq) => exec_set_query(sq, pager, catalog),
        Statement::Update(upd) => exec_update(upd, pager, catalog),
        Statement::Delete(del) => exec_delete(del, pager, catalog),
        Statement::ShowTables => exec_show_tables(pager, catalog),
        Statement::ShowCreateTable(name) => exec_show_create_table(name, pager, catalog),
        Statement::Describe(name) => exec_describe(name, pager, catalog),
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::ShowCheckpointStats
        | Statement::ShowDatabaseStats => Err(MuroError::Execution(
            "BEGIN/COMMIT/ROLLBACK/SHOW CHECKPOINT STATS/SHOW DATABASE STATS must be handled by Session".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::storage::pager::Pager;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    fn setup() -> (Pager, SystemCatalog, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let catalog = SystemCatalog::create(&mut pager).unwrap();
        (pager, catalog, dir)
    }

    #[test]
    fn test_create_table_and_insert() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        execute(
            "INSERT INTO t (id, name) VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t (id, name) VALUES (2, 'Bob')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
            assert_eq!(rows[1].get("name"), Some(&Value::Varchar("Bob".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_cmp_values_large_int_vs_float_ordering() {
        let i = Value::Integer(9_007_199_254_740_993);
        let f = Value::Float(9_007_199_254_740_992.0);
        assert_eq!(cmp_values(Some(&i), Some(&f)), std::cmp::Ordering::Greater);
        assert_eq!(cmp_values(Some(&f), Some(&i)), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_select_where() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'Charlie')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute("SELECT * FROM t WHERE id = 2", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_update() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "UPDATE t SET name = 'Alicia' WHERE id = 1",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 1);
        }

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alicia".into())));
        }
    }

    #[test]
    fn test_update_pk_seek_rechecks_full_where_clause() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "UPDATE t SET name = 'Alicia' WHERE id = 1 AND name = 'Bob'",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 0);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Alice".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_update_uses_index_seek_for_indexed_predicate() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, age BIGINT)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "CREATE INDEX idx_name ON t (name)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice', 20)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (2, 'Bob', 30)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'Bob', 40)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "UPDATE t SET age = age + 1 WHERE name = 'Bob'",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute(
            "SELECT id, age FROM t ORDER BY id ASC",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].get("age"), Some(&Value::Integer(20)));
            assert_eq!(rows[1].get("age"), Some(&Value::Integer(31)));
            assert_eq!(rows[2].get("age"), Some(&Value::Integer(41)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_update_pk_row_dependent_predicate_falls_back_to_scan() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, other_id BIGINT, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (1, 1, 'A')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (2, 1, 'B')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (3, 3, 'C')", &mut pager, &mut catalog).unwrap();

        let result = execute(
            "UPDATE t SET name = 'Z' WHERE id = other_id",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute(
            "SELECT id, name FROM t ORDER BY id ASC",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Z".into())));
            assert_eq!(rows[1].get("name"), Some(&Value::Varchar("B".into())));
            assert_eq!(rows[2].get("name"), Some(&Value::Varchar("Z".into())));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_update_index_row_dependent_predicate_falls_back_to_scan() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, alias VARCHAR, age BIGINT)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "CREATE INDEX idx_name ON t (name)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'a', 'a', 10)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (2, 'b', 'a', 20)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'b', 'b', 30)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "UPDATE t SET age = age + 100 WHERE name = alias",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute(
            "SELECT id, age FROM t ORDER BY id ASC",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("age"), Some(&Value::Integer(110)));
            assert_eq!(rows[1].get("age"), Some(&Value::Integer(20)));
            assert_eq!(rows[2].get("age"), Some(&Value::Integer(130)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_delete() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        execute("DELETE FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));
        }
    }

    #[test]
    fn test_delete_uses_index_seek_for_indexed_predicate() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "CREATE INDEX idx_name ON t (name)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();
        execute("INSERT INTO t VALUES (3, 'Bob')", &mut pager, &mut catalog).unwrap();

        let result = execute("DELETE FROM t WHERE name = 'Bob'", &mut pager, &mut catalog).unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute("SELECT id FROM t ORDER BY id ASC", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_delete_row_dependent_predicate_falls_back_to_scan() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, alias VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "CREATE INDEX idx_name ON t (name)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'a', 'a')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (2, 'b', 'a')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'b', 'b')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute("DELETE FROM t WHERE name = alias", &mut pager, &mut catalog).unwrap();
        if let ExecResult::RowsAffected(n) = result {
            assert_eq!(n, 2);
        } else {
            panic!("Expected RowsAffected");
        }

        let result = execute("SELECT id FROM t ORDER BY id ASC", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id"), Some(&Value::Integer(2)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_order_by_and_limit() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (3, 'Charlie')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'Alice')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (2, 'Bob')", &mut pager, &mut catalog).unwrap();

        let result = execute(
            "SELECT * FROM t ORDER BY id DESC LIMIT 2",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get("id"), Some(&Value::Integer(3)));
            assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
        }
    }

    #[test]
    fn test_unique_constraint() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, 'a@b.com')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        // Duplicate PK
        let result = execute(
            "INSERT INTO t VALUES (1, 'x@y.com')",
            &mut pager,
            &mut catalog,
        );
        assert!(result.is_err());

        // Duplicate UNIQUE
        let result = execute(
            "INSERT INTO t VALUES (2, 'a@b.com')",
            &mut pager,
            &mut catalog,
        );
        assert!(result.is_err());

        // Different value should work
        execute(
            "INSERT INTO t VALUES (2, 'c@d.com')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    #[test]
    fn test_null_values() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();

        let result = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows[0].get("name"), Some(&Value::Null));
        }
    }

    #[test]
    fn test_many_inserts() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        for i in 0..100 {
            let sql = format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i);
            execute(&sql, &mut pager, &mut catalog).unwrap();
        }

        let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 100);
        }
    }

    #[test]
    fn test_temporal_in_subquery_materialization_preserves_type() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, '2026-02-21')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (2, '2026-02-22')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "SELECT id FROM t WHERE d IN (SELECT d FROM t WHERE id = 1)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_having_max_date_preserves_temporal_type() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, '2026-02-21')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (2, '2026-02-20')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute(
            "SELECT MAX(d) AS md FROM t HAVING MAX(d) = CAST('2026-02-21' AS DATE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("md"), Some(&Value::Date(20260221)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_encode_value_float_to_temporal_is_non_empty_impossible_key() {
        let key_date = encode_value(&Value::Float(1.5), &DataType::Date);
        let key_dt = encode_value(&Value::Float(1.5), &DataType::DateTime);
        let key_ts = encode_value(&Value::Float(1.5), &DataType::Timestamp);
        assert_eq!(key_date.len(), 9);
        assert_eq!(key_dt.len(), 9);
        assert_eq!(key_ts.len(), 9);
    }

    #[test]
    fn test_timestamp_insert_with_timezone_is_normalized_to_utc() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, ts TIMESTAMP)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        execute(
            "INSERT INTO t VALUES (1, '2026-02-22 09:30:00+09:00')",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let result = execute("SELECT ts FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
        if let ExecResult::Rows(rows) = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get("ts"), Some(&Value::Timestamp(20260222003000)));
        } else {
            panic!("Expected rows");
        }
    }

    #[test]
    fn test_invalid_temporal_literals_are_rejected() {
        let (mut pager, mut catalog, _dir) = setup();

        execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, d DATE, ts TIMESTAMP)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

        let invalid_date = execute(
            "INSERT INTO t VALUES (1, '2026-02-30', '2026-02-22 00:00:00Z')",
            &mut pager,
            &mut catalog,
        );
        assert!(invalid_date.is_err());

        let invalid_timestamp = execute(
            "INSERT INTO t VALUES (2, '2026-02-22', '2026-02-22 00:00:00+24:00')",
            &mut pager,
            &mut catalog,
        );
        assert!(invalid_timestamp.is_err());
    }
}
