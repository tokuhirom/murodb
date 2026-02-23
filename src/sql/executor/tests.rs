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

#[test]
fn test_foreign_key_insert_and_delete_restrict() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE parents (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES parents(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    execute("INSERT INTO parents VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute(
        "INSERT INTO children VALUES (10, 1)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let missing_parent = execute(
        "INSERT INTO children VALUES (11, 999)",
        &mut pager,
        &mut catalog,
    );
    assert!(missing_parent.is_err());

    let delete_parent = execute("DELETE FROM parents WHERE id = 1", &mut pager, &mut catalog);
    assert!(delete_parent.is_err());
}

#[test]
fn test_foreign_key_composite_and_nullable() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE parents (a BIGINT, b BIGINT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE children (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, FOREIGN KEY (a, b) REFERENCES parents(a, b))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    execute(
        "INSERT INTO parents VALUES (1, 2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO children VALUES (1, 1, 2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO children VALUES (2, NULL, 2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let bad = execute(
        "INSERT INTO children VALUES (3, 1, 9)",
        &mut pager,
        &mut catalog,
    );
    assert!(bad.is_err());
}

#[test]
fn test_show_create_and_describe_include_foreign_key() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let show = execute("SHOW CREATE TABLE c", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = show {
        let ddl = match rows[0].get("Create Table") {
            Some(Value::Varchar(s)) => s,
            _ => panic!("expected Create Table string"),
        };
        assert!(ddl.contains("FOREIGN KEY (p_id) REFERENCES p(id)"));
    } else {
        panic!("expected rows");
    }

    let desc = execute("DESCRIBE c", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = desc {
        assert!(rows
            .iter()
            .any(|r| { matches!(r.get("Key"), Some(Value::Varchar(k)) if k == "FK") }));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_describe_reports_foreign_key_actions() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE ON UPDATE SET NULL)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    let desc = execute("DESCRIBE c", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = desc {
        let fk_row = rows
            .iter()
            .find(|r| matches!(r.get("Key"), Some(Value::Varchar(k)) if k == "FK"))
            .expect("expected FK row");
        assert_eq!(
            fk_row.get("Extra"),
            Some(&Value::Varchar(
                "ON DELETE CASCADE ON UPDATE SET NULL".to_string()
            ))
        );
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_drop_table_referenced_by_foreign_key_is_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let res = execute("DROP TABLE p", &mut pager, &mut catalog);
    assert!(res.is_err());
}

#[test]
fn test_rename_table_updates_foreign_key_references() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("RENAME TABLE p TO p2", &mut pager, &mut catalog).unwrap();

    let show = execute("SHOW CREATE TABLE c", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = show {
        let ddl = match rows[0].get("Create Table") {
            Some(Value::Varchar(s)) => s,
            _ => panic!("expected Create Table string"),
        };
        assert!(ddl.contains("REFERENCES p2(id)"));
    } else {
        panic!("expected rows");
    }

    execute("INSERT INTO p2 VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (1, 1)", &mut pager, &mut catalog).unwrap();
    let err = execute("INSERT INTO c VALUES (2, 999)", &mut pager, &mut catalog);
    assert!(err.is_err());
}

#[test]
fn test_foreign_key_on_delete_cascade() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (10, 1)", &mut pager, &mut catalog).unwrap();

    execute("DELETE FROM p WHERE id = 1", &mut pager, &mut catalog).unwrap();
    let result = execute("SELECT * FROM c", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert!(rows.is_empty());
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_foreign_key_on_delete_set_null() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE SET NULL)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (10, 1)", &mut pager, &mut catalog).unwrap();

    execute("DELETE FROM p WHERE id = 1", &mut pager, &mut catalog).unwrap();
    let result = execute("SELECT p_id FROM c WHERE id = 10", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = result {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("p_id"), Some(&Value::Null));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_foreign_key_on_update_cascade_and_set_null() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, uk BIGINT UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c1 (id BIGINT PRIMARY KEY, p_uk BIGINT, FOREIGN KEY (p_uk) REFERENCES p(uk) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c2 (id BIGINT PRIMARY KEY, p_uk BIGINT, FOREIGN KEY (p_uk) REFERENCES p(uk) ON UPDATE SET NULL)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    execute("INSERT INTO p VALUES (1, 100)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c1 VALUES (10, 100)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c2 VALUES (20, 100)", &mut pager, &mut catalog).unwrap();

    execute(
        "UPDATE p SET uk = 200 WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let c1 = execute(
        "SELECT p_uk FROM c1 WHERE id = 10",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = c1 {
        assert_eq!(rows[0].get("p_uk"), Some(&Value::Integer(200)));
    } else {
        panic!("expected rows");
    }

    let c2 = execute(
        "SELECT p_uk FROM c2 WHERE id = 20",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = c2 {
        assert_eq!(rows[0].get("p_uk"), Some(&Value::Null));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_alter_table_add_foreign_key_validates_existing_rows() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("INSERT INTO c VALUES (1, 999)", &mut pager, &mut catalog).unwrap();
    let err = execute(
        "ALTER TABLE c ADD FOREIGN KEY (p_id) REFERENCES p(id)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    execute("DELETE FROM c WHERE id = 1", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO p VALUES (10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (2, 10)", &mut pager, &mut catalog).unwrap();
    execute(
        "ALTER TABLE c ADD FOREIGN KEY (p_id) REFERENCES p(id)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
}

#[test]
fn test_alter_table_drop_foreign_key_removes_constraint() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "ALTER TABLE c DROP FOREIGN KEY (p_id)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("INSERT INTO c VALUES (1, 999)", &mut pager, &mut catalog).unwrap();
}

#[test]
fn test_cascade_delete_honors_grandchild_restrict() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
        "CREATE TABLE g (id BIGINT PRIMARY KEY, c_id BIGINT, FOREIGN KEY (c_id) REFERENCES c(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO p VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (10, 1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO g VALUES (100, 10)", &mut pager, &mut catalog).unwrap();

    let err = execute("DELETE FROM p WHERE id = 1", &mut pager, &mut catalog);
    assert!(err.is_err());
}

#[test]
fn test_cascade_update_validates_other_outgoing_foreign_keys() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p1 (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE p2 (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, a_id BIGINT, FOREIGN KEY (a_id) REFERENCES p1(id) ON UPDATE CASCADE, FOREIGN KEY (a_id) REFERENCES p2(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p1 VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO p2 VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (10, 1)", &mut pager, &mut catalog).unwrap();

    // Updating p1.id to 2 would cascade c.a_id=2, which violates c.a_id -> p2(id).
    let err = execute(
        "UPDATE p1 SET id = 2 WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());
}

#[test]
fn test_drop_column_not_used_by_fk_child_side_is_allowed() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, id BIGINT, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES p(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    execute("ALTER TABLE c DROP COLUMN id", &mut pager, &mut catalog).unwrap();
}

#[test]
fn test_self_referencing_delete_ignores_rows_pending_deletion() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES t(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES (2, 1)", &mut pager, &mut catalog).unwrap();

    execute("DELETE FROM t", &mut pager, &mut catalog).unwrap();
    let rows = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert!(rows.is_empty());
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_cascade_update_rekeys_child_when_fk_is_part_of_pk() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (u BIGINT, seq BIGINT, PRIMARY KEY (u, seq), FOREIGN KEY (u) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1, 100)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (100, 1)", &mut pager, &mut catalog).unwrap();

    execute(
        "UPDATE p SET u = 200 WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // If child PK re-keying is correct, old PK can be reused.
    execute("INSERT INTO p VALUES (2, 100)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (100, 1)", &mut pager, &mut catalog).unwrap();
    let rows = execute(
        "SELECT * FROM c WHERE u = 200 AND seq = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_parent_unique_failure_does_not_mutate_child_before_cascade() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, u BIGINT UNIQUE, u2 BIGINT UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, pu BIGINT, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
        "INSERT INTO p VALUES (1, 10, 100)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO p VALUES (2, 20, 200)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO c VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    let err = execute("UPDATE p SET u = 20 WHERE id = 1", &mut pager, &mut catalog);
    assert!(err.is_err());

    let rows = execute("SELECT pu FROM c WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows[0].get("pu"), Some(&Value::Integer(10)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_delete_cascade_cycle_does_not_recurse_forever() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, b_id BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE b (id BIGINT PRIMARY KEY, a_id BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "ALTER TABLE a ADD FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "ALTER TABLE b ADD FOREIGN KEY (a_id) REFERENCES a(id) ON DELETE CASCADE",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("INSERT INTO a VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO b VALUES (1, 1)", &mut pager, &mut catalog).unwrap();
    execute(
        "UPDATE a SET b_id = 1 WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("DELETE FROM a WHERE id = 1", &mut pager, &mut catalog).unwrap();
    let a_rows = execute("SELECT * FROM a", &mut pager, &mut catalog).unwrap();
    let b_rows = execute("SELECT * FROM b", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = a_rows {
        assert!(rows.is_empty());
    } else {
        panic!("expected rows");
    }
    if let ExecResult::Rows(rows) = b_rows {
        assert!(rows.is_empty());
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_cascade_update_propagates_parent_side_fk_checks() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, pu BIGINT UNIQUE, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
        "CREATE TABLE g (id BIGINT PRIMARY KEY, cu BIGINT, FOREIGN KEY (cu) REFERENCES c(pu))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("INSERT INTO p VALUES (1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO g VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    // Updating p.u cascades c.pu. Since g.cu references c(pu) with RESTRICT,
    // this must fail unless g is also handled.
    let err = execute("UPDATE p SET u = 20 WHERE id = 1", &mut pager, &mut catalog);
    assert!(err.is_err());

    let c_rows = execute("SELECT pu FROM c WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = c_rows {
        assert_eq!(rows[0].get("pu"), Some(&Value::Integer(10)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_parent_update_failing_outgoing_fk_does_not_apply_incoming_cascade() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE gp (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE p (id BIGINT PRIMARY KEY, g_id BIGINT, u BIGINT UNIQUE, FOREIGN KEY (g_id) REFERENCES gp(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, pu BIGINT, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO gp VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO p VALUES (1, 1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    let err = execute(
        "UPDATE p SET u = 20, g_id = 999 WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    let rows = execute("SELECT pu FROM c WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows[0].get("pu"), Some(&Value::Integer(10)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_on_duplicate_update_failing_outgoing_fk_does_not_apply_incoming_cascade() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE gp (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE p (id BIGINT PRIMARY KEY, g_id BIGINT, u BIGINT UNIQUE, FOREIGN KEY (g_id) REFERENCES gp(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, pu BIGINT, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO gp VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO p VALUES (1, 1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    let err = execute(
        "INSERT INTO p VALUES (1, 999, 10) ON DUPLICATE KEY UPDATE g_id = 999, u = 20",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    let rows = execute("SELECT pu FROM c WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows[0].get("pu"), Some(&Value::Integer(10)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_replace_checks_foreign_keys_for_all_conflicting_rows() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, email VARCHAR UNIQUE, uname VARCHAR UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (id BIGINT PRIMARY KEY, uname VARCHAR, FOREIGN KEY (uname) REFERENCES p(uname))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
        "INSERT INTO p VALUES (1, 'a@example.com', 'u1')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO p VALUES (2, 'b@example.com', 'u2')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO c VALUES (1, 'u2')", &mut pager, &mut catalog).unwrap();

    // Conflicts with id=1 by email and id=2 by uname. id=2 is referenced.
    let err = execute(
        "REPLACE INTO p VALUES (3, 'a@example.com', 'u2')",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    let p2 = execute("SELECT * FROM p WHERE id = 2", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = p2 {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_drop_foreign_key_ambiguous_columns_is_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p1 (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE p2 (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, x BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p1(id)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "ALTER TABLE c ADD FOREIGN KEY (x) REFERENCES p2(id)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let err = execute(
        "ALTER TABLE c DROP FOREIGN KEY (x)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());
}

#[test]
fn test_drop_column_referenced_by_self_fk_parent_side_is_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, u BIGINT UNIQUE, pu BIGINT, FOREIGN KEY (pu) REFERENCES t(u))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    let err = execute("ALTER TABLE t DROP COLUMN u", &mut pager, &mut catalog);
    assert!(err.is_err());
}

#[test]
fn test_delete_mixed_actions_has_no_partial_side_effect_on_restrict_failure() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c_cas (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c_res (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE RESTRICT)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_cas VALUES (1, 1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_res VALUES (1, 1)", &mut pager, &mut catalog).unwrap();

    let err = execute("DELETE FROM p WHERE id = 1", &mut pager, &mut catalog);
    assert!(err.is_err());

    // CASCADE side table must remain unchanged on failure.
    let rows = execute("SELECT * FROM c_cas", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_delete_multi_parent_rows_has_no_partial_side_effect_on_restrict_failure() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c_cas (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c_res (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id) ON DELETE RESTRICT)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO p VALUES (2)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_cas VALUES (1, 1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_res VALUES (1, 2)", &mut pager, &mut catalog).unwrap();

    let err = execute(
        "DELETE FROM p WHERE id = 1 OR id = 2",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    let cas_rows = execute("SELECT * FROM c_cas", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = cas_rows {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected rows");
    }

    let p_rows = execute("SELECT * FROM p", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = p_rows {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_update_mixed_actions_has_no_partial_side_effect_on_restrict_failure() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY, u BIGINT UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c_cas (id BIGINT PRIMARY KEY, pu BIGINT, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE CASCADE)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
            "CREATE TABLE c_res (id BIGINT PRIMARY KEY, pu BIGINT, FOREIGN KEY (pu) REFERENCES p(u) ON UPDATE RESTRICT)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute("INSERT INTO p VALUES (1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_cas VALUES (1, 10)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO c_res VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    let err = execute("UPDATE p SET u = 20 WHERE id = 1", &mut pager, &mut catalog);
    assert!(err.is_err());

    let rows = execute(
        "SELECT pu FROM c_cas WHERE id = 1",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = rows {
        assert_eq!(rows[0].get("pu"), Some(&Value::Integer(10)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_modify_column_rejected_when_fk_depends_on_it() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, p_id BIGINT, FOREIGN KEY (p_id) REFERENCES p(id))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let err = execute(
        "ALTER TABLE c MODIFY COLUMN p_id INT",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());
}

#[test]
fn test_change_column_rejected_when_self_fk_parent_depends_on_it() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, u BIGINT UNIQUE, pu BIGINT, FOREIGN KEY (pu) REFERENCES t(u))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    let err = execute(
        "ALTER TABLE t CHANGE COLUMN u u2 BIGINT",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());
}

#[test]
fn test_replace_rechecks_fk_after_conflict_deletes() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR UNIQUE, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES t(id) ON DELETE SET NULL)",
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    execute(
        "INSERT INTO t VALUES (1, 'p', NULL)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (2, 'c', 1)", &mut pager, &mut catalog).unwrap();

    // Conflicts with code='p' and deletes parent row id=1 first.
    // Without post-delete recheck this could insert an orphan parent_id=1.
    let err = execute(
        "REPLACE INTO t VALUES (3, 'p', 1)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.is_err());

    let p = execute("SELECT * FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
    if let ExecResult::Rows(rows) = p {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected rows");
    }

    let c = execute(
        "SELECT parent_id FROM t WHERE id = 2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    if let ExecResult::Rows(rows) = c {
        assert_eq!(rows[0].get("parent_id"), Some(&Value::Integer(1)));
    } else {
        panic!("expected rows");
    }
}

#[test]
fn test_modify_unrelated_child_column_allowed_even_if_name_matches_parent_ref_col() {
    let (mut pager, mut catalog, _dir) = setup();
    execute(
        "CREATE TABLE p (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
            "CREATE TABLE c (cid BIGINT PRIMARY KEY, id INT, parent_id BIGINT, FOREIGN KEY (parent_id) REFERENCES p(id))",
            &mut pager,
            &mut catalog,
        )
        .unwrap();

    let ok = execute(
        "ALTER TABLE c MODIFY COLUMN id BIGINT",
        &mut pager,
        &mut catalog,
    );
    assert!(ok.is_ok());
}
