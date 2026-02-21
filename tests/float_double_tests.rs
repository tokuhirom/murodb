use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult};
use murodb::storage::pager::Pager;
use murodb::types::Value;
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

fn assert_float_eq(val: &Value, expected: f64) {
    match val {
        Value::Float(n) => assert!((*n - expected).abs() < 1e-6, "expected {expected}, got {n}"),
        other => panic!("expected float, got {:?}", other),
    }
}

#[test]
fn test_float_double_insert_and_select() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, f FLOAT, d DOUBLE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t VALUES (1, 1.25, 2.5), (2, 3, 4.75)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT f, d FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_float_eq(&rows[0].values[0].1, 1.25);
    assert_float_eq(&rows[0].values[1].1, 2.5);
}

#[test]
fn test_float_order_by() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, score DOUBLE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t VALUES (1, 10.5), (2, 2.25), (3, 7.75)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT score FROM t ORDER BY score ASC",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };

    assert_float_eq(&rows[0].values[0].1, 2.25);
    assert_float_eq(&rows[1].values[0].1, 7.75);
    assert_float_eq(&rows[2].values[0].1, 10.5);
}

#[test]
fn test_float_arithmetic_and_cast() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, x DOUBLE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 1.5)", &mut pager, &mut catalog).unwrap();

    let result = execute(
        "SELECT x + 0.25, CAST('3.75' AS DOUBLE) FROM t",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };

    assert_float_eq(&rows[0].values[0].1, 1.75);
    assert_float_eq(&rows[0].values[1].1, 3.75);
}

#[test]
fn test_float_default_value() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, ratio FLOAT DEFAULT 0.5)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t (id) VALUES (1)", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT ratio FROM t", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };

    assert_float_eq(&rows[0].values[0].1, 0.5);
}

#[test]
fn test_avg_large_integer_precision() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1, 9007199254740992), (2, 9007199254740994)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT AVG(v) FROM t", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows[0].values[0].1, Value::Integer(9007199254740993));
}

#[test]
fn test_signed_zero_pk_lookup_consistency() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id DOUBLE PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (-0.0, 1)", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT v FROM t WHERE id = 0.0", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_integer_pk_seek_with_float_literal() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 42)", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT v FROM t WHERE id = 1.0", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(42));

    let result = execute("SELECT v FROM t WHERE id = 1.5", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert!(rows.is_empty());
}

#[test]
fn test_count_distinct_signed_zero() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, x DOUBLE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1, -0.0), (2, 0.0)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT COUNT(DISTINCT x) FROM t", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_insert_float_into_bigint_coerces_not_corrupts() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 1.5)", &mut pager, &mut catalog).unwrap();

    let result = execute("SELECT v FROM t WHERE id = 1", &mut pager, &mut catalog).unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_count_distinct_mixed_int_float_numeric() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1), (2)", &mut pager, &mut catalog).unwrap();

    let result = execute(
        "SELECT COUNT(DISTINCT CASE WHEN id = 1 THEN 1 ELSE 1.0 END) FROM t",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_insert_rejects_non_finite_float_to_bigint() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "INSERT INTO t VALUES (1, CAST('NaN' AS DOUBLE))",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
}

#[test]
fn test_update_rejects_non_finite_float_to_bigint() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 10)", &mut pager, &mut catalog).unwrap();

    let result = execute(
        "UPDATE t SET v = CAST('inf' AS DOUBLE) WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
}

#[test]
fn test_insert_rejects_out_of_range_float_to_bigint() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "INSERT INTO t VALUES (1, 9223372036854775808.0)",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
}

#[test]
fn test_bigint_seek_out_of_range_float_literal_does_not_hit_max() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (9223372036854775807, 1)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT v FROM t WHERE id = 9223372036854775808.0",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert!(rows.is_empty());
}

#[test]
fn test_alter_modify_varchar_to_double_rejects_non_finite() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 'NaN')", &mut pager, &mut catalog).unwrap();

    let result = execute(
        "ALTER TABLE t MODIFY COLUMN v DOUBLE",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
}

#[test]
fn test_count_distinct_large_ints_with_float_is_stable() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1), (2), (3)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT COUNT(DISTINCT CASE \
         WHEN id = 1 THEN 9007199254740992 \
         WHEN id = 2 THEN 9007199254740993 \
         ELSE 9007199254740992.0 END) FROM t",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows[0].values[0].1, Value::Integer(2));
}

#[test]
fn test_composite_pk_seek_float_column_with_integer_literal() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (a DOUBLE, b BIGINT, v BIGINT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (1.0, 2, 42)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT v FROM t WHERE a = 1 AND b = 2",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let ExecResult::Rows(rows) = result else {
        panic!("Expected rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(42));
}
