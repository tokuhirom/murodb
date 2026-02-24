#![cfg(feature = "test-utils")]
use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::ExecResult;
use murodb::sql::session::Session;
use murodb::storage::pager::Pager;
use murodb::types::Value;
use murodb::wal::writer::WalWriter;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn setup_session() -> (Session, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.db.wal");
    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    (Session::new(pager, catalog, wal), dir)
}

fn exec(session: &mut Session, sql: &str) -> Vec<Vec<(String, Value)>> {
    match session.execute(sql).unwrap() {
        ExecResult::Rows(rows) => rows.into_iter().map(|r| r.values).collect(),
        ExecResult::Ok => vec![],
        ExecResult::RowsAffected(_) => vec![],
    }
}

fn exec_err(session: &mut Session, sql: &str) -> String {
    session.execute(sql).unwrap_err().to_string()
}

#[test]
fn test_decimal_create_table_and_insert() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, price DECIMAL(18,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 19.99)");
    exec(&mut s, "INSERT INTO t VALUES (2, 100.5)");

    let rows = exec(&mut s, "SELECT id, price FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1].1.to_string(), "19.99");
    assert_eq!(rows[1][1].1.to_string(), "100.50");
}

#[test]
fn test_decimal_from_string_literal() {
    // String literals preserve exact decimal representation
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, price DECIMAL(18,2))",
    );
    exec(
        &mut s,
        "INSERT INTO t VALUES (1, CAST('100.50' AS DECIMAL(18,2)))",
    );

    let rows = exec(&mut s, "SELECT price FROM t");
    assert_eq!(rows[0][0].1.to_string(), "100.50");
}

#[test]
fn test_decimal_default_precision() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL)",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 12345)");

    let rows = exec(&mut s, "SELECT val FROM t");
    assert_eq!(rows[0][0].1.to_string(), "12345");
}

#[test]
fn test_decimal_exact_arithmetic() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 1.1, 2.2)");

    let rows = exec(&mut s, "SELECT a + b FROM t");
    // Decimal arithmetic: 1.10 + 2.20 = 3.30 exactly (no float error)
    assert_eq!(rows[0][0].1.to_string(), "3.30");
}

#[test]
fn test_decimal_negative_values() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, -42.5)");

    let rows = exec(&mut s, "SELECT val FROM t");
    assert_eq!(rows[0][0].1.to_string(), "-42.50");
}

#[test]
fn test_decimal_null_handling() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, NULL)");

    let rows = exec(&mut s, "SELECT val FROM t");
    assert!(rows[0][0].1.is_null());
}

#[test]
fn test_decimal_order_by() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 3.14)");
    exec(&mut s, "INSERT INTO t VALUES (2, 1.0)");
    exec(&mut s, "INSERT INTO t VALUES (3, 2.71)");

    let rows = exec(&mut s, "SELECT val FROM t ORDER BY val ASC");
    assert_eq!(rows[0][0].1.to_string(), "1.00");
    assert_eq!(rows[1][0].1.to_string(), "2.71");
    assert_eq!(rows[2][0].1.to_string(), "3.14");
}

#[test]
fn test_decimal_group_by() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, cat DECIMAL(5,1), val INT)",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 1.0, 10)");
    exec(&mut s, "INSERT INTO t VALUES (2, 1.0, 20)");
    exec(&mut s, "INSERT INTO t VALUES (3, 2.0, 30)");

    let rows = exec(
        &mut s,
        "SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY cat",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0].1.to_string(), "1.0");
    assert_eq!(rows[0][1].1.to_string(), "30");
}

#[test]
fn test_decimal_aggregation_sum_min_max() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.5)");
    exec(&mut s, "INSERT INTO t VALUES (2, 20.25)");
    exec(&mut s, "INSERT INTO t VALUES (3, 30.75)");

    let rows = exec(&mut s, "SELECT SUM(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "61.50");

    let rows = exec(&mut s, "SELECT MIN(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "10.50");

    let rows = exec(&mut s, "SELECT MAX(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "30.75");
}

#[test]
fn test_decimal_aggregation_avg() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.0)");
    exec(&mut s, "INSERT INTO t VALUES (2, 20.0)");
    exec(&mut s, "INSERT INTO t VALUES (3, 30.0)");

    let rows = exec(&mut s, "SELECT AVG(val) FROM t");
    let avg_str = rows[0][0].1.to_string();
    // 60/3 = 20
    assert!(avg_str == "20" || avg_str.starts_with("20."));
}

#[test]
fn test_decimal_cast_from_integer() {
    let (mut s, _dir) = setup_session();
    let rows = exec(&mut s, "SELECT CAST(42 AS DECIMAL(10,2))");
    assert_eq!(rows[0][0].1.to_string(), "42.00");
}

#[test]
fn test_decimal_cast_from_varchar() {
    let (mut s, _dir) = setup_session();
    let rows = exec(&mut s, "SELECT CAST('123.456' AS DECIMAL(10,3))");
    assert_eq!(rows[0][0].1.to_string(), "123.456");
}

#[test]
fn test_decimal_cast_to_integer() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 42.99)");

    let rows = exec(&mut s, "SELECT CAST(val AS INT) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "42");
}

#[test]
fn test_decimal_cast_to_varchar() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 42.5)");

    let rows = exec(&mut s, "SELECT CAST(val AS VARCHAR) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "42.50");
}

#[test]
fn test_decimal_comparison() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.0)");
    exec(&mut s, "INSERT INTO t VALUES (2, 20.0)");
    exec(&mut s, "INSERT INTO t VALUES (3, 30.0)");

    let rows = exec(&mut s, "SELECT val FROM t WHERE val > 15.0");
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_decimal_with_integer_comparison() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.0)");
    exec(&mut s, "INSERT INTO t VALUES (2, 20.0)");

    let rows = exec(&mut s, "SELECT val FROM t WHERE val > 15");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_decimal_arithmetic_operations() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a DECIMAL(10,2), b DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.5, 3.25)");

    let rows = exec(&mut s, "SELECT a - b FROM t");
    assert_eq!(rows[0][0].1.to_string(), "7.25");

    let rows = exec(&mut s, "SELECT a * b FROM t");
    assert_eq!(rows[0][0].1.to_string(), "34.1250");
}

#[test]
fn test_decimal_integer_arithmetic() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.5)");

    let rows = exec(&mut s, "SELECT val + 5 FROM t");
    assert_eq!(rows[0][0].1.to_string(), "15.50");

    let rows = exec(&mut s, "SELECT val * 2 FROM t");
    assert_eq!(rows[0][0].1.to_string(), "21.00");
}

#[test]
fn test_decimal_numeric_type_alias() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val NUMERIC(8,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 99.99)");

    let rows = exec(&mut s, "SELECT val FROM t");
    assert_eq!(rows[0][0].1.to_string(), "99.99");
}

#[test]
fn test_decimal_precision_validation() {
    let (mut s, _dir) = setup_session();
    let err = exec_err(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(0,0))",
    );
    assert!(err.contains("precision must be between 1 and 28"));

    let err = exec_err(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(29,0))",
    );
    assert!(err.contains("precision must be between 1 and 28"));
}

#[test]
fn test_decimal_scale_validation() {
    let (mut s, _dir) = setup_session();
    let err = exec_err(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(5,6))",
    );
    assert!(err.contains("scale must be between 0 and 5"));
}

#[test]
fn test_decimal_round_function() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,4))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 3.1459)");

    let rows = exec(&mut s, "SELECT ROUND(val, 2) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "3.15");
}

#[test]
fn test_decimal_abs_function() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, -42.5)");

    let rows = exec(&mut s, "SELECT ABS(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "42.50");
}

#[test]
fn test_decimal_floor_ceil() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 3.14)");

    let rows = exec(&mut s, "SELECT FLOOR(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "3");

    let rows = exec(&mut s, "SELECT CEIL(val) FROM t");
    assert_eq!(rows[0][0].1.to_string(), "4");
}

#[test]
fn test_decimal_describe_table() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, price DECIMAL(18,2))",
    );

    let rows = exec(&mut s, "DESCRIBE t");
    let price_row = rows.iter().find(|r| r[0].1.to_string() == "price").unwrap();
    assert!(price_row[1].1.to_string().contains("DECIMAL"));
}

#[test]
fn test_decimal_division_by_zero() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 10.0)");

    let err = exec_err(&mut s, "SELECT val / 0 FROM t");
    assert!(err.contains("Division by zero"));
}

#[test]
fn test_decimal_negation() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "INSERT INTO t VALUES (1, 42.5)");

    let rows = exec(&mut s, "SELECT -val FROM t");
    assert_eq!(rows[0][0].1.to_string(), "-42.50");
}

#[test]
fn test_decimal_index() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DECIMAL(10,2))",
    );
    exec(&mut s, "CREATE INDEX idx_val ON t (val)");
    exec(&mut s, "INSERT INTO t VALUES (1, 3.14)");
    exec(&mut s, "INSERT INTO t VALUES (2, 1.0)");
    exec(&mut s, "INSERT INTO t VALUES (3, 2.71)");

    let rows = exec(&mut s, "SELECT val FROM t WHERE val = 3.14");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1.to_string(), "3.14");
}

#[test]
fn test_decimal_precision_overflow_on_insert() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, d DECIMAL(5,2))",
    );

    // 1234.567 has 4 integer digits, but DECIMAL(5,2) allows only 3 integer digits (5-2)
    let err = exec_err(&mut s, "INSERT INTO t VALUES (1, 1234.567)");
    assert!(err.contains("out of range for DECIMAL(5,2)"));

    // 999.99 should work (3 integer digits, max for DECIMAL(5,2))
    exec(&mut s, "INSERT INTO t VALUES (1, 999.99)");
    let rows = exec(&mut s, "SELECT d FROM t");
    assert_eq!(rows[0][0].1.to_string(), "999.99");
}

#[test]
fn test_decimal_scale_rounding_on_insert() {
    let (mut s, _dir) = setup_session();
    exec(
        &mut s,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, d DECIMAL(10,2))",
    );

    // Extra fractional digits should be rounded
    exec(&mut s, "INSERT INTO t VALUES (1, 3.456)");
    let rows = exec(&mut s, "SELECT d FROM t");
    assert_eq!(rows[0][0].1.to_string(), "3.46");
}

#[test]
fn test_decimal_cast_precision_overflow() {
    let (mut s, _dir) = setup_session();
    let err = exec_err(&mut s, "SELECT CAST(12345 AS DECIMAL(4,0))");
    assert!(err.contains("out of range for DECIMAL(4,0)"));
}
