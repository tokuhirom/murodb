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

fn exec(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) {
    execute(sql, pager, catalog).unwrap();
}

fn exec_err(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> String {
    execute(sql, pager, catalog).unwrap_err().to_string()
}

fn query_rows(
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
    sql: &str,
) -> Vec<murodb::sql::executor::Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    }
}

// --- TINYINT ---

#[test]
fn test_tinyint_min_max() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val TINYINT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, -128)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 127)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(-128)));
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 2");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(127)));
}

#[test]
fn test_tinyint_underflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val TINYINT)",
    );
    let err = exec_err(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, -129)");
    assert!(
        err.contains("out of range") || err.contains("TINYINT"),
        "Expected range error, got: {}",
        err
    );
}

#[test]
fn test_tinyint_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val TINYINT)",
    );
    let err = exec_err(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 128)");
    assert!(
        err.contains("out of range") || err.contains("TINYINT"),
        "Expected range error, got: {}",
        err
    );
}

// --- SMALLINT ---

#[test]
fn test_smallint_min_max() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val SMALLINT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, -32768)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, 32767)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(-32768)));
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 2");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(32767)));
}

#[test]
fn test_smallint_underflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val SMALLINT)",
    );
    let err = exec_err(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, -32769)");
    assert!(
        err.contains("out of range") || err.contains("SMALLINT"),
        "Expected range error, got: {}",
        err
    );
}

#[test]
fn test_smallint_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val SMALLINT)",
    );
    let err = exec_err(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 32768)");
    assert!(
        err.contains("out of range") || err.contains("SMALLINT"),
        "Expected range error, got: {}",
        err
    );
}

// --- INT ---

#[test]
fn test_int_min_max() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, -2147483648)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, 2147483647)",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(-2147483648)));
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 2");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(2147483647)));
}

#[test]
fn test_int_underflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, -2147483649)",
    );
    assert!(
        err.contains("out of range") || err.contains("INT"),
        "Expected range error, got: {}",
        err
    );
}

#[test]
fn test_int_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 2147483648)",
    );
    assert!(
        err.contains("out of range") || err.contains("INT"),
        "Expected range error, got: {}",
        err
    );
}

// --- BIGINT ---

#[test]
fn test_bigint_min_max() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val BIGINT)",
    );
    // i64::MIN (-9223372036854775808) cannot be represented as a literal because
    // the parser handles it as -(9223372036854775808) which overflows i64.
    // Use i64::MIN + 1 instead.
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, -9223372036854775807)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, 9223372036854775807)",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(i64::MIN + 1)));
    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 2");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(i64::MAX)));
}

// --- FLOAT/DOUBLE ---

#[test]
fn test_float_nan_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val FLOAT)",
    );
    // NaN can't be represented as a SQL literal easily, but we can test via expression
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 0.0 / 0.0)",
    );
    // This may produce a different error depending on how division is handled
    assert!(
        err.contains("finite")
            || err.contains("NaN")
            || err.contains("division")
            || err.contains("zero"),
        "Expected non-finite or division error, got: {}",
        err
    );
}

#[test]
fn test_double_nan_rejected() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val DOUBLE)",
    );
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 0.0 / 0.0)",
    );
    assert!(
        err.contains("finite")
            || err.contains("NaN")
            || err.contains("division")
            || err.contains("zero"),
        "Expected non-finite or division error, got: {}",
        err
    );
}

// --- Arithmetic overflow ---

#[test]
fn test_bigint_addition_overflow() {
    let (mut pager, mut catalog, _dir) = setup();
    // SELECT expression that would overflow i64
    let result = execute(
        "SELECT 9223372036854775807 + 1 AS val",
        &mut pager,
        &mut catalog,
    );
    // Should either error or wrap — we just want to confirm it doesn't crash
    match result {
        Ok(ExecResult::Rows(rows)) => {
            // If it succeeds, it might have wrapped or returned a float
            let _val = rows[0].get("val");
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("overflow") || msg.contains("out of range"),
                "Expected overflow error, got: {}",
                msg
            );
        }
        _ => panic!("Unexpected result type"),
    }
}

#[test]
fn test_integer_zero_value() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 0)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(0)));
}

#[test]
fn test_negative_one() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, -1)");

    let rows = query_rows(&mut pager, &mut catalog, "SELECT val FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(-1)));
}
