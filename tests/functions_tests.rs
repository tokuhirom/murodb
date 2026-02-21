/// Integration tests for Phase 2: Built-in scalar functions.
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

fn query_one(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> Value {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows[0].values[0].1.clone(),
        other => panic!("Expected rows, got {:?}", other),
    }
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

fn exec(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) {
    execute(sql, pager, catalog).unwrap();
}

// ── String functions (basic) ──

#[test]
fn test_length() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LENGTH(name) FROM t"),
        Value::Integer(5)
    );
}

#[test]
fn test_char_length() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'héllo')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CHAR_LENGTH(name) FROM t"),
        Value::Integer(5)
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LENGTH(name) FROM t"),
        Value::Integer(6)
    );
}

#[test]
fn test_concat() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR, b VARCHAR)",
    );
    exec(
        &mut p,
        &mut c,
        "INSERT INTO t VALUES (1, 'hello', ' world')",
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CONCAT(a, b) FROM t"),
        Value::Varchar("hello world".into())
    );
}

#[test]
fn test_concat_null() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR, b VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t (id, a) VALUES (1, 'hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CONCAT(a, b) FROM t"),
        Value::Null
    );
}

#[test]
fn test_substring() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello world')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT SUBSTRING(name, 7) FROM t"),
        Value::Varchar("world".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT SUBSTRING(name, 1, 5) FROM t"),
        Value::Varchar("hello".into())
    );
}

#[test]
fn test_upper_lower() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'Hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT UPPER(name) FROM t"),
        Value::Varchar("HELLO".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LOWER(name) FROM t"),
        Value::Varchar("hello".into())
    );
}

// ── String functions (extended) ──

#[test]
fn test_trim() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, '  hello  ')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT TRIM(name) FROM t"),
        Value::Varchar("hello".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LTRIM(name) FROM t"),
        Value::Varchar("hello  ".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT RTRIM(name) FROM t"),
        Value::Varchar("  hello".into())
    );
}

#[test]
fn test_replace() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello world')");
    assert_eq!(
        query_one(
            &mut p,
            &mut c,
            "SELECT REPLACE(name, 'world', 'rust') FROM t"
        ),
        Value::Varchar("hello rust".into())
    );
}

#[test]
fn test_reverse() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT REVERSE(name) FROM t"),
        Value::Varchar("olleh".into())
    );
}

#[test]
fn test_repeat() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'ab')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT REPEAT(name, 3) FROM t"),
        Value::Varchar("ababab".into())
    );
}

#[test]
fn test_left_right() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LEFT(name, 3) FROM t"),
        Value::Varchar("hel".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT RIGHT(name, 3) FROM t"),
        Value::Varchar("llo".into())
    );
}

#[test]
fn test_lpad_rpad() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hi')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LPAD(name, 5, '*') FROM t"),
        Value::Varchar("***hi".into())
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT RPAD(name, 5, '*') FROM t"),
        Value::Varchar("hi***".into())
    );
}

#[test]
fn test_instr() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello world')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT INSTR(name, 'world') FROM t"),
        Value::Integer(7)
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT INSTR(name, 'xyz') FROM t"),
        Value::Integer(0)
    );
}

#[test]
fn test_locate() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello hello')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LOCATE('hello', name) FROM t"),
        Value::Integer(1)
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LOCATE('hello', name, 2) FROM t"),
        Value::Integer(7)
    );
}

// ── REGEXP ──

#[test]
fn test_regexp() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello123')");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 'world')");
    let rows = query_rows(
        &mut p,
        &mut c,
        "SELECT id FROM t WHERE name REGEXP '[0-9]+'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

#[test]
fn test_regexp_like_function() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'hello123')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT REGEXP_LIKE(name, '^hello') FROM t"),
        Value::Integer(1)
    );
}

// ── Numeric functions ──

#[test]
fn test_abs() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, -42)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT ABS(val) FROM t"),
        Value::Integer(42)
    );
}

#[test]
fn test_ceil_floor() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 42)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CEIL(val) FROM t"),
        Value::Integer(42)
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT FLOOR(val) FROM t"),
        Value::Integer(42)
    );
}

#[test]
fn test_mod_function() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 10)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT MOD(val, 3) FROM t"),
        Value::Integer(1)
    );
}

#[test]
fn test_power() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 2)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT POWER(val, 10) FROM t"),
        Value::Integer(1024)
    );
}

// ── NULL handling & conditional ──

#[test]
fn test_coalesce() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR, b VARCHAR)",
    );
    exec(
        &mut p,
        &mut c,
        "INSERT INTO t (id, b) VALUES (1, 'fallback')",
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT COALESCE(a, b) FROM t"),
        Value::Varchar("fallback".into())
    );
}

#[test]
fn test_ifnull() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT IFNULL(a, 'default') FROM t"),
        Value::Varchar("default".into())
    );
}

#[test]
fn test_nullif() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 0)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT NULLIF(val, 0) FROM t"),
        Value::Null
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 5)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT NULLIF(val, 0) FROM t WHERE id = 2"),
        Value::Integer(5)
    );
}

#[test]
fn test_if_function() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 1)");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 0)");
    assert_eq!(
        query_one(
            &mut p,
            &mut c,
            "SELECT IF(val, 'yes', 'no') FROM t WHERE id = 1"
        ),
        Value::Varchar("yes".into())
    );
    assert_eq!(
        query_one(
            &mut p,
            &mut c,
            "SELECT IF(val, 'yes', 'no') FROM t WHERE id = 2"
        ),
        Value::Varchar("no".into())
    );
}

// ── CASE WHEN ──

#[test]
fn test_case_when_searched() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 10)");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 20)");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (3, 30)");
    assert_eq!(
        query_one(
            &mut p, &mut c,
            "SELECT CASE WHEN val < 15 THEN 'low' WHEN val < 25 THEN 'mid' ELSE 'high' END FROM t WHERE id = 1"
        ),
        Value::Varchar("low".into())
    );
    assert_eq!(
        query_one(
            &mut p, &mut c,
            "SELECT CASE WHEN val < 15 THEN 'low' WHEN val < 25 THEN 'mid' ELSE 'high' END FROM t WHERE id = 3"
        ),
        Value::Varchar("high".into())
    );
}

#[test]
fn test_case_when_simple() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, status INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 1)");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 2)");
    assert_eq!(
        query_one(
            &mut p, &mut c,
            "SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t WHERE id = 1"
        ),
        Value::Varchar("active".into())
    );
}

#[test]
fn test_case_when_no_else() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 99)");
    assert_eq!(
        query_one(
            &mut p,
            &mut c,
            "SELECT CASE WHEN val = 1 THEN 'one' WHEN val = 2 THEN 'two' END FROM t"
        ),
        Value::Null
    );
}

// ── CAST ──

#[test]
fn test_cast_varchar_to_int() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, '42')");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CAST(val AS INT) FROM t"),
        Value::Integer(42)
    );
}

#[test]
fn test_cast_int_to_varchar() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 42)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CAST(val AS VARCHAR) FROM t"),
        Value::Varchar("42".into())
    );
}

#[test]
fn test_cast_null() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT CAST(val AS INT) FROM t"),
        Value::Null
    );
}

// ── NULL propagation ──

#[test]
fn test_null_propagation() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t (id) VALUES (1)");
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT LENGTH(val) FROM t"),
        Value::Null
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT UPPER(val) FROM t"),
        Value::Null
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT REPLACE(val, 'a', 'b') FROM t"),
        Value::Null
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT REVERSE(val) FROM t"),
        Value::Null
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT TRIM(val) FROM t"),
        Value::Null
    );
}

// ── Functions in WHERE clause ──

#[test]
fn test_function_in_where() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut p, &mut c, "INSERT INTO t VALUES (1, 'alice')");
    exec(&mut p, &mut c, "INSERT INTO t VALUES (2, 'Bob')");
    let rows = query_rows(
        &mut p,
        &mut c,
        "SELECT id FROM t WHERE UPPER(name) = 'ALICE'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].1, Value::Integer(1));
}

// ── Nested function calls ──

#[test]
fn test_nested_functions() {
    let (mut p, mut c, _d) = setup();
    exec(
        &mut p,
        &mut c,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut p,
        &mut c,
        "INSERT INTO t VALUES (1, '  Hello World  ')",
    );
    assert_eq!(
        query_one(&mut p, &mut c, "SELECT UPPER(TRIM(name)) FROM t"),
        Value::Varchar("HELLO WORLD".into())
    );
}
