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

fn count_rows(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> usize {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows.len(),
        _ => panic!("Expected rows"),
    }
}

fn get_rows(
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
    sql: &str,
) -> Vec<murodb::sql::executor::Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    }
}

// ============================================================
// DROP TABLE / DROP TABLE IF EXISTS
// ============================================================

#[test]
fn test_drop_table() {
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

    execute("DROP TABLE t", &mut pager, &mut catalog).unwrap();

    // Table should no longer exist
    let result = execute("SELECT * FROM t", &mut pager, &mut catalog);
    assert!(result.is_err());
}

#[test]
fn test_drop_table_not_exists_error() {
    let (mut pager, mut catalog, _dir) = setup();
    let result = execute("DROP TABLE nonexistent", &mut pager, &mut catalog);
    assert!(result.is_err());
}

#[test]
fn test_drop_table_if_exists() {
    let (mut pager, mut catalog, _dir) = setup();

    // Should not error when table doesn't exist
    execute("DROP TABLE IF EXISTS nonexistent", &mut pager, &mut catalog).unwrap();

    // Create and drop
    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("DROP TABLE IF EXISTS t", &mut pager, &mut catalog).unwrap();
    let result = execute("SELECT * FROM t", &mut pager, &mut catalog);
    assert!(result.is_err());
}

#[test]
fn test_drop_table_with_indexes() {
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

    execute("DROP TABLE t", &mut pager, &mut catalog).unwrap();

    // Table gone
    let result = execute("SELECT * FROM t", &mut pager, &mut catalog);
    assert!(result.is_err());
}

// ============================================================
// DROP INDEX
// ============================================================

#[test]
fn test_drop_index() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_name ON t(name)", &mut pager, &mut catalog).unwrap();
    execute("DROP INDEX idx_name", &mut pager, &mut catalog).unwrap();
}

#[test]
fn test_drop_index_not_exists_error() {
    let (mut pager, mut catalog, _dir) = setup();
    let result = execute("DROP INDEX nonexistent", &mut pager, &mut catalog);
    assert!(result.is_err());
}

#[test]
fn test_drop_index_if_exists() {
    let (mut pager, mut catalog, _dir) = setup();
    execute("DROP INDEX IF EXISTS nonexistent", &mut pager, &mut catalog).unwrap();
}

// ============================================================
// IF NOT EXISTS for CREATE TABLE / CREATE INDEX
// ============================================================

#[test]
fn test_create_table_if_not_exists() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Should not error
    execute(
        "CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Table should still have original schema (1 column)
    let rows = get_rows(&mut pager, &mut catalog, "DESCRIBE t");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_create_index_if_not_exists() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_name ON t(name)", &mut pager, &mut catalog).unwrap();

    // Should not error
    execute(
        "CREATE INDEX IF NOT EXISTS idx_name ON t(name)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
}

// ============================================================
// SHOW CREATE TABLE
// ============================================================

#[test]
fn test_show_create_table() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SHOW CREATE TABLE users");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("Table"), Some(&Value::Varchar("users".into())));
    let create_sql = rows[0].get("Create Table").unwrap();
    if let Value::Varchar(sql) = create_sql {
        assert!(sql.contains("id BIGINT PRIMARY KEY"));
        assert!(sql.contains("name VARCHAR NOT NULL"));
        assert!(sql.contains("email VARCHAR UNIQUE"));
    } else {
        panic!("Expected varchar");
    }
}

#[test]
fn test_show_create_table_with_default() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, status INT DEFAULT 0, name VARCHAR DEFAULT 'unknown')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SHOW CREATE TABLE t");
    let sql = rows[0].get("Create Table").unwrap();
    if let Value::Varchar(s) = sql {
        assert!(s.contains("DEFAULT 0"), "SQL: {}", s);
        assert!(s.contains("DEFAULT 'unknown'"), "SQL: {}", s);
    }
}

// ============================================================
// DESCRIBE / DESC table
// ============================================================

#[test]
fn test_describe() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR UNIQUE)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "DESCRIBE users");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("Field"), Some(&Value::Varchar("id".into())));
    assert_eq!(rows[0].get("Key"), Some(&Value::Varchar("PRI".into())));
    assert_eq!(rows[1].get("Field"), Some(&Value::Varchar("name".into())));
    assert_eq!(rows[1].get("Null"), Some(&Value::Varchar("NO".into())));
    assert_eq!(rows[2].get("Field"), Some(&Value::Varchar("email".into())));
    assert_eq!(rows[2].get("Key"), Some(&Value::Varchar("UNI".into())));
}

#[test]
fn test_desc() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // DESC as alias for DESCRIBE
    let rows = get_rows(&mut pager, &mut catalog, "DESC t");
    assert_eq!(rows.len(), 1);
}

// ============================================================
// LIKE / NOT LIKE
// ============================================================

#[test]
fn test_like() {
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

    // % wildcard
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name LIKE 'A%'"
        ),
        1
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name LIKE '%li%'"
        ),
        2 // Alice and Charlie
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name LIKE '%e'"
        ),
        2 // Alice and Charlie
    );

    // _ wildcard
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name LIKE 'Bo_'"
        ),
        1
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name LIKE '___'"
        ),
        1 // Bob
    );
}

#[test]
fn test_not_like() {
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

    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name NOT LIKE 'A%'"
        ),
        1
    );
}

// ============================================================
// IN (value list)
// ============================================================

#[test]
fn test_in_list() {
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

    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE id IN (1, 3)"
        ),
        2
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name IN ('Alice', 'Bob')"
        ),
        2
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE id NOT IN (1, 3)"
        ),
        1
    );
}

// ============================================================
// BETWEEN ... AND ...
// ============================================================

#[test]
fn test_between() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    for i in 1..=10 {
        execute(
            &format!("INSERT INTO t VALUES ({}, {})", i, i * 10),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE val BETWEEN 30 AND 70"
        ),
        5
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE val NOT BETWEEN 30 AND 70"
        ),
        5
    );
}

// ============================================================
// IS NULL / IS NOT NULL
// ============================================================

#[test]
fn test_is_null() {
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
    execute("INSERT INTO t VALUES (2, NULL)", &mut pager, &mut catalog).unwrap();
    execute(
        "INSERT INTO t VALUES (3, 'Charlie')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name IS NULL"
        ),
        1
    );
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE name IS NOT NULL"
        ),
        2
    );
}

// ============================================================
// NOT operator (general)
// ============================================================

#[test]
fn test_not_operator() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, active INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES (2, 0)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES (3, 1)", &mut pager, &mut catalog).unwrap();

    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM t WHERE NOT active = 1"
        ),
        1
    );
}

// ============================================================
// OFFSET
// ============================================================

#[test]
fn test_offset() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    for i in 1..=5 {
        execute(
            &format!("INSERT INTO t VALUES ({}, 'name_{}')", i, i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
    }

    let rows = get_rows(
        &mut pager,
        &mut catalog,
        "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 2",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(3)));
    assert_eq!(rows[1].get("id"), Some(&Value::Integer(4)));
}

#[test]
fn test_offset_beyond_rows() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(
        &mut pager,
        &mut catalog,
        "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 100",
    );
    assert_eq!(rows.len(), 0);
}

// ============================================================
// DEFAULT column values
// ============================================================

#[test]
fn test_default_integer() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, status INT DEFAULT 0)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t (id) VALUES (1)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("status"), Some(&Value::Integer(0)));
}

#[test]
fn test_default_string() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR DEFAULT 'unknown')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t (id) VALUES (1)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("unknown".into())));
}

#[test]
fn test_default_null() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR DEFAULT NULL)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t (id) VALUES (1)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("name"), Some(&Value::Null));
}

#[test]
fn test_default_override() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, status INT DEFAULT 0)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 42)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("status"), Some(&Value::Integer(42)));
}

// ============================================================
// AUTO_INCREMENT
// ============================================================

#[test]
fn test_auto_increment() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (name) VALUES ('Alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (name) VALUES ('Bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (name) VALUES ('Charlie')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
    assert_eq!(rows[2].get("id"), Some(&Value::Integer(3)));
}

#[test]
fn test_auto_increment_describe() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "DESCRIBE t");
    assert_eq!(
        rows[0].get("Extra"),
        Some(&Value::Varchar("auto_increment".into()))
    );
}

// ============================================================
// Arithmetic operators (+, -, *, /, %)
// ============================================================

#[test]
fn test_arithmetic_in_select() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 10, 3)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(
        &mut pager,
        &mut catalog,
        "SELECT a + b AS sum, a - b AS diff, a * b AS prod, a / b AS quot, a % b AS remainder FROM t",
    );
    assert_eq!(rows[0].get("sum"), Some(&Value::Integer(13)));
    assert_eq!(rows[0].get("diff"), Some(&Value::Integer(7)));
    assert_eq!(rows[0].get("prod"), Some(&Value::Integer(30)));
    assert_eq!(rows[0].get("quot"), Some(&Value::Integer(3)));
    assert_eq!(rows[0].get("remainder"), Some(&Value::Integer(1)));
}

#[test]
fn test_arithmetic_in_where() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 5, 3)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES (2, 10, 7)", &mut pager, &mut catalog).unwrap();

    assert_eq!(
        count_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE a + b > 10"),
        1
    );
}

#[test]
fn test_arithmetic_precedence() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1)", &mut pager, &mut catalog).unwrap();

    // 2 + 3 * 4 should be 14, not 20
    let rows = get_rows(
        &mut pager,
        &mut catalog,
        "SELECT 2 + 3 * 4 AS result FROM t",
    );
    assert_eq!(rows[0].get("result"), Some(&Value::Integer(14)));
}

// ============================================================
// BOOLEAN type (alias for TINYINT)
// ============================================================

#[test]
fn test_boolean_type() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, active BOOLEAN DEFAULT 1)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t (id) VALUES (1)", &mut pager, &mut catalog).unwrap();
    execute("INSERT INTO t VALUES (2, 0)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows[0].get("active"), Some(&Value::Integer(1)));
    assert_eq!(rows[1].get("active"), Some(&Value::Integer(0)));

    // DESCRIBE should show TINYINT
    let desc = get_rows(&mut pager, &mut catalog, "DESCRIBE t");
    assert_eq!(desc[1].get("Type"), Some(&Value::Varchar("TINYINT".into())));
}

// ============================================================
// CHECK constraint
// ============================================================

#[test]
fn test_check_constraint_pass() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, age INT CHECK (age > 0))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, 25)", &mut pager, &mut catalog).unwrap();

    assert_eq!(count_rows(&mut pager, &mut catalog, "SELECT * FROM t"), 1);
}

#[test]
fn test_check_constraint_fail() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, age INT CHECK (age > 0))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("INSERT INTO t VALUES (1, -5)", &mut pager, &mut catalog);
    assert!(result.is_err());
}

#[test]
fn test_check_constraint_null_passes() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, age INT CHECK (age > 0))",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // NULL should pass CHECK (MySQL behavior)
    execute("INSERT INTO t VALUES (1, NULL)", &mut pager, &mut catalog).unwrap();

    assert_eq!(count_rows(&mut pager, &mut catalog, "SELECT * FROM t"), 1);
}

// ============================================================
// Unary minus
// ============================================================

#[test]
fn test_negative_numbers() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("INSERT INTO t VALUES (1, -42)", &mut pager, &mut catalog).unwrap();

    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(-42)));
}

// ============================================================
// Combined features
// ============================================================

#[test]
fn test_combined_features() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR NOT NULL,
            status INT DEFAULT 1 CHECK (status >= 0),
            email VARCHAR UNIQUE
        )",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO users (name, status, email) VALUES ('Charlie', 0, 'charlie@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Auto-increment worked
    let rows = get_rows(&mut pager, &mut catalog, "SELECT * FROM users ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[2].get("id"), Some(&Value::Integer(3)));

    // Default status
    assert_eq!(rows[0].get("status"), Some(&Value::Integer(1)));
    assert_eq!(rows[2].get("status"), Some(&Value::Integer(0)));

    // LIKE
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM users WHERE name LIKE '%li%'"
        ),
        2 // Alice, Charlie
    );

    // IS NOT NULL
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM users WHERE email IS NOT NULL"
        ),
        3
    );

    // IN
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM users WHERE id IN (1, 3)"
        ),
        2
    );

    // BETWEEN
    assert_eq!(
        count_rows(
            &mut pager,
            &mut catalog,
            "SELECT * FROM users WHERE id BETWEEN 1 AND 2"
        ),
        2
    );

    // OFFSET
    let rows = get_rows(
        &mut pager,
        &mut catalog,
        "SELECT * FROM users ORDER BY id LIMIT 1 OFFSET 1",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("Bob".into())));

    // DESCRIBE
    let desc = get_rows(&mut pager, &mut catalog, "DESCRIBE users");
    assert_eq!(desc.len(), 4); // id, name, status, email

    // SHOW CREATE TABLE
    let show = get_rows(&mut pager, &mut catalog, "SHOW CREATE TABLE users");
    assert_eq!(show.len(), 1);

    // DROP TABLE
    execute("DROP TABLE users", &mut pager, &mut catalog).unwrap();
    let result = execute("SELECT * FROM users", &mut pager, &mut catalog);
    assert!(result.is_err());
}
