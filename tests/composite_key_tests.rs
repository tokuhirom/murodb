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

fn exec(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> ExecResult {
    execute(sql, pager, catalog).unwrap()
}

fn exec_err(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> String {
    execute(sql, pager, catalog).unwrap_err().to_string()
}

fn get_rows(result: ExecResult) -> Vec<Vec<(String, Value)>> {
    match result {
        ExecResult::Rows(rows) => rows.into_iter().map(|r| r.values).collect(),
        _ => panic!("Expected Rows"),
    }
}

// --- Composite PRIMARY KEY ---

#[test]
fn test_composite_pk_create_insert_select() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE orders (
            user_id INT,
            order_id INT,
            amount INT,
            PRIMARY KEY (user_id, order_id)
        )",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO orders (user_id, order_id, amount) VALUES (1, 1, 100)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO orders (user_id, order_id, amount) VALUES (1, 2, 200)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO orders (user_id, order_id, amount) VALUES (2, 1, 300)",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT user_id, order_id, amount FROM orders",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_composite_pk_uniqueness_violation() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 100)",
        &mut pager,
        &mut catalog,
    );

    // Same (a, b) should fail
    let err = exec_err(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 200)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate primary key"), "Got: {}", err);

    // Different (a, b) should succeed
    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 3, 300)",
        &mut pager,
        &mut catalog,
    );
}

#[test]
fn test_composite_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c VARCHAR, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 1, 'one-one')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 'one-two')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c) VALUES (2, 1, 'two-one')",
        &mut pager,
        &mut catalog,
    );

    // Seek with both PK columns
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 1 AND b = 2",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Varchar("one-two".to_string()));
}

#[test]
fn test_composite_pk_order_by() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b) VALUES (2, 1)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b) VALUES (1, 2)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b) VALUES (1, 1)",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT a, b FROM t ORDER BY a, b",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows[0][0].1, Value::Integer(1));
    assert_eq!(rows[0][1].1, Value::Integer(1));
    assert_eq!(rows[1][0].1, Value::Integer(1));
    assert_eq!(rows[1][1].1, Value::Integer(2));
    assert_eq!(rows[2][0].1, Value::Integer(2));
    assert_eq!(rows[2][1].1, Value::Integer(1));
}

// --- Composite UNIQUE ---

#[test]
fn test_composite_unique_constraint() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (
            id BIGINT PRIMARY KEY,
            a INT,
            b INT,
            UNIQUE (a, b)
        )",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (id, a, b) VALUES (1, 10, 20)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b) VALUES (2, 10, 30)",
        &mut pager,
        &mut catalog,
    );

    // Same (a, b) = (10, 20) should fail
    let err = exec_err(
        "INSERT INTO t (id, a, b) VALUES (3, 10, 20)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate"), "Got: {}", err);
}

#[test]
fn test_composite_unique_with_null() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (
            id BIGINT PRIMARY KEY,
            a INT,
            b INT,
            UNIQUE (a, b)
        )",
        &mut pager,
        &mut catalog,
    );

    // NULL in unique columns should not cause violation (SQL standard)
    exec(
        "INSERT INTO t (id, a, b) VALUES (1, 10, NULL)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b) VALUES (2, 10, NULL)",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec("SELECT * FROM t", &mut pager, &mut catalog));
    assert_eq!(rows.len(), 2);
}

// --- Composite INDEX ---

#[test]
fn test_composite_index() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    exec("CREATE INDEX idx_ab ON t (a, b)", &mut pager, &mut catalog);

    exec(
        "INSERT INTO t (id, a, b, c) VALUES (1, 10, 20, 'hello')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (2, 10, 30, 'world')",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 10 AND b = 20",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Varchar("hello".to_string()));
}

#[test]
fn test_composite_unique_index() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "CREATE UNIQUE INDEX idx_ab ON t (a, b)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (id, a, b) VALUES (1, 10, 20)",
        &mut pager,
        &mut catalog,
    );

    let err = exec_err(
        "INSERT INTO t (id, a, b) VALUES (2, 10, 20)",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate"), "Got: {}", err);

    // Different combo should work
    exec(
        "INSERT INTO t (id, a, b) VALUES (2, 10, 30)",
        &mut pager,
        &mut catalog,
    );
}

// --- Column-level + table-level PK conflict ---

#[test]
fn test_column_level_and_table_level_pk_conflict() {
    let (mut pager, mut catalog, _dir) = setup();

    let err = exec_err(
        "CREATE TABLE t (
            id BIGINT PRIMARY KEY,
            a INT,
            PRIMARY KEY (id, a)
        )",
        &mut pager,
        &mut catalog,
    );
    assert!(
        err.contains("both column-level and table-level PRIMARY KEY"),
        "Got: {}",
        err
    );
}

// --- SHOW CREATE TABLE / DESCRIBE ---

#[test]
fn test_show_create_table_composite_pk() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c VARCHAR, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec("SHOW CREATE TABLE t", &mut pager, &mut catalog));
    let create_sql = match &rows[0][1].1 {
        Value::Varchar(s) => s.clone(),
        _ => panic!("Expected varchar"),
    };
    assert!(
        create_sql.contains("PRIMARY KEY (a, b)"),
        "Got: {}",
        create_sql
    );
}

#[test]
fn test_describe_composite_pk() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c VARCHAR, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec("DESCRIBE t", &mut pager, &mut catalog));
    // a and b should have Key = PRI
    let a_key = &rows[0][3].1;
    let b_key = &rows[1][3].1;
    assert_eq!(*a_key, Value::Varchar("PRI".to_string()));
    assert_eq!(*b_key, Value::Varchar("PRI".to_string()));
}

// --- UPDATE / DELETE with composite PK ---

#[test]
fn test_update_with_composite_pk() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 1, 100)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 200)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "UPDATE t SET c = 999 WHERE a = 1 AND b = 1",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 1 AND b = 1",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Integer(999));

    // Other row unchanged
    let rows2 = get_rows(exec(
        "SELECT c FROM t WHERE a = 1 AND b = 2",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows2[0][0].1, Value::Integer(200));
}

#[test]
fn test_delete_with_composite_pk() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 1, 100)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c) VALUES (1, 2, 200)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c) VALUES (2, 1, 300)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "DELETE FROM t WHERE a = 1 AND b = 1",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec("SELECT * FROM t", &mut pager, &mut catalog));
    assert_eq!(rows.len(), 2);
}

// --- ALTER TABLE DROP COLUMN on composite PK column ---

#[test]
fn test_alter_table_drop_composite_pk_column() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))",
        &mut pager,
        &mut catalog,
    );

    let err = exec_err("ALTER TABLE t DROP COLUMN a", &mut pager, &mut catalog);
    assert!(err.contains("PRIMARY KEY"), "Got: {}", err);
}

// --- Three-column composite PK ---

#[test]
fn test_three_column_composite_pk() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (a INT, b INT, c INT, d VARCHAR, PRIMARY KEY (a, b, c))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (a, b, c, d) VALUES (1, 1, 1, 'aaa')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (a, b, c, d) VALUES (1, 1, 2, 'bbb')",
        &mut pager,
        &mut catalog,
    );

    // Same triple should fail
    let err = exec_err(
        "INSERT INTO t (a, b, c, d) VALUES (1, 1, 1, 'ccc')",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate"), "Got: {}", err);

    let rows = get_rows(exec(
        "SELECT d FROM t WHERE a = 1 AND b = 1 AND c = 2",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Varchar("bbb".to_string()));
}

// --- Composite PK with VARCHAR ---

#[test]
fn test_composite_pk_with_varchar() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (region VARCHAR, id INT, data VARCHAR, PRIMARY KEY (region, id))",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (region, id, data) VALUES ('us', 1, 'us-data')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (region, id, data) VALUES ('eu', 1, 'eu-data')",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT data FROM t WHERE region = 'eu' AND id = 1",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Varchar("eu-data".to_string()));
}

// --- Bug fix: CREATE TABLE atomicity (constraint validation before catalog write) ---

#[test]
fn test_create_table_with_invalid_pk_column_does_not_leave_table() {
    let (mut pager, mut catalog, _dir) = setup();

    // PK references a non-existent column
    let err = exec_err(
        "CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, nonexistent))",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("not found"), "Got: {}", err);

    // Table should NOT exist in catalog
    let rows = get_rows(exec("SHOW TABLES", &mut pager, &mut catalog));
    assert!(rows.is_empty(), "Table should not have been created");
}

#[test]
fn test_create_table_conflicting_pk_does_not_leave_table() {
    let (mut pager, mut catalog, _dir) = setup();

    let err = exec_err(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, PRIMARY KEY (id, a))",
        &mut pager,
        &mut catalog,
    );
    assert!(
        err.contains("both column-level and table-level PRIMARY KEY"),
        "Got: {}",
        err
    );

    let rows = get_rows(exec("SHOW TABLES", &mut pager, &mut catalog));
    assert!(rows.is_empty(), "Table should not have been created");
}

// --- Bug fix: UNIQUE constraint column existence validation ---

#[test]
fn test_unique_constraint_with_invalid_column_errors() {
    let (mut pager, mut catalog, _dir) = setup();

    let err = exec_err(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, UNIQUE (a, nonexistent))",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("not found"), "Got: {}", err);
}

#[test]
fn test_duplicate_unique_constraint_does_not_leave_table() {
    let (mut pager, mut catalog, _dir) = setup();

    // Column-level UNIQUE(a) and table-level UNIQUE(a) produce the same auto index name
    let err = exec_err(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT UNIQUE, UNIQUE (a))",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate UNIQUE"), "Got: {}", err);

    // Table should NOT exist
    let rows = get_rows(exec("SHOW TABLES", &mut pager, &mut catalog));
    assert!(rows.is_empty(), "Table should not have been created");
}

#[test]
fn test_duplicate_table_level_unique_does_not_leave_table() {
    let (mut pager, mut catalog, _dir) = setup();

    // Two identical table-level UNIQUE(a) constraints
    let err = exec_err(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, UNIQUE (a), UNIQUE (a))",
        &mut pager,
        &mut catalog,
    );
    assert!(err.contains("Duplicate UNIQUE"), "Got: {}", err);

    let rows = get_rows(exec("SHOW TABLES", &mut pager, &mut catalog));
    assert!(rows.is_empty(), "Table should not have been created");
}

// --- Bug fix: Non-unique composite index with duplicate values ---

#[test]
fn test_non_unique_index_duplicate_values() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    exec("CREATE INDEX idx_ab ON t (a, b)", &mut pager, &mut catalog);

    // Insert multiple rows with same (a, b) values
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (1, 10, 20, 'first')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (2, 10, 20, 'second')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (3, 10, 20, 'third')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (4, 10, 30, 'other')",
        &mut pager,
        &mut catalog,
    );

    // IndexSeek should return ALL 3 rows with (a=10, b=20)
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 10 AND b = 20",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 3, "Should find all 3 duplicate-key rows");

    // Full scan should return all 4 rows
    let all_rows = get_rows(exec("SELECT * FROM t", &mut pager, &mut catalog));
    assert_eq!(all_rows.len(), 4);
}

#[test]
fn test_composite_index_prefix_equality_with_range_on_last_column() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );
    exec("CREATE INDEX idx_ab ON t (a, b)", &mut pager, &mut catalog);

    for i in 1..=8 {
        let sql = format!(
            "INSERT INTO t (id, a, b, c) VALUES ({}, 10, {}, 'v{}')",
            i, i, i
        );
        exec(&sql, &mut pager, &mut catalog);
    }
    exec(
        "INSERT INTO t (id, a, b, c) VALUES (100, 11, 3, 'other_a')",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT id FROM t WHERE a = 10 AND b BETWEEN 3 AND 5 ORDER BY id",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0].1, Value::Integer(3));
    assert_eq!(rows[1][0].1, Value::Integer(4));
    assert_eq!(rows[2][0].1, Value::Integer(5));
}

#[test]
fn test_non_unique_single_column_index_duplicates() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, category INT, name VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "CREATE INDEX idx_cat ON t (category)",
        &mut pager,
        &mut catalog,
    );

    exec(
        "INSERT INTO t (id, category, name) VALUES (1, 1, 'Alice')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, category, name) VALUES (2, 1, 'Bob')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, category, name) VALUES (3, 2, 'Charlie')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, category, name) VALUES (4, 1, 'Diana')",
        &mut pager,
        &mut catalog,
    );

    // Should find all 3 rows with category=1
    let rows = get_rows(exec(
        "SELECT name FROM t WHERE category = 1",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 3, "Should find all 3 rows with category=1");
}

#[test]
fn test_non_unique_varchar_range_seek_does_not_early_stop() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, s VARCHAR)",
        &mut pager,
        &mut catalog,
    );
    exec("CREATE INDEX idx_s ON t (s)", &mut pager, &mut catalog);

    // For non-unique keys, physical order is by (s || pk). With variable-length s,
    // rows with s='a' can appear after s='ab'/'ac' depending on pk bytes.
    exec(
        "INSERT INTO t (id, s) VALUES (1, 'ab')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, s) VALUES (2, 'ac')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, s) VALUES (3, 'a')",
        &mut pager,
        &mut catalog,
    );

    let rows = get_rows(exec(
        "SELECT id FROM t WHERE s < 'ab' ORDER BY id",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(
        rows.len(),
        1,
        "range seek must not drop trailing s='a' rows"
    );
    assert_eq!(rows[0][0].1, Value::Integer(3));
}

#[test]
fn test_range_seek_does_not_plan_row_dependent_bound() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    );
    exec("CREATE INDEX idx_a ON t (a)", &mut pager, &mut catalog);

    exec(
        "INSERT INTO t (id, a, b) VALUES (1, 5, 3)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b) VALUES (2, 2, 4)",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, b) VALUES (3, 7, 7)",
        &mut pager,
        &mut catalog,
    );

    // a > b is row-dependent and must fall back to row-wise predicate evaluation.
    let rows = get_rows(exec(
        "SELECT id FROM t WHERE a > b ORDER BY id",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Integer(1));
}

#[test]
fn test_non_unique_index_delete_preserves_others() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    exec("CREATE INDEX idx_a ON t (a)", &mut pager, &mut catalog);

    exec(
        "INSERT INTO t (id, a, c) VALUES (1, 10, 'first')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, c) VALUES (2, 10, 'second')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, c) VALUES (3, 10, 'third')",
        &mut pager,
        &mut catalog,
    );

    // Delete one row
    exec("DELETE FROM t WHERE id = 2", &mut pager, &mut catalog);

    // Should still find the other 2 via index
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 10",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 2, "Should find 2 remaining rows with a=10");
}

#[test]
fn test_non_unique_index_update_preserves_others() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    exec("CREATE INDEX idx_a ON t (a)", &mut pager, &mut catalog);

    exec(
        "INSERT INTO t (id, a, c) VALUES (1, 10, 'first')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, c) VALUES (2, 10, 'second')",
        &mut pager,
        &mut catalog,
    );

    // Update one row to change its indexed value
    exec("UPDATE t SET a = 20 WHERE id = 1", &mut pager, &mut catalog);

    // Should find only 1 row with a=10 now
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 10",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1, "Should find 1 row with a=10");
    assert_eq!(rows[0][0].1, Value::Varchar("second".to_string()));

    // Should find 1 row with a=20
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 20",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 1, "Should find 1 row with a=20");
    assert_eq!(rows[0][0].1, Value::Varchar("first".to_string()));
}

#[test]
fn test_create_index_on_existing_data_with_duplicates() {
    let (mut pager, mut catalog, _dir) = setup();

    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, c VARCHAR)",
        &mut pager,
        &mut catalog,
    );

    // Insert data BEFORE creating the index
    exec(
        "INSERT INTO t (id, a, c) VALUES (1, 10, 'first')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, c) VALUES (2, 10, 'second')",
        &mut pager,
        &mut catalog,
    );
    exec(
        "INSERT INTO t (id, a, c) VALUES (3, 20, 'third')",
        &mut pager,
        &mut catalog,
    );

    // Create index on existing data with duplicate values
    exec("CREATE INDEX idx_a ON t (a)", &mut pager, &mut catalog);

    // Index should find both rows with a=10
    let rows = get_rows(exec(
        "SELECT c FROM t WHERE a = 10",
        &mut pager,
        &mut catalog,
    ));
    assert_eq!(rows.len(), 2, "Should find both rows with a=10 via index");
}
