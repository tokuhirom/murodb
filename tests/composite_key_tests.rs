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
