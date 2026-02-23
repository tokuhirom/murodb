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

fn exec(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) {
    execute(sql, pager, catalog).unwrap();
}

fn query_rows(
    sql: &str,
    pager: &mut Pager,
    catalog: &mut SystemCatalog,
) -> Vec<Vec<(String, Value)>> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows.into_iter().map(|r| r.values).collect(),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

fn get_explain_access_type(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> String {
    let rows = query_rows(sql, pager, catalog);
    assert!(!rows.is_empty(), "EXPLAIN returned no rows");
    let access_type = rows[0]
        .iter()
        .find(|(name, _)| name == "type")
        .expect("No type column in EXPLAIN output");
    match &access_type.1 {
        Value::Varchar(s) => s.clone(),
        other => panic!("Expected Varchar for access_type, got {:?}", other),
    }
}

fn get_explain_key(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> String {
    let rows = query_rows(sql, pager, catalog);
    assert!(!rows.is_empty(), "EXPLAIN returned no rows");
    let key = rows[0]
        .iter()
        .find(|(name, _)| name == "key")
        .expect("No key column in EXPLAIN output");
    match &key.1 {
        Value::Varchar(s) => s.clone(),
        other => panic!("Expected Varchar for key, got {:?}", other),
    }
}

fn setup_test_table(pager: &mut Pager, catalog: &mut SystemCatalog) {
    exec(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, age BIGINT)",
        pager,
        catalog,
    );
    exec("CREATE INDEX idx_age ON t (age)", pager, catalog);
    exec("INSERT INTO t VALUES (1, 'alice', 10)", pager, catalog);
    exec("INSERT INTO t VALUES (2, 'bob', 20)", pager, catalog);
    exec("INSERT INTO t VALUES (3, 'charlie', 30)", pager, catalog);
}

// ==================== Parser tests ====================

#[test]
fn test_parse_force_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    // FORCE INDEX should parse and execute without error
    let rows = query_rows(
        "SELECT * FROM t FORCE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_parse_use_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    let rows = query_rows(
        "SELECT * FROM t USE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_parse_ignore_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    let rows = query_rows(
        "SELECT * FROM t IGNORE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_parse_multiple_index_names() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);
    exec(
        "CREATE INDEX idx_name ON t (name)",
        &mut pager,
        &mut catalog,
    );

    // Multiple index names in a single hint
    let rows = query_rows(
        "SELECT * FROM t FORCE INDEX (idx_age, idx_name) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
}

// ==================== EXPLAIN / planner tests ====================

#[test]
fn test_force_index_uses_specified_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t FORCE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ref", "FORCE INDEX should use IndexSeek (ref)");

    let key = get_explain_key(
        "EXPLAIN SELECT * FROM t FORCE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(key, "idx_age");
}

#[test]
fn test_force_index_skips_pk_seek() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    // Without FORCE INDEX, id = 1 would use PK seek (const)
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "const");

    // With FORCE INDEX (idx_age), PK seek should be skipped, fallback to FullScan
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t FORCE INDEX (idx_age) WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ALL", "FORCE INDEX should skip PK seek");
}

#[test]
fn test_ignore_index_falls_back_to_full_scan() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    // Without IGNORE INDEX, age = 20 uses idx_age
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ref");

    // With IGNORE INDEX (idx_age), should fall back to FullScan
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t IGNORE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ALL", "IGNORE INDEX should cause FullScan");
}

#[test]
fn test_use_index_allows_specified_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t USE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ref");
}

#[test]
fn test_force_index_fallback_to_full_scan() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    // FORCE INDEX with an index that can't be used for this query
    // should fallback to FullScan (MySQL behavior)
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t FORCE INDEX (idx_age) WHERE name = 'alice'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(
        access, "ALL",
        "FORCE INDEX with unusable index should FullScan"
    );
}

// ==================== UPDATE / DELETE with hints ====================

#[test]
fn test_update_with_force_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    exec(
        "UPDATE t FORCE INDEX (idx_age) SET name = 'updated' WHERE age = 20",
        &mut pager,
        &mut catalog,
    );

    let rows = query_rows(
        "SELECT name FROM t WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].1, Value::Varchar("updated".to_string()));
}

#[test]
fn test_delete_with_ignore_index() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    exec(
        "DELETE FROM t IGNORE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );

    let rows = query_rows("SELECT * FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 2);
}

// ==================== Index hint with alias ====================

#[test]
fn test_force_index_with_alias() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    // MySQL syntax: FROM tbl_name [AS alias] [index_hint]
    let rows = query_rows(
        "SELECT * FROM t AS x FORCE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    // Alias without AS keyword
    let rows = query_rows(
        "SELECT * FROM t x FORCE INDEX (idx_age) WHERE age = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);

    // EXPLAIN should show correct plan with alias + hint
    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t AS x FORCE INDEX (idx_age) WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "ALL", "FORCE INDEX with alias should skip PK seek");
}

// ==================== Range query with hints ====================

#[test]
fn test_force_index_range_query() {
    let (mut pager, mut catalog, _dir) = setup();
    setup_test_table(&mut pager, &mut catalog);

    let access = get_explain_access_type(
        "EXPLAIN SELECT * FROM t FORCE INDEX (idx_age) WHERE age > 15",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(access, "range", "FORCE INDEX should use IndexRangeSeek");
}
