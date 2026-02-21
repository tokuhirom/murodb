/// Integration tests for Phase 3: Aggregation & Grouping.
use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::{execute, ExecResult, Row};
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

fn query_rows(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> Vec<Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    }
}

fn query_one(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> Value {
    let rows = query_rows(pager, catalog, sql);
    assert!(!rows.is_empty(), "Expected at least one row");
    rows[0].values[0].1.clone()
}

fn setup_sample_data() -> (Pager, SystemCatalog, TempDir) {
    let (mut pager, mut catalog, dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, category VARCHAR, amount INT, status VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO orders VALUES (1, 'A', 100, 'active')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO orders VALUES (2, 'B', 200, 'active')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO orders VALUES (3, 'A', 150, 'inactive')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO orders VALUES (4, 'B', 300, 'active')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO orders VALUES (5, 'A', 250, 'active')",
    );
    (pager, catalog, dir)
}

// --- COUNT tests ---

#[test]
fn test_count_star() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT COUNT(*) AS cnt FROM orders",
    );
    assert_eq!(val, Value::Integer(5));
}

#[test]
fn test_count_column() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, 30)");

    // COUNT(col) skips NULLs
    let val = query_one(&mut pager, &mut catalog, "SELECT COUNT(val) AS cnt FROM t");
    assert_eq!(val, Value::Integer(2));

    // COUNT(*) counts all rows
    let val = query_one(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(val, Value::Integer(3));
}

#[test]
fn test_count_distinct() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT COUNT(DISTINCT category) AS cnt FROM orders",
    );
    assert_eq!(val, Value::Integer(2));
}

// --- SUM tests ---

#[test]
fn test_sum() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT SUM(amount) AS total FROM orders",
    );
    assert_eq!(val, Value::Integer(1000)); // 100+200+150+300+250
}

#[test]
fn test_sum_null_handling() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (1, 10)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (2, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t VALUES (3, 30)");

    let val = query_one(&mut pager, &mut catalog, "SELECT SUM(val) AS s FROM t");
    assert_eq!(val, Value::Integer(40)); // NULL is skipped

    // All NULLs -> NULL
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t2 VALUES (1, NULL)");
    exec(&mut pager, &mut catalog, "INSERT INTO t2 VALUES (2, NULL)");
    let val = query_one(&mut pager, &mut catalog, "SELECT SUM(val) AS s FROM t2");
    assert_eq!(val, Value::Null);
}

// --- AVG tests ---

#[test]
fn test_avg() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT AVG(amount) AS avg_amount FROM orders",
    );
    assert_eq!(val, Value::Integer(200)); // 1000/5 = 200
}

// --- MIN / MAX tests ---

#[test]
fn test_min_max() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT MIN(amount) AS min_amt FROM orders",
    );
    assert_eq!(val, Value::Integer(100));

    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT MAX(amount) AS max_amt FROM orders",
    );
    assert_eq!(val, Value::Integer(300));
}

#[test]
fn test_min_max_varchar() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT MIN(category) AS min_cat FROM orders",
    );
    assert_eq!(val, Value::Varchar("A".to_string()));

    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT MAX(category) AS max_cat FROM orders",
    );
    assert_eq!(val, Value::Varchar("B".to_string()));
}

// --- GROUP BY tests ---

#[test]
fn test_group_by_single_column() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category ORDER BY category",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(3)));
    assert_eq!(
        rows[1].get("category"),
        Some(&Value::Varchar("B".to_string()))
    );
    assert_eq!(rows[1].get("cnt"), Some(&Value::Integer(2)));
}

#[test]
fn test_group_by_with_sum() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, SUM(amount) AS total FROM orders GROUP BY category ORDER BY category",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("total"), Some(&Value::Integer(500))); // A: 100+150+250
    assert_eq!(rows[1].get("total"), Some(&Value::Integer(500))); // B: 200+300
}

#[test]
fn test_group_by_multiple_columns() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, status, COUNT(*) AS cnt FROM orders GROUP BY category, status ORDER BY category, status",
    );
    // A+active=2, A+inactive=1, B+active=2
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(
        rows[0].get("status"),
        Some(&Value::Varchar("active".to_string()))
    );
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(2)));
    assert_eq!(
        rows[1].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(
        rows[1].get("status"),
        Some(&Value::Varchar("inactive".to_string()))
    );
    assert_eq!(rows[1].get("cnt"), Some(&Value::Integer(1)));
    assert_eq!(
        rows[2].get("category"),
        Some(&Value::Varchar("B".to_string()))
    );
    assert_eq!(rows[2].get("cnt"), Some(&Value::Integer(2)));
}

// --- HAVING tests ---

#[test]
fn test_having() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, SUM(amount) AS total FROM orders GROUP BY category HAVING SUM(amount) >= 500 ORDER BY category",
    );
    // Both groups have total=500
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_having_filters() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category HAVING COUNT(*) > 2",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(3)));
}

// --- SELECT DISTINCT tests ---

#[test]
fn test_select_distinct() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT DISTINCT category FROM orders ORDER BY category",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(
        rows[1].get("category"),
        Some(&Value::Varchar("B".to_string()))
    );
}

#[test]
fn test_select_distinct_multiple_columns() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT DISTINCT category, status FROM orders ORDER BY category, status",
    );
    assert_eq!(rows.len(), 3); // (A,active), (A,inactive), (B,active)
}

// --- Empty table tests ---

#[test]
fn test_count_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let val = query_one(&mut pager, &mut catalog, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(val, Value::Integer(0));
}

#[test]
fn test_sum_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let val = query_one(&mut pager, &mut catalog, "SELECT SUM(val) AS s FROM t");
    assert_eq!(val, Value::Null);
}

#[test]
fn test_avg_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let val = query_one(&mut pager, &mut catalog, "SELECT AVG(val) AS a FROM t");
    assert_eq!(val, Value::Null);
}

#[test]
fn test_min_max_empty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );

    let val = query_one(&mut pager, &mut catalog, "SELECT MIN(val) AS m FROM t");
    assert_eq!(val, Value::Null);

    let val = query_one(&mut pager, &mut catalog, "SELECT MAX(val) AS m FROM t");
    assert_eq!(val, Value::Null);
}

#[test]
fn test_group_by_no_matching_rows() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, COUNT(*) AS cnt FROM orders WHERE amount > 9999 GROUP BY category",
    );
    assert_eq!(rows.len(), 0);
}

// --- NULL handling in GROUP BY ---

#[test]
fn test_group_by_null_group() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, category VARCHAR, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (1, 'A', 10)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (2, NULL, 20)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (3, 'A', 30)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t VALUES (4, NULL, 40)",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, SUM(val) AS total FROM t GROUP BY category ORDER BY category",
    );
    // NULL group should exist
    assert_eq!(rows.len(), 2);
    // NULL sorts first in our ordering
    assert_eq!(rows[0].get("category"), Some(&Value::Null));
    assert_eq!(rows[0].get("total"), Some(&Value::Integer(60))); // 20+40
    assert_eq!(
        rows[1].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
    assert_eq!(rows[1].get("total"), Some(&Value::Integer(40))); // 10+30
}

// --- Multiple aggregates in one query ---

#[test]
fn test_multiple_aggregates() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT COUNT(*) AS cnt, SUM(amount) AS total, MIN(amount) AS min_a, MAX(amount) AS max_a, AVG(amount) AS avg_a FROM orders",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("cnt"), Some(&Value::Integer(5)));
    assert_eq!(rows[0].get("total"), Some(&Value::Integer(1000)));
    assert_eq!(rows[0].get("min_a"), Some(&Value::Integer(100)));
    assert_eq!(rows[0].get("max_a"), Some(&Value::Integer(300)));
    assert_eq!(rows[0].get("avg_a"), Some(&Value::Integer(200)));
}

// --- Aggregates with WHERE ---

#[test]
fn test_aggregate_with_where() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let val = query_one(
        &mut pager,
        &mut catalog,
        "SELECT SUM(amount) AS total FROM orders WHERE category = 'A'",
    );
    assert_eq!(val, Value::Integer(500)); // 100+150+250
}

// --- GROUP BY with LIMIT/OFFSET ---

#[test]
fn test_group_by_with_limit() {
    let (mut pager, mut catalog, _dir) = setup_sample_data();
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT category, COUNT(*) AS cnt FROM orders GROUP BY category ORDER BY category LIMIT 1",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("category"),
        Some(&Value::Varchar("A".to_string()))
    );
}
