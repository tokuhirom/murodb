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

fn query_rows(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> Vec<Vec<Value>> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows
            .into_iter()
            .map(|r| r.values.into_iter().map(|(_, v)| v).collect())
            .collect(),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

fn affected_rows(sql: &str, pager: &mut Pager, catalog: &mut SystemCatalog) -> u64 {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::RowsAffected(n) => n,
        other => panic!("Expected RowsAffected, got {:?}", other),
    }
}

// ---- INSERT ... ON DUPLICATE KEY UPDATE ----

#[test]
fn test_on_duplicate_key_update_basic() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, val INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Initial insert
    let n = affected_rows(
        "INSERT INTO t (id, name, val) VALUES (1, 'Alice', 10)",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 1);

    // Duplicate key → update
    let n = affected_rows(
        "INSERT INTO t (id, name, val) VALUES (1, 'Bob', 20) ON DUPLICATE KEY UPDATE name = 'Bob', val = 20",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 2); // MySQL convention: 2 for update

    let rows = query_rows(
        "SELECT id, name, val FROM t WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Varchar("Bob".to_string()));
    assert_eq!(rows[0][2], Value::Integer(20));
}

#[test]
fn test_on_duplicate_key_update_no_conflict() {
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

    // No conflict → normal insert
    let n = affected_rows(
        "INSERT INTO t (id, name) VALUES (2, 'Bob') ON DUPLICATE KEY UPDATE name = 'Updated'",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 1);

    let rows = query_rows(
        "SELECT id, name FROM t ORDER BY id",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1], Value::Varchar("Bob".to_string())); // Not 'Updated'
}

#[test]
fn test_on_duplicate_key_update_with_expression() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE counters (id BIGINT PRIMARY KEY, cnt INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO counters (id, cnt) VALUES (1, 1)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Update with expression referencing existing value
    affected_rows(
        "INSERT INTO counters (id, cnt) VALUES (1, 1) ON DUPLICATE KEY UPDATE cnt = cnt + 1",
        &mut pager,
        &mut catalog,
    );

    let rows = query_rows(
        "SELECT id, cnt FROM counters WHERE id = 1",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows[0][1], Value::Integer(2));
}

// ---- REPLACE INTO ----

#[test]
fn test_replace_into_no_conflict() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let n = affected_rows(
        "REPLACE INTO t (id, name) VALUES (1, 'Alice')",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 1);

    let rows = query_rows("SELECT id, name FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Varchar("Alice".to_string()));
}

#[test]
fn test_replace_into_with_conflict() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, val INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t (id, name, val) VALUES (1, 'Alice', 10)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Replace: delete existing + insert new
    let n = affected_rows(
        "REPLACE INTO t (id, name, val) VALUES (1, 'Bob', 20)",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 1);

    let rows = query_rows("SELECT id, name, val FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Varchar("Bob".to_string()));
    assert_eq!(rows[0][2], Value::Integer(20));
}

#[test]
fn test_replace_into_multiple_rows() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // Replace both existing rows
    let n = affected_rows(
        "REPLACE INTO t (id, name) VALUES (1, 'Alicia'), (2, 'Bobby'), (3, 'Charlie')",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 3);

    let rows = query_rows(
        "SELECT id, name FROM t ORDER BY id",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Varchar("Alicia".to_string()));
    assert_eq!(rows[1][1], Value::Varchar("Bobby".to_string()));
    assert_eq!(rows[2][1], Value::Varchar("Charlie".to_string()));
}

#[test]
fn test_replace_into_unique_index_conflict() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, email VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "CREATE UNIQUE INDEX idx_email ON t(email)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute(
        "INSERT INTO t (id, email) VALUES (1, 'alice@example.com')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    // REPLACE with same unique index value but different PK
    // Should delete row with id=1 (unique conflict on email) and insert new row
    let n = affected_rows(
        "REPLACE INTO t (id, email) VALUES (2, 'alice@example.com')",
        &mut pager,
        &mut catalog,
    );
    assert_eq!(n, 1);

    let rows = query_rows("SELECT id, email FROM t", &mut pager, &mut catalog);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[0][1], Value::Varchar("alice@example.com".to_string()));
}
