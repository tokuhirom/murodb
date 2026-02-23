use murodb::sql::executor::ExecResult;
use murodb::types::Value;
use murodb::Database;
use tempfile::TempDir;

fn setup_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("prepared.db");
    let db = Database::create_plaintext(&db_path).unwrap();
    (db, dir)
}

#[test]
fn test_prepared_insert_and_query_reuse() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, age INT)")
        .unwrap();

    let insert = db
        .prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
        .unwrap();

    for (id, name, age) in [(1, "alice", 30), (2, "bob", 40), (3, "carol", 50)] {
        db.execute_prepared(
            &insert,
            &[
                Value::Integer(id),
                Value::Varchar(name.to_string()),
                Value::Integer(age),
            ],
        )
        .unwrap();
    }

    let select = db
        .prepare("SELECT name, age FROM users WHERE id = ?")
        .unwrap();
    let rows = db.query_prepared(&select, &[Value::Integer(2)]).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("bob".to_string()))
    );
    assert_eq!(rows[0].get("age"), Some(&Value::Integer(40)));
}

#[test]
fn test_prepared_supports_null() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE notes (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();

    db.execute_params(
        "INSERT INTO notes (id, body) VALUES (?, ?)",
        &[Value::Integer(1), Value::Null],
    )
    .unwrap();

    let rows = db
        .query_params("SELECT body FROM notes WHERE id = ?", &[Value::Integer(1)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("body"), Some(&Value::Null));
}

#[test]
fn test_prepared_type_coercion_date_from_string() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, d DATE)")
        .unwrap();

    db.execute_params(
        "INSERT INTO events (id, d) VALUES (?, ?)",
        &[Value::Integer(1), Value::Varchar("2026-02-23".to_string())],
    )
    .unwrap();

    let rows = db
        .query_params("SELECT d FROM events WHERE id = ?", &[Value::Integer(1)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("d"), Some(&Value::Date(20260223)));
}

#[test]
fn test_prepared_parameter_count_mismatch_is_error() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
        .unwrap();

    let stmt = db.prepare("INSERT INTO t (id, v) VALUES (?, ?)").unwrap();
    let err = db
        .execute_prepared(&stmt, &[Value::Integer(1)])
        .unwrap_err();
    assert!(format!("{err}").contains("expects 2 parameters"));
}

#[test]
fn test_direct_execute_with_placeholder_is_rejected() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
        .unwrap();

    let err = db
        .execute("INSERT INTO t (id, v) VALUES (?, ?)")
        .unwrap_err();
    assert!(format!("{err}").contains("use prepare()/execute_prepared()"));
}

#[test]
fn test_query_prepared_rejects_write_statement() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    let stmt = db.prepare("INSERT INTO t VALUES (?)").unwrap();

    let err = db.query_prepared(&stmt, &[Value::Integer(1)]).unwrap_err();
    assert!(format!("{err}").contains("read-only SQL only"));
}

#[test]
fn test_prepared_rows_affected() {
    let (mut db, _dir) = setup_db();

    db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v INT)")
        .unwrap();
    let stmt = db
        .prepare("INSERT INTO t (id, v) VALUES (?, ?), (?, ?)")
        .unwrap();

    let result = db
        .execute_prepared(
            &stmt,
            &[
                Value::Integer(1),
                Value::Integer(10),
                Value::Integer(2),
                Value::Integer(20),
            ],
        )
        .unwrap();

    match result {
        ExecResult::RowsAffected(n) => assert_eq!(n, 2),
        other => panic!("expected RowsAffected, got {other:?}"),
    }
}
