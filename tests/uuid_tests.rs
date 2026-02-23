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

fn get_rows(result: ExecResult) -> Vec<Row> {
    match result {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected Rows, got {:?}", other),
    }
}

#[test]
fn test_uuid_create_table_and_insert() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("id").unwrap().to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("alice".to_string()))
    );
}

#[test]
fn test_uuid_v4_function() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (UUID_V4(), 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);

    // UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    let uuid_str = rows[0].get("id").unwrap().to_string();
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(&uuid_str[14..15], "4"); // version nibble
}

#[test]
fn test_uuid_v7_function() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES (UUID_V7(), 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);

    // UUID v7 format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
    let uuid_str = rows[0].get("id").unwrap().to_string();
    assert_eq!(uuid_str.len(), 36);
    assert_eq!(&uuid_str[14..15], "7"); // version nibble
}

#[test]
fn test_uuid_v7_ordering() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, seq INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    for i in 0..5 {
        execute(
            &format!("INSERT INTO t VALUES (UUID_V7(), {})", i),
            &mut pager,
            &mut catalog,
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let result = execute(
        "SELECT id, seq FROM t ORDER BY id ASC",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 5);

    for i in 0..5 {
        assert_eq!(
            rows[i].get("seq"),
            Some(&Value::Integer(i as i64)),
            "Row {} should have seq={}",
            i,
            i
        );
    }
}

#[test]
fn test_uuid_cast_to_varchar() {
    let (mut pager, mut catalog, _dir) = setup();

    let result = execute(
        "SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(
        rows[0].values[0].1.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    // Cast UUID to VARCHAR
    let result = execute(
        "SELECT CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(
        rows[0].values[0].1.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
}

#[test]
fn test_uuid_cast_to_varbinary() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT CAST(id AS VARBINARY) FROM t",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    match &rows[0].values[0].1 {
        Value::Varbinary(b) => assert_eq!(b.len(), 16),
        other => panic!("Expected Varbinary, got {:?}", other),
    }
}

#[test]
fn test_uuid_index_lookup() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('660e8400-e29b-41d4-a716-446655440000', 'bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT name FROM t WHERE id = '550e8400-e29b-41d4-a716-446655440000'",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("alice".to_string()))
    );
}

#[test]
fn test_uuid_secondary_index() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, uid UUID, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_uid ON t (uid)", &mut pager, &mut catalog).unwrap();
    execute(
        "INSERT INTO t (uid, name) VALUES ('550e8400-e29b-41d4-a716-446655440000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (uid, name) VALUES ('660e8400-e29b-41d4-a716-446655440000', 'bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT name FROM t WHERE uid = '550e8400-e29b-41d4-a716-446655440000'",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("alice".to_string()))
    );
}

#[test]
fn test_uuid_order_by() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('bb000000-0000-0000-0000-000000000000', 'bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('aa000000-0000-0000-0000-000000000000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('cc000000-0000-0000-0000-000000000000', 'charlie')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT id, name FROM t ORDER BY id ASC",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("alice".to_string()))
    );
    assert_eq!(
        rows[1].get("name"),
        Some(&Value::Varchar("bob".to_string()))
    );
    assert_eq!(
        rows[2].get("name"),
        Some(&Value::Varchar("charlie".to_string()))
    );
}

#[test]
fn test_uuid_without_hyphens() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('550e8400e29b41d4a716446655440000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT * FROM t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(
        rows[0].get("id").unwrap().to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
}

#[test]
fn test_uuid_describe_table() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("DESCRIBE t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    // First row should be the id column with UUID type
    assert_eq!(
        rows[0].get("Field"),
        Some(&Value::Varchar("id".to_string()))
    );
    assert_eq!(
        rows[0].get("Type"),
        Some(&Value::Varchar("UUID".to_string()))
    );
}

#[test]
fn test_uuid_select_function_without_table() {
    let (mut pager, mut catalog, _dir) = setup();

    let result = execute("SELECT UUID_V4()", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    let uuid_str = rows[0].values[0].1.to_string();
    assert_eq!(uuid_str.len(), 36);

    let result = execute("SELECT UUID_V7()", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    let uuid_str = rows[0].values[0].1.to_string();
    assert_eq!(uuid_str.len(), 36);
}

#[test]
fn test_uuid_null_handling() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, uid UUID)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (uid) VALUES (NULL)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute("SELECT uid FROM t", &mut pager, &mut catalog).unwrap();
    let rows = get_rows(result);
    assert_eq!(rows[0].get("uid"), Some(&Value::Null));
}

#[test]
fn test_uuid_comparison() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('aa000000-0000-0000-0000-000000000000', 'alice')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t VALUES ('bb000000-0000-0000-0000-000000000000', 'bob')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let result = execute(
        "SELECT name FROM t WHERE id > 'aa000000-0000-0000-0000-000000000000'",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    let rows = get_rows(result);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name"),
        Some(&Value::Varchar("bob".to_string()))
    );
}

#[test]
fn test_uuid_column_serialization_roundtrip() {
    use murodb::schema::column::ColumnDef;
    use murodb::types::DataType;

    let col = ColumnDef::new("id", DataType::Uuid).primary_key();
    let bytes = col.serialize();
    let (col2, _) = ColumnDef::deserialize(&bytes).unwrap();
    assert_eq!(col2.name, "id");
    assert_eq!(col2.data_type, DataType::Uuid);
    assert!(col2.is_primary_key);
}
