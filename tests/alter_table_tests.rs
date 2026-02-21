/// Integration tests for Phase 4: ALTER TABLE & RENAME TABLE.
use murodb::btree::ops::BTree;
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

fn exec_err(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> String {
    execute(sql, pager, catalog).unwrap_err().to_string()
}

fn query_rows(pager: &mut Pager, catalog: &mut SystemCatalog, sql: &str) -> Vec<Row> {
    match execute(sql, pager, catalog).unwrap() {
        ExecResult::Rows(rows) => rows,
        other => panic!("Expected rows, got {:?}", other),
    }
}

// ─── ADD COLUMN ────────────────────────────────────────────────

#[test]
fn test_add_column_nullable() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'bob')",
    );

    // Add a new nullable column
    exec(&mut pager, &mut catalog, "ALTER TABLE t ADD COLUMN age INT");

    // Existing rows should get NULL for the new column
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name, age FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("age"), Some(&Value::Null));
    assert_eq!(rows[1].get("age"), Some(&Value::Null));
}

#[test]
fn test_add_column_with_default() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Add a column with DEFAULT
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN status INT DEFAULT 0",
    );

    // Existing rows should get the default value
    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, name, status FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("status"), Some(&Value::Integer(0)));
}

#[test]
fn test_add_column_then_insert() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN email VARCHAR",
    );

    // Insert new row with the new column
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, email) VALUES (2, 'bob', 'bob@example.com')",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name, email FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("email"), Some(&Value::Null));
    assert_eq!(
        rows[1].get("email"),
        Some(&Value::Varchar("bob@example.com".into()))
    );
}

#[test]
fn test_add_column_without_column_keyword() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );
    // ADD without COLUMN keyword should also work
    exec(&mut pager, &mut catalog, "ALTER TABLE t ADD name VARCHAR");

    let rows = query_rows(&mut pager, &mut catalog, "DESCRIBE t");
    // Should have id and name columns (plus possibly hidden _rowid)
    let col_names: Vec<&Value> = rows.iter().map(|r| &r.values[0].1).collect();
    assert!(col_names.contains(&&Value::Varchar("name".into())));
}

#[test]
fn test_add_column_duplicate_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN name VARCHAR",
    );
    assert!(err.contains("already exists"), "Error: {}", err);
}

#[test]
fn test_add_column_pk_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN id2 BIGINT PRIMARY KEY",
    );
    assert!(err.contains("PRIMARY KEY"), "Error: {}", err);
}

// ─── DROP COLUMN ───────────────────────────────────────────────

#[test]
fn test_drop_column() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR, age INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, age) VALUES (1, 'alice', 30)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, age) VALUES (2, 'bob', 25)",
    );

    exec(&mut pager, &mut catalog, "ALTER TABLE t DROP COLUMN age");

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alice".into())));
    assert_eq!(rows[1].get("name"), Some(&Value::Varchar("bob".into())));
}

#[test]
fn test_drop_column_pk_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );

    let err = exec_err(&mut pager, &mut catalog, "ALTER TABLE t DROP COLUMN id");
    assert!(err.contains("PRIMARY KEY"), "Error: {}", err);
}

#[test]
fn test_drop_column_nonexistent_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t DROP COLUMN nonexistent",
    );
    assert!(err.contains("not found"), "Error: {}", err);
}

#[test]
fn test_drop_column_with_index_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut pager, &mut catalog, "CREATE INDEX idx_name ON t(name)");

    let err = exec_err(&mut pager, &mut catalog, "ALTER TABLE t DROP COLUMN name");
    assert!(err.contains("index"), "Error: {}", err);
}

// ─── MODIFY COLUMN ─────────────────────────────────────────────

#[test]
fn test_modify_column_type_int_to_bigint() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, val) VALUES (1, 42)",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN val BIGINT",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, val FROM t");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(42)));
}

#[test]
fn test_modify_column_type_int_to_varchar() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, val INT)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, val) VALUES (1, 42)",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN val VARCHAR",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, val FROM t");
    assert_eq!(rows[0].get("val"), Some(&Value::Varchar("42".into())));
}

#[test]
fn test_modify_column_metadata_only() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Change nullable to NOT NULL (metadata-only, same type)
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN name VARCHAR NOT NULL",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, name FROM t");
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alice".into())));
}

// ─── CHANGE COLUMN ─────────────────────────────────────────────

#[test]
fn test_change_column_rename() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t CHANGE COLUMN name username VARCHAR",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, username FROM t");
    assert_eq!(
        rows[0].get("username"),
        Some(&Value::Varchar("alice".into()))
    );
}

#[test]
fn test_change_column_nonexistent_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t CHANGE COLUMN nonexistent new_name VARCHAR",
    );
    assert!(err.contains("not found"), "Error: {}", err);
}

// ─── RENAME TABLE ──────────────────────────────────────────────

#[test]
fn test_rename_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE old_t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO old_t (id, name) VALUES (1, 'alice')",
    );

    exec(&mut pager, &mut catalog, "RENAME TABLE old_t TO new_t");

    // Access by new name
    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, name FROM new_t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alice".into())));
}

#[test]
fn test_rename_table_old_name_gone() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE old_t (id BIGINT PRIMARY KEY)",
    );

    exec(&mut pager, &mut catalog, "RENAME TABLE old_t TO new_t");

    // Old name should not be accessible
    let err = exec_err(&mut pager, &mut catalog, "SELECT * FROM old_t");
    assert!(err.contains("not found"), "Error: {}", err);
}

#[test]
fn test_rename_table_nonexistent_error() {
    let (mut pager, mut catalog, _dir) = setup();

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "RENAME TABLE nonexistent TO new_t",
    );
    assert!(err.contains("does not exist"), "Error: {}", err);
}

#[test]
fn test_rename_table_target_exists_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t1 (id BIGINT PRIMARY KEY)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t2 (id BIGINT PRIMARY KEY)",
    );

    let err = exec_err(&mut pager, &mut catalog, "RENAME TABLE t1 TO t2");
    assert!(err.contains("already exists"), "Error: {}", err);
}

// ─── ALTER TABLE on non-existent table ─────────────────────────

#[test]
fn test_alter_table_nonexistent_error() {
    let (mut pager, mut catalog, _dir) = setup();

    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE nonexistent ADD COLUMN x INT",
    );
    assert!(err.contains("not found"), "Error: {}", err);
}

// ─── SELECT * after ADD COLUMN ─────────────────────────────────

#[test]
fn test_select_star_after_add_column() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN age INT DEFAULT 25",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    // Should include all 3 columns
    assert_eq!(rows[0].values.len(), 3);
    assert_eq!(rows[0].get("age"), Some(&Value::Integer(25)));
}

// ─── UNIQUE constraint via ALTER TABLE ─────────────────────────

#[test]
fn test_add_column_unique_creates_index() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN email VARCHAR UNIQUE",
    );

    // Insert with unique email should work
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, email) VALUES (2, 'bob', 'bob@test.com')",
    );

    // Insert duplicate email should fail
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, email) VALUES (3, 'charlie', 'bob@test.com')",
    );
    assert!(
        err.contains("unique") || err.contains("Duplicate"),
        "Error: {}",
        err
    );
}

#[test]
fn test_modify_column_add_unique() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'bob')",
    );

    // Add UNIQUE constraint via MODIFY
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN name VARCHAR UNIQUE",
    );

    // Duplicate should now fail
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (3, 'alice')",
    );
    assert!(
        err.contains("unique") || err.contains("Duplicate"),
        "Error: {}",
        err
    );
}

#[test]
fn test_modify_column_add_unique_with_duplicates_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'alice')",
    );

    // Adding UNIQUE should fail because duplicates exist
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN name VARCHAR UNIQUE",
    );
    assert!(
        err.contains("Duplicate") || err.contains("unique"),
        "Error: {}",
        err
    );
}

// ─── NOT NULL validation ───────────────────────────────────────

#[test]
fn test_add_column_not_null_without_default_on_nonempty_table() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t (id) VALUES (1)");

    // Should fail: NOT NULL without DEFAULT on table with rows
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN val INT NOT NULL",
    );
    assert!(
        err.contains("NOT NULL") && err.contains("DEFAULT"),
        "Error: {}",
        err
    );
}

#[test]
fn test_add_column_not_null_with_default_ok() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t (id) VALUES (1)");

    // NOT NULL with DEFAULT should succeed
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN val INT NOT NULL DEFAULT 0",
    );

    let rows = query_rows(&mut pager, &mut catalog, "SELECT id, val FROM t");
    assert_eq!(rows[0].get("val"), Some(&Value::Integer(0)));
}

#[test]
fn test_add_column_not_null_on_empty_table_ok() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY)",
    );

    // NOT NULL without DEFAULT on empty table should succeed
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN val INT NOT NULL",
    );
}

#[test]
fn test_modify_column_to_not_null_with_nulls_error() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(&mut pager, &mut catalog, "INSERT INTO t (id) VALUES (1)"); // name is NULL

    // MODIFY to NOT NULL should fail because name has NULL values
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN name VARCHAR NOT NULL",
    );
    assert!(
        err.contains("NULL") && err.contains("NOT NULL"),
        "Error: {}",
        err
    );
}

#[test]
fn test_modify_column_to_not_null_without_nulls_ok() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // All values are non-NULL, so this should succeed
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t MODIFY COLUMN name VARCHAR NOT NULL",
    );
}

/// Downgrade a table to v0 format: rewrite all rows without the u16 column-count
/// prefix and set row_format_version=0 in the catalog. This simulates a table
/// created by an older version of murodb.
fn downgrade_table_to_v0(pager: &mut Pager, catalog: &mut SystemCatalog, table_name: &str) {
    let mut table_def = catalog.get_table(pager, table_name).unwrap().unwrap();
    let data_btree = BTree::open(table_def.data_btree_root);
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    data_btree
        .scan(pager, |k, v| {
            // Strip the u16 prefix (first 2 bytes) to get v0 format
            entries.push((k.to_vec(), v[2..].to_vec()));
            Ok(true)
        })
        .unwrap();

    let mut data_btree = BTree::open(table_def.data_btree_root);
    for (key, v0_data) in entries {
        data_btree.insert(pager, &key, &v0_data).unwrap();
    }

    table_def.row_format_version = 0;
    catalog.update_table(pager, &table_def).unwrap();
}

// --- Regression tests for v0/v1 row format and UNIQUE+DEFAULT ---

#[test]
fn test_legacy_v0_table_insert_then_read() {
    // Simulate: a table created before row_format_version was introduced
    // would have row_format_version=0. Inserting new rows should upgrade
    // the table to v1 and all rows (old and new) should remain readable.
    let (mut pager, mut catalog, _dir) = setup();

    // Create table (gets v1 by default in new code)
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Downgrade to v0 to simulate a legacy table
    downgrade_table_to_v0(&mut pager, &mut catalog, "t");

    // Now insert a new row — this should trigger ensure_row_format_v1
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'bob')",
    );

    // Both rows should be readable
    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alice".into())));
    assert_eq!(rows[1].get("id"), Some(&Value::Integer(2)));
    assert_eq!(rows[1].get("name"), Some(&Value::Varchar("bob".into())));

    // Verify table is now v1
    let table_def = catalog.get_table(&mut pager, "t").unwrap().unwrap();
    assert_eq!(table_def.row_format_version, 1);
}

#[test]
fn test_legacy_v0_table_update_then_read() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Downgrade to v0
    downgrade_table_to_v0(&mut pager, &mut catalog, "t");

    // Update should trigger ensure_row_format_v1
    exec(
        &mut pager,
        &mut catalog,
        "UPDATE t SET name = 'alicia' WHERE id = 1",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alicia".into())));
}

#[test]
fn test_legacy_v0_table_add_column_upgrades() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Downgrade to v0
    downgrade_table_to_v0(&mut pager, &mut catalog, "t");

    // ADD COLUMN should upgrade to v1
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN age INT DEFAULT 0",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name, age FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(rows[0].get("name"), Some(&Value::Varchar("alice".into())));
    assert_eq!(rows[0].get("age"), Some(&Value::Integer(0)));
}

#[test]
fn test_add_column_unique_with_nonnull_default_rejects_nonempty() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'bob')",
    );

    // UNIQUE with non-NULL default on 2+ rows should fail
    let result = execute(
        "ALTER TABLE t ADD COLUMN code VARCHAR UNIQUE DEFAULT 'X'",
        &mut pager,
        &mut catalog,
    );
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("UNIQUE"),
        "Error should mention UNIQUE: {}",
        err_msg
    );
}

#[test]
fn test_add_column_unique_with_null_default_ok() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (2, 'bob')",
    );

    // UNIQUE with NULL default is fine (NULLs don't violate UNIQUE)
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN code VARCHAR UNIQUE",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name, code FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("code"), Some(&Value::Null));
    assert_eq!(rows[1].get("code"), Some(&Value::Null));
}

#[test]
fn test_add_column_unique_with_nonnull_default_single_row_ok() {
    let (mut pager, mut catalog, _dir) = setup();
    exec(
        &mut pager,
        &mut catalog,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)",
    );
    exec(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name) VALUES (1, 'alice')",
    );

    // Only 1 row — no duplicate, so UNIQUE + default is fine
    exec(
        &mut pager,
        &mut catalog,
        "ALTER TABLE t ADD COLUMN code VARCHAR UNIQUE DEFAULT 'X'",
    );

    let rows = query_rows(
        &mut pager,
        &mut catalog,
        "SELECT id, name, code FROM t ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("code"), Some(&Value::Varchar("X".into())));

    // Inserting another row with the same default value should fail (index was backfilled)
    let err = exec_err(
        &mut pager,
        &mut catalog,
        "INSERT INTO t (id, name, code) VALUES (2, 'bob', 'X')",
    );
    assert!(
        err.contains("Duplicate") || err.contains("unique"),
        "Expected unique violation: {}",
        err
    );
}
