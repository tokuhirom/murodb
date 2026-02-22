use murodb::crypto::aead::MasterKey;
use murodb::schema::catalog::SystemCatalog;
use murodb::sql::executor::execute;
use murodb::storage::pager::Pager;
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

#[test]
fn test_analyze_table_persists_basic_stats() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b INT)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_a ON t(a)", &mut pager, &mut catalog).unwrap();

    execute(
        "INSERT INTO t (id, a, b) VALUES (1, 10, 1)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, a, b) VALUES (2, 10, 2)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, a, b) VALUES (3, 20, 3)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, a, b) VALUES (4, 30, 4)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    let table_before = catalog.get_table(&mut pager, "t").unwrap().unwrap();
    assert_eq!(table_before.stats_row_count, 0);

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();

    let table_after = catalog.get_table(&mut pager, "t").unwrap().unwrap();
    assert_eq!(table_after.stats_row_count, 4);

    let idx = catalog.get_index(&mut pager, "idx_a").unwrap().unwrap();
    assert_eq!(idx.stats_distinct_keys, 3);
}

#[test]
fn test_analyze_table_distinct_keys_non_unique_varchar_not_order_dependent() {
    let (mut pager, mut catalog, _dir) = setup();

    execute(
        "CREATE TABLE t (id VARCHAR PRIMARY KEY, s VARCHAR)",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute("CREATE INDEX idx_s ON t(s)", &mut pager, &mut catalog).unwrap();

    // For non-unique index key encoding (s || pk), the same logical s='a'
    // can appear in separate scan runs when pk bytes sort around other terms.
    execute(
        "INSERT INTO t (id, s) VALUES ('!', 'a')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, s) VALUES ('m', 'ab')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();
    execute(
        "INSERT INTO t (id, s) VALUES ('~', 'a')",
        &mut pager,
        &mut catalog,
    )
    .unwrap();

    execute("ANALYZE TABLE t", &mut pager, &mut catalog).unwrap();

    let idx = catalog.get_index(&mut pager, "idx_s").unwrap().unwrap();
    assert_eq!(
        idx.stats_distinct_keys, 2,
        "distinct key count should be based on logical index key, not contiguous runs"
    );
}
