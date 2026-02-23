use super::*;
use crate::crypto::aead::MasterKey;
use tempfile::TempDir;

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

#[test]
fn test_commit_survives_checkpoint_failure_and_leaves_wal_for_recovery() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.set_checkpoint_policy_for_test(1, 0, 0);
    session.inject_checkpoint_failures_for_test(CHECKPOINT_MAX_ATTEMPTS);
    let result = session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    assert!(matches!(result, ExecResult::Ok));

    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert!(
        wal_size > 0,
        "WAL should remain when checkpoint is injected to fail"
    );

    let mut db = crate::Database::open(&db_path, &test_key()).unwrap();
    let rows = match db.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_rollback_survives_checkpoint_failure_and_discards_uncommitted_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.set_checkpoint_policy_for_test(1, 0, 0);
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'alice')")
        .unwrap();

    session.inject_checkpoint_failures_for_test(CHECKPOINT_MAX_ATTEMPTS);
    let result = session.execute("ROLLBACK").unwrap();
    assert!(matches!(result, ExecResult::Ok));

    let rows = match session.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_commit_checkpoint_retries_transient_failure_and_clears_wal() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.set_checkpoint_policy_for_test(1, 0, 0);
    session.inject_checkpoint_failure_once_for_test();
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    let wal_size = std::fs::metadata(&wal_path).unwrap().len();
    assert_eq!(
        wal_size,
        crate::wal::WAL_HEADER_SIZE as u64,
        "transient checkpoint failure should be recovered by retry"
    );
}

#[test]
fn test_retry_attempt_count_is_reported_on_transient_success() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.inject_checkpoint_failure_once_for_test();
    let attempts = session.try_checkpoint_truncate_with_retry().unwrap();
    assert_eq!(attempts, 2);
}

#[test]
fn test_auto_commit_survives_all_checkpoint_failures() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    // Inject failures that exhaust all retry attempts
    session.inject_checkpoint_failures_for_test(CHECKPOINT_MAX_ATTEMPTS);

    // Auto-commit should still succeed even though checkpoint fails
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Subsequent auto-commit should also succeed
    session.inject_checkpoint_failures_for_test(CHECKPOINT_MAX_ATTEMPTS);
    session
        .execute("INSERT INTO t VALUES (1, 'alice')")
        .unwrap();

    // Data should be queryable
    match session.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => assert_eq!(rows.len(), 1),
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_multiple_commits_each_retry_independently() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Each auto-commit gets its own independent retry cycle
    for i in 0..5 {
        session.inject_checkpoint_failure_once_for_test();
        session
            .execute(&format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i))
            .unwrap();
    }

    match session.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => assert_eq!(rows.len(), 5),
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_checkpoint_backoff_does_not_block_commit() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    // Even with backoff sleep, the commit completes successfully
    session.inject_checkpoint_failure_once_for_test();
    let start = std::time::Instant::now();
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    let elapsed = start.elapsed();

    // Backoff is 1ms, so the total should be well under 1 second
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "Backoff should not significantly delay commit: {:?}",
        elapsed
    );

    match session.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows(rows) => assert_eq!(rows.len(), 1),
        _ => panic!("Expected rows"),
    }
}

#[test]
fn test_checkpoint_stats_tracked() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    // Initial stats should be zero
    assert_eq!(session.checkpoint_stats().total_checkpoints, 0);
    assert_eq!(session.checkpoint_stats().failed_checkpoints, 0);
    assert_eq!(session.database_stats().deferred_checkpoints, 0);

    // Successful checkpoint
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    assert_eq!(session.checkpoint_stats().total_checkpoints, 1);
    assert_eq!(session.checkpoint_stats().failed_checkpoints, 0);

    // Failed checkpoint (all retries exhausted)
    session.inject_checkpoint_failures_for_test(CHECKPOINT_MAX_ATTEMPTS);
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(session.checkpoint_stats().total_checkpoints, 2);
    assert_eq!(session.checkpoint_stats().failed_checkpoints, 1);
    assert!(session.checkpoint_stats().last_failure_error.is_some());
    assert!(session
        .checkpoint_stats()
        .last_failure_timestamp_ms
        .is_some());
    assert_eq!(session.database_stats().deferred_checkpoints, 0);
}

#[test]
fn test_checkpoint_can_be_deferred_by_tx_threshold() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(3, 0, 0);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    assert_eq!(session.database_stats().total_checkpoints, 0);
    assert_eq!(session.database_stats().deferred_checkpoints, 1);
    assert!(std::fs::metadata(&wal_path).unwrap().len() > crate::wal::WAL_HEADER_SIZE as u64);

    session.execute("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(session.database_stats().total_checkpoints, 0);
    assert_eq!(session.database_stats().deferred_checkpoints, 2);

    session.execute("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(session.database_stats().total_checkpoints, 1);
    assert_eq!(session.database_stats().deferred_checkpoints, 2);
    assert_eq!(session.database_stats().failed_checkpoints, 0);
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        crate::wal::WAL_HEADER_SIZE as u64
    );
}

#[test]
fn test_checkpoint_triggered_by_wal_size_threshold_before_tx_threshold() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();

    // Keep tx threshold high, but set WAL threshold low enough that large inserts trigger checkpoint.
    session.set_checkpoint_policy_for_test(1000, 600, 0);
    let checkpoints_before = session.database_stats().total_checkpoints;
    session
        .execute("INSERT INTO t VALUES (1, REPEAT('a', 2048))")
        .unwrap();

    assert_eq!(
        session.database_stats().total_checkpoints,
        checkpoints_before + 1
    );
    assert_eq!(session.database_stats().failed_checkpoints, 0);
    assert_eq!(
        std::fs::metadata(&wal_path).unwrap().len(),
        crate::wal::WAL_HEADER_SIZE as u64
    );
}

#[test]
fn test_show_checkpoint_stats_sql() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    // Execute a commit to get some stats
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    // Query stats via SQL
    match session.execute("SHOW CHECKPOINT STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 4);
            // First row: total_checkpoints
            assert_eq!(
                rows[0].get("stat"),
                Some(&crate::types::Value::Varchar(
                    "total_checkpoints".to_string()
                ))
            );
            assert_eq!(
                rows[0].get("value"),
                Some(&crate::types::Value::Varchar("1".to_string()))
            );
            // Second row: failed_checkpoints
            assert_eq!(
                rows[1].get("value"),
                Some(&crate::types::Value::Varchar("0".to_string()))
            );
        }
        _ => panic!("Expected rows from SHOW CHECKPOINT STATS"),
    }
}

#[test]
fn test_show_database_stats_sql() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    session.set_checkpoint_policy_for_test(1, 0, 0);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 19);
            // Verify checkpoint stats
            assert_eq!(
                rows[0].get("stat"),
                Some(&Value::Varchar("total_checkpoints".to_string()))
            );
            assert_eq!(rows[0].get("value"), Some(&Value::Varchar("1".to_string())));
            // Verify commit_in_doubt_count present
            assert_eq!(
                rows[4].get("stat"),
                Some(&Value::Varchar("commit_in_doubt_count".to_string()))
            );
            assert_eq!(rows[4].get("value"), Some(&Value::Varchar("0".to_string())));
            // Verify freelist_sanitize_count present
            assert_eq!(
                rows[7].get("stat"),
                Some(&Value::Varchar("freelist_sanitize_count".to_string()))
            );
            assert_eq!(rows[7].get("value"), Some(&Value::Varchar("0".to_string())));
            assert_eq!(
                rows[10].get("stat"),
                Some(&Value::Varchar("deferred_checkpoints".to_string()))
            );
            assert_eq!(
                rows[11].get("stat"),
                Some(&Value::Varchar("checkpoint_pending_ops".to_string()))
            );
            assert_eq!(
                rows[12].get("stat"),
                Some(&Value::Varchar(
                    "checkpoint_policy_tx_threshold".to_string()
                ))
            );
            assert_eq!(
                rows[13].get("stat"),
                Some(&Value::Varchar(
                    "checkpoint_policy_wal_bytes_threshold".to_string()
                ))
            );
            assert_eq!(
                rows[14].get("stat"),
                Some(&Value::Varchar("checkpoint_policy_interval_ms".to_string()))
            );
            assert_eq!(
                rows[15].get("stat"),
                Some(&Value::Varchar("pager_cache_hits".to_string()))
            );
            assert_eq!(
                rows[16].get("stat"),
                Some(&Value::Varchar("pager_cache_misses".to_string()))
            );
            assert_eq!(
                rows[17].get("stat"),
                Some(&Value::Varchar("pager_cache_hit_rate_pct".to_string()))
            );
            assert_eq!(
                rows[18].get("stat"),
                Some(&Value::Varchar("wal_file_size_bytes".to_string()))
            );
            let wal_size = match rows[18].get("value") {
                Some(Value::Varchar(v)) => v.parse::<u64>().unwrap(),
                other => panic!("unexpected wal_file_size_bytes value: {:?}", other),
            };
            assert!(wal_size > 0, "expected wal_file_size_bytes > 0");
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }
}

#[cfg(unix)]
#[test]
fn test_show_database_stats_uses_open_wal_handle_when_path_unlinked() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    std::fs::remove_file(&wal_path).unwrap();

    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            let wal_row = rows
                .iter()
                .find(|row| {
                    row.get("stat") == Some(&Value::Varchar("wal_file_size_bytes".to_string()))
                })
                .unwrap();
            let wal_size = match wal_row.get("value") {
                Some(Value::Varchar(v)) => v.parse::<u64>().unwrap(),
                other => panic!("unexpected wal_file_size_bytes value: {:?}", other),
            };
            assert!(wal_size > 0, "expected wal_file_size_bytes > 0");
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }
}

#[test]
fn test_set_runtime_option_sql_updates_checkpoint_policy_stats() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.execute("SET checkpoint_tx_threshold = 9").unwrap();
    session
        .execute("SET checkpoint_wal_bytes_threshold = 1024")
        .unwrap();
    session.execute("SET checkpoint_interval_ms = 250").unwrap();

    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            let get_stat = |name: &str| -> String {
                let row = rows
                    .iter()
                    .find(|row| row.get("stat") == Some(&Value::Varchar(name.to_string())))
                    .unwrap();
                match row.get("value") {
                    Some(Value::Varchar(v)) => v.clone(),
                    other => panic!("unexpected stat value for {}: {:?}", name, other),
                }
            };
            assert_eq!(get_stat("checkpoint_policy_tx_threshold"), "9");
            assert_eq!(get_stat("checkpoint_policy_wal_bytes_threshold"), "1024");
            assert_eq!(get_stat("checkpoint_policy_interval_ms"), "250");
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }
}

#[test]
fn test_set_runtime_option_rejected_inside_transaction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session.execute("BEGIN").unwrap();
    let err = session
        .execute("SET checkpoint_tx_threshold = 2")
        .unwrap_err();
    assert!(matches!(err, MuroError::Execution(_)));
    assert!(format!("{}", err).contains("inside a transaction"));
}

#[test]
fn test_default_policy_defers_checkpoints() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);
    // Use the compiled-in defaults (no set_checkpoint_policy_for_test call)

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    for i in 0..5 {
        session
            .execute(&format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
    }

    let stats = session.database_stats();
    // With DEFAULT_CHECKPOINT_TX_THRESHOLD=8, 6 ops (1 CREATE + 5 INSERT)
    // should not yet trigger a tx-count checkpoint.
    assert_eq!(
        stats.total_checkpoints, 0,
        "default policy should defer checkpoints for fewer than 8 ops"
    );
    assert!(
        stats.deferred_checkpoints >= 6,
        "expected at least 6 deferred checkpoints, got {}",
        stats.deferred_checkpoints
    );
}

mod tail;
