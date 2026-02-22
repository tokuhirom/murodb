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

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 18);
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
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }
}

#[test]
fn test_commit_in_doubt_increments_counter() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    assert_eq!(session.database_stats().commit_in_doubt_count, 0);

    // Set up a table first (needs a working pager)
    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();

    // Inject write_page failure so next commit triggers CommitInDoubt
    session
        .pager_mut()
        .set_inject_write_page_failure(Some(std::io::ErrorKind::Other));

    let result = session.execute("INSERT INTO t VALUES (1)");
    assert!(
        matches!(&result, Err(MuroError::CommitInDoubt(_))),
        "expected CommitInDoubt, got: {:?}",
        result
    );

    // Counter must have been incremented
    assert_eq!(session.database_stats().commit_in_doubt_count, 1);
    assert!(session
        .database_stats()
        .last_commit_in_doubt_error
        .is_some());
    assert!(session
        .database_stats()
        .last_commit_in_doubt_timestamp_ms
        .is_some());
}

#[test]
fn test_show_checkpoint_stats_still_works() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    // SHOW CHECKPOINT STATS should still return 4 rows (backward compat)
    match session.execute("SHOW CHECKPOINT STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 4);
        }
        _ => panic!("Expected rows from SHOW CHECKPOINT STATS"),
    }
}

#[test]
fn test_freelist_sanitize_stats_on_clean_open() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let session = Session::new(pager, catalog, wal);

    // Clean pager should have no sanitize stats
    assert_eq!(session.database_stats().freelist_sanitize_count, 0);
    assert_eq!(session.database_stats().freelist_out_of_range_total, 0);
    assert_eq!(session.database_stats().freelist_duplicates_total, 0);
}

#[test]
fn test_stats_readable_on_poisoned_session() {
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

    // Poison the session via CommitInDoubt
    session
        .pager_mut()
        .set_inject_write_page_failure(Some(std::io::ErrorKind::Other));
    let result = session.execute("INSERT INTO t VALUES (1)");
    assert!(matches!(&result, Err(MuroError::CommitInDoubt(_))));

    // Regular queries must be rejected
    let result = session.execute("SELECT * FROM t");
    assert!(matches!(&result, Err(MuroError::SessionPoisoned(_))));

    // SHOW DATABASE STATS must still work on poisoned session
    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 18);
            // commit_in_doubt_count should be 1
            assert_eq!(
                rows[4].get("stat"),
                Some(&Value::Varchar("commit_in_doubt_count".to_string()))
            );
            assert_eq!(rows[4].get("value"), Some(&Value::Varchar("1".to_string())));
            // Pager cache stats should be present
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
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }

    // SHOW CHECKPOINT STATS must also work on poisoned session
    match session.execute("SHOW CHECKPOINT STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 4);
        }
        _ => panic!("Expected rows from SHOW CHECKPOINT STATS"),
    }
}

#[test]
fn test_read_only_query_select_does_not_create_wal_records() {
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
    session
        .execute("INSERT INTO t VALUES (1, 'alice')")
        .unwrap();

    let wal_before = std::fs::metadata(&wal_path).unwrap().len();
    let rows = session
        .execute_read_only_query("SELECT id, name FROM t")
        .unwrap();
    let wal_after = std::fs::metadata(&wal_path).unwrap().len();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(wal_before, wal_after, "read-only query must not append WAL");
}

#[test]
fn test_read_only_query_rejects_writes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    let result = session.execute_read_only_query("INSERT INTO t VALUES (1)");
    assert!(matches!(result, Err(MuroError::Execution(_))));
}

#[test]
fn test_read_only_query_in_explicit_tx_reads_uncommitted_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session
        .execute("INSERT INTO t VALUES (1, 'alice')")
        .unwrap();

    let rows = session.execute_read_only_query("SELECT id FROM t").unwrap();
    assert_eq!(rows.len(), 1);

    session.execute("ROLLBACK").unwrap();
    let rows = session.execute_read_only_query("SELECT id FROM t").unwrap();
    assert!(rows.is_empty());
}
