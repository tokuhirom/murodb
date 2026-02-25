use super::*;

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
            assert_eq!(rows.len(), 19);
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
fn test_rekey_wal_recreate_failure_poison_session() {
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
    session.execute("INSERT INTO t VALUES (1)").unwrap();

    session.inject_wal_recreate_failure_once_for_test();
    let result = session.rekey_with_password("next_pass");
    assert!(matches!(&result, Err(MuroError::SessionPoisoned(_))));

    // Any regular statement is rejected after the WAL recreation failure.
    let result = session.execute("SELECT * FROM t");
    assert!(matches!(&result, Err(MuroError::SessionPoisoned(_))));

    // Stats SQL remains available for operators.
    match session.execute("SHOW DATABASE STATS").unwrap() {
        ExecResult::Rows(rows) => {
            assert_eq!(rows.len(), 19);
        }
        _ => panic!("Expected rows from SHOW DATABASE STATS"),
    }
}

#[test]
fn test_cancel_handle_returns_false_when_no_statement_is_running() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let session = Session::new(pager, catalog, wal);

    let handle = session.cancel_handle();
    assert!(!handle.cancel());
}

#[test]
fn test_cancel_handle_marks_in_flight_statement() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let wal_path = dir.path().join("test.wal");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&wal_path, &test_key()).unwrap();
    let session = Session::new(pager, catalog, wal);

    let handle = session.cancel_handle();
    let statement_guard = session.enter_statement();
    assert!(handle.cancel());
    assert!(matches!(
        session.cancellation_point(),
        Err(MuroError::Cancelled)
    ));
    drop(statement_guard);
    assert!(session.cancellation_point().is_ok());
}

#[test]
fn test_inflight_cancel_interrupts_long_running_query() {
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
    for i in 0..1000 {
        session
            .execute(&format!("INSERT INTO t VALUES ({})", i))
            .unwrap();
    }

    let handle = session.cancel_handle();
    let canceller = std::thread::spawn(move || {
        for _ in 0..200_000 {
            if handle.cancel() {
                return true;
            }
            std::thread::yield_now();
        }
        false
    });

    let result = session.execute_read_only_query("SELECT a.id FROM t a CROSS JOIN t b");
    let cancel_observed = canceller.join().unwrap();

    assert!(
        cancel_observed,
        "canceller did not observe in-flight statement"
    );
    assert!(matches!(result, Err(MuroError::Cancelled)));
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

#[test]
fn test_savepoint_nested_rollback_and_release_flow() {
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
    session.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    session.execute("SAVEPOINT a").unwrap();
    session.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
    session.execute("SAVEPOINT b").unwrap();
    session.execute("INSERT INTO t VALUES (3, 'c')").unwrap();

    session.execute("ROLLBACK TO SAVEPOINT b").unwrap();
    let rows = match session.execute("SELECT id FROM t ORDER BY id").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 2);

    session.execute("ROLLBACK TO a").unwrap();
    let rows = match session.execute("SELECT id FROM t ORDER BY id").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 1);

    session.execute("RELEASE SAVEPOINT a").unwrap();
    session.execute("COMMIT").unwrap();

    let rows = match session.execute("SELECT id FROM t ORDER BY id").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_savepoint_duplicate_name_overwrites_previous() {
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
    session.execute("BEGIN").unwrap();
    session.execute("INSERT INTO t VALUES (1)").unwrap();
    session.execute("SAVEPOINT s").unwrap();
    session.execute("INSERT INTO t VALUES (2)").unwrap();
    session.execute("SAVEPOINT s").unwrap(); // overwrite
    session.execute("INSERT INTO t VALUES (3)").unwrap();
    session.execute("ROLLBACK TO SAVEPOINT s").unwrap();
    session.execute("COMMIT").unwrap();

    let rows = match session.execute("SELECT id FROM t ORDER BY id").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_savepoint_invalid_usage_errors() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    let err = session.execute("SAVEPOINT s").unwrap_err();
    assert!(matches!(err, MuroError::Transaction(msg) if msg == "No active transaction"));

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    session.execute("BEGIN").unwrap();

    let err = session.execute("ROLLBACK TO SAVEPOINT s").unwrap_err();
    assert!(matches!(err, MuroError::Transaction(msg) if msg == "Unknown savepoint: s"));

    let err = session.execute("RELEASE SAVEPOINT s").unwrap_err();
    assert!(matches!(err, MuroError::Transaction(msg) if msg == "Unknown savepoint: s"));
}

#[test]
fn test_rollback_to_savepoint_restores_pager_allocation_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    let mut pager = Pager::create(&db_path, &test_key()).unwrap();
    let catalog = SystemCatalog::create(&mut pager).unwrap();
    pager.set_catalog_root(catalog.root_page_id());
    pager.flush_meta().unwrap();
    let wal = WalWriter::create(&dir.path().join("test.wal"), &test_key()).unwrap();
    let mut session = Session::new(pager, catalog, wal);

    session
        .execute("CREATE TABLE t (id BIGINT PRIMARY KEY, payload VARCHAR)")
        .unwrap();
    session.execute("BEGIN").unwrap();
    session.execute("SAVEPOINT s").unwrap();
    let page_count_before = session.pager().page_count();

    for i in 0..200 {
        session
            .execute(&format!("INSERT INTO t VALUES ({}, REPEAT('a', 2048))", i))
            .unwrap();
    }
    assert!(
        session.pager().page_count() > page_count_before,
        "test setup should allocate pages after savepoint"
    );

    session.execute("ROLLBACK TO SAVEPOINT s").unwrap();
    assert_eq!(session.pager().page_count(), page_count_before);

    let rows = match session.execute("SELECT id FROM t").unwrap() {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert!(rows.is_empty());
}

#[test]
fn test_cancelled_update_in_explicit_tx_does_not_apply_partial_changes() {
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
        .execute("CREATE UNIQUE INDEX idx_name ON t(name)")
        .unwrap();
    session.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    session.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    session.execute("BEGIN").unwrap();
    let handle = session.cancel_handle();
    let statement_guard = session.enter_statement();
    assert!(handle.cancel());
    let stmt = parse_sql("UPDATE t SET name = 'x'").unwrap();
    let err = session
        .execute_in_tx(&stmt)
        .expect_err("update should be cancelled");
    drop(statement_guard);
    assert!(matches!(err, MuroError::Cancelled));

    let rows = match session
        .execute("SELECT id, name FROM t WHERE name = 'x'")
        .unwrap()
    {
        ExecResult::Rows(rows) => rows,
        _ => panic!("Expected rows"),
    };
    assert_eq!(
        rows.len(),
        0,
        "cancelled statement must not leave partial updates"
    );

    session.execute("ROLLBACK").unwrap();
}
