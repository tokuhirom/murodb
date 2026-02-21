use crate::error::{MuroError, Result};
use crate::schema::catalog::SystemCatalog;
use crate::sql::ast::Statement;
use crate::sql::executor::{execute_statement, ExecResult, Row};
use crate::sql::parser::parse_sql;
use crate::storage::pager::Pager;
use crate::tx::page_store::TxPageStore;
use crate::tx::transaction::Transaction;
use crate::types::Value;
use crate::wal::record::TxId;
use crate::wal::writer::WalWriter;

const CHECKPOINT_MAX_ATTEMPTS: usize = 2;

/// Database operation statistics for observability.
#[derive(Debug, Clone, Default)]
pub struct DatabaseStats {
    // Checkpoint stats
    pub total_checkpoints: u64,
    pub failed_checkpoints: u64,
    pub last_failure_error: Option<String>,
    pub last_failure_timestamp_ms: Option<u64>,
    // CommitInDoubt stats
    pub commit_in_doubt_count: u64,
    pub last_commit_in_doubt_error: Option<String>,
    pub last_commit_in_doubt_timestamp_ms: Option<u64>,
    // Freelist sanitize stats
    pub freelist_sanitize_count: u64,
    pub freelist_out_of_range_total: u64,
    pub freelist_duplicates_total: u64,
}

/// Backward-compatible alias.
pub type CheckpointStats = DatabaseStats;

/// A session that manages explicit transaction state.
///
/// - `BEGIN` starts a transaction (dirty-page buffering).
/// - `COMMIT` flushes dirty pages via WAL, then to disk.
/// - `ROLLBACK` discards dirty pages without WAL append.
/// - Without `BEGIN`, each statement executes in auto-commit mode
///   (wrapped in an implicit transaction with WAL).
pub struct Session {
    pager: Pager,
    catalog: SystemCatalog,
    wal: WalWriter,
    active_tx: Option<Transaction>,
    next_txid: TxId,
    stats: DatabaseStats,
    poisoned: Option<String>,
    #[cfg(test)]
    inject_checkpoint_failures_remaining: usize,
}

impl Session {
    pub fn new(pager: Pager, catalog: SystemCatalog, wal: WalWriter) -> Self {
        let next_txid = pager.next_txid();
        let mut stats = DatabaseStats::default();

        // Absorb freelist sanitize report from pager open
        if let Some(report) = pager.freelist_sanitize_report() {
            stats.freelist_sanitize_count += 1;
            stats.freelist_out_of_range_total += report.out_of_range.len() as u64;
            stats.freelist_duplicates_total += report.duplicates.len() as u64;
            eprintln!(
                "WARNING: freelist_sanitized out_of_range={} duplicates={} total_removed={}",
                report.out_of_range.len(),
                report.duplicates.len(),
                report.total_removed()
            );
        }

        Session {
            pager,
            catalog,
            wal,
            active_tx: None,
            next_txid,
            stats,
            poisoned: None,
            #[cfg(test)]
            inject_checkpoint_failures_remaining: 0,
        }
    }

    fn check_poisoned(&self) -> Result<()> {
        if let Some(ref msg) = self.poisoned {
            return Err(MuroError::SessionPoisoned(msg.clone()));
        }
        Ok(())
    }

    /// Execute a SQL string, handling BEGIN/COMMIT/ROLLBACK at the session level.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let stmt = parse_sql(sql).map_err(MuroError::Parse)?;

        // Stats queries are always allowed, even on poisoned sessions,
        // so operators can inspect counters after CommitInDoubt.
        match &stmt {
            Statement::ShowCheckpointStats => return self.handle_show_checkpoint_stats(),
            Statement::ShowDatabaseStats => return self.handle_show_database_stats(),
            _ => {}
        }

        self.check_poisoned()?;

        match &stmt {
            Statement::Begin => self.handle_begin(),
            Statement::Commit => self.handle_commit(),
            Statement::Rollback => self.handle_rollback(),
            _ => {
                if self.active_tx.is_some() {
                    self.execute_in_tx(&stmt)
                } else {
                    // Auto-commit: wrap in an implicit transaction with WAL
                    self.execute_auto_commit(&stmt)
                }
            }
        }
    }

    fn handle_begin(&mut self) -> Result<ExecResult> {
        if self.active_tx.is_some() {
            return Err(MuroError::Transaction("Transaction already active".into()));
        }
        let txid = self.next_txid;
        self.next_txid += 1;
        let snapshot_lsn = self.wal.current_lsn();
        self.active_tx = Some(Transaction::begin(txid, snapshot_lsn));
        Ok(ExecResult::Ok)
    }

    fn handle_commit(&mut self) -> Result<ExecResult> {
        let mut tx = self
            .active_tx
            .take()
            .ok_or_else(|| MuroError::Transaction("No active transaction".into()))?;
        let catalog_root = self.catalog.root_page_id();
        self.pager.set_next_txid(self.next_txid);
        match tx.commit(&mut self.pager, &mut self.wal, catalog_root) {
            Err(e @ MuroError::CommitInDoubt(_)) => {
                self.record_commit_in_doubt(&e);
                self.poisoned = Some(e.to_string());
                return Err(e);
            }
            Err(e) => return Err(e),
            Ok(_) => {}
        }
        self.post_commit_checkpoint();
        Ok(ExecResult::Ok)
    }

    fn handle_rollback(&mut self) -> Result<ExecResult> {
        let mut tx = self
            .active_tx
            .take()
            .ok_or_else(|| MuroError::Transaction("No active transaction".into()))?;
        tx.rollback_no_wal();
        self.post_rollback_checkpoint();
        // Reload catalog from disk since in-memory catalog may have been modified
        let catalog_root = self.pager.catalog_root();
        self.catalog = SystemCatalog::open(catalog_root);
        Ok(ExecResult::Ok)
    }

    /// Execute a statement in auto-commit mode: wrap in an implicit transaction.
    fn execute_auto_commit(&mut self, stmt: &Statement) -> Result<ExecResult> {
        let txid = self.next_txid;
        self.next_txid += 1;
        let snapshot_lsn = self.wal.current_lsn();
        let tx = Transaction::begin(txid, snapshot_lsn);

        // Save catalog state for rollback on error
        let catalog_root_before = self.catalog.root_page_id();

        let mut store = TxPageStore::new(tx, &mut self.pager);
        let result = execute_statement(stmt, &mut store, &mut self.catalog);
        let mut tx = store.into_tx();

        match result {
            Ok(exec_result) => {
                // Commit via WAL (catalog_root included in WAL MetaUpdate)
                let catalog_root = self.catalog.root_page_id();
                self.pager.set_next_txid(self.next_txid);
                match tx.commit(&mut self.pager, &mut self.wal, catalog_root) {
                    Err(e @ MuroError::CommitInDoubt(_)) => {
                        self.record_commit_in_doubt(&e);
                        self.poisoned = Some(e.to_string());
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                    Ok(_) => {}
                }
                self.post_commit_checkpoint();
                Ok(exec_result)
            }
            Err(e) => {
                // Rollback: discard dirty pages, restore catalog
                tx.rollback_no_wal();
                self.catalog = SystemCatalog::open(catalog_root_before);
                Err(e)
            }
        }
    }

    /// Execute a statement within an active transaction.
    fn execute_in_tx(&mut self, stmt: &Statement) -> Result<ExecResult> {
        // Save catalog state so we can restore on error
        let catalog_root_before = self.catalog.root_page_id();

        // Take the transaction out temporarily
        let tx = self.active_tx.take().unwrap();
        let mut store = TxPageStore::new(tx, &mut self.pager);

        let result = execute_statement(stmt, &mut store, &mut self.catalog);

        // Put the transaction back
        self.active_tx = Some(store.into_tx());

        if result.is_err() {
            // Restore catalog to pre-statement state on error
            self.catalog = SystemCatalog::open(catalog_root_before);
        }

        result
    }

    /// Get a reference to the pager (for flush/metadata operations).
    pub fn pager(&self) -> &Pager {
        &self.pager
    }

    /// Get a mutable reference to the pager.
    pub fn pager_mut(&mut self) -> &mut Pager {
        &mut self.pager
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &SystemCatalog {
        &self.catalog
    }

    fn post_commit_checkpoint(&mut self) {
        self.stats.total_checkpoints += 1;
        // Best-effort: commit already reached durable state in data file.
        // If WAL truncate fails, keep serving and rely on startup recovery path.
        if let Err((attempts, e)) = self.try_checkpoint_truncate_with_retry() {
            self.stats.failed_checkpoints += 1;
            self.stats.last_failure_error = Some(format!("{}", e));
            self.stats.last_failure_timestamp_ms = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            );
            self.emit_checkpoint_warning("post-commit", attempts, &e);
        }
    }

    fn post_rollback_checkpoint(&mut self) {
        self.stats.total_checkpoints += 1;
        // Best-effort: rollback leaves no committed changes to preserve in WAL.
        if let Err((attempts, e)) = self.try_checkpoint_truncate_with_retry() {
            self.stats.failed_checkpoints += 1;
            self.stats.last_failure_error = Some(format!("{}", e));
            self.stats.last_failure_timestamp_ms = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            );
            self.emit_checkpoint_warning("post-rollback", attempts, &e);
        }
    }

    fn handle_show_checkpoint_stats(&self) -> Result<ExecResult> {
        let stats = &self.stats;
        let rows = vec![
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("total_checkpoints".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.total_checkpoints.to_string()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("failed_checkpoints".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.failed_checkpoints.to_string()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("last_failure_error".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(stats.last_failure_error.clone().unwrap_or_default()),
                    ),
                ],
            },
            Row {
                values: vec![
                    (
                        "stat".to_string(),
                        Value::Varchar("last_failure_timestamp_ms".to_string()),
                    ),
                    (
                        "value".to_string(),
                        Value::Varchar(
                            stats
                                .last_failure_timestamp_ms
                                .map(|v| v.to_string())
                                .unwrap_or_default(),
                        ),
                    ),
                ],
            },
        ];
        Ok(ExecResult::Rows(rows))
    }

    pub fn checkpoint_stats(&self) -> &CheckpointStats {
        &self.stats
    }

    pub fn database_stats(&self) -> &DatabaseStats {
        &self.stats
    }

    fn record_commit_in_doubt(&mut self, error: &MuroError) {
        self.stats.commit_in_doubt_count += 1;
        self.stats.last_commit_in_doubt_error = Some(error.to_string());
        self.stats.last_commit_in_doubt_timestamp_ms = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );
        eprintln!("WARNING: commit_in_doubt error=\"{}\"", error);
    }

    fn handle_show_database_stats(&self) -> Result<ExecResult> {
        let stats = &self.stats;
        fn stat_row(name: &str, value: String) -> Row {
            Row {
                values: vec![
                    ("stat".to_string(), Value::Varchar(name.to_string())),
                    ("value".to_string(), Value::Varchar(value)),
                ],
            }
        }
        let rows = vec![
            stat_row("total_checkpoints", stats.total_checkpoints.to_string()),
            stat_row("failed_checkpoints", stats.failed_checkpoints.to_string()),
            stat_row(
                "last_failure_error",
                stats.last_failure_error.clone().unwrap_or_default(),
            ),
            stat_row(
                "last_failure_timestamp_ms",
                stats
                    .last_failure_timestamp_ms
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ),
            stat_row(
                "commit_in_doubt_count",
                stats.commit_in_doubt_count.to_string(),
            ),
            stat_row(
                "last_commit_in_doubt_error",
                stats.last_commit_in_doubt_error.clone().unwrap_or_default(),
            ),
            stat_row(
                "last_commit_in_doubt_timestamp_ms",
                stats
                    .last_commit_in_doubt_timestamp_ms
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            ),
            stat_row(
                "freelist_sanitize_count",
                stats.freelist_sanitize_count.to_string(),
            ),
            stat_row(
                "freelist_out_of_range_total",
                stats.freelist_out_of_range_total.to_string(),
            ),
            stat_row(
                "freelist_duplicates_total",
                stats.freelist_duplicates_total.to_string(),
            ),
        ];
        Ok(ExecResult::Rows(rows))
    }

    fn emit_checkpoint_warning(&self, phase: &str, attempts: usize, error: &MuroError) {
        let wal_path = self.wal.wal_path().display();
        let wal_size = self
            .wal
            .file_size_bytes()
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        eprintln!(
            "WARNING: checkpoint_failed phase={} attempts={} error=\"{}\" wal_path={} wal_size_bytes={}",
            phase, attempts, error, wal_path, wal_size
        );
    }

    fn try_checkpoint_truncate_once(&mut self) -> Result<()> {
        #[cfg(test)]
        if self.inject_checkpoint_failures_remaining > 0 {
            self.inject_checkpoint_failures_remaining -= 1;
            return Err(MuroError::Io(std::io::Error::other(
                "injected checkpoint failure",
            )));
        }
        self.wal.checkpoint_truncate()
    }

    fn try_checkpoint_truncate_with_retry(
        &mut self,
    ) -> std::result::Result<usize, (usize, MuroError)> {
        let mut last_err = None;
        for attempt in 1..=CHECKPOINT_MAX_ATTEMPTS {
            if attempt > 1 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            match self.try_checkpoint_truncate_once() {
                Ok(()) => return Ok(attempt),
                Err(e) => last_err = Some(e),
            }
        }
        Err((
            CHECKPOINT_MAX_ATTEMPTS,
            last_err.unwrap_or_else(|| {
                MuroError::Io(std::io::Error::other(
                    "checkpoint truncate failed without error detail",
                ))
            }),
        ))
    }

    #[cfg(test)]
    fn inject_checkpoint_failure_once_for_test(&mut self) {
        self.inject_checkpoint_failures_remaining = 1;
    }

    #[cfg(test)]
    fn inject_checkpoint_failures_for_test(&mut self, count: usize) {
        self.inject_checkpoint_failures_remaining = count;
    }
}

#[cfg(test)]
mod tests {
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
                assert_eq!(rows.len(), 10);
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
                assert_eq!(rows.len(), 10);
                // commit_in_doubt_count should be 1
                assert_eq!(
                    rows[4].get("stat"),
                    Some(&Value::Varchar("commit_in_doubt_count".to_string()))
                );
                assert_eq!(rows[4].get("value"), Some(&Value::Varchar("1".to_string())));
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
}
