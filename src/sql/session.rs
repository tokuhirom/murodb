use crate::error::{MuroError, Result};
use crate::schema::catalog::SystemCatalog;
use crate::sql::ast::Statement;
use crate::sql::executor::{execute_statement, ExecResult};
use crate::sql::parser::parse_sql;
use crate::storage::pager::Pager;
use crate::tx::page_store::TxPageStore;
use crate::tx::transaction::Transaction;
use crate::wal::record::TxId;
use crate::wal::writer::WalWriter;

const CHECKPOINT_MAX_ATTEMPTS: usize = 2;

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
    #[cfg(test)]
    inject_checkpoint_failures_remaining: usize,
}

impl Session {
    pub fn new(pager: Pager, catalog: SystemCatalog, wal: WalWriter) -> Self {
        Session {
            pager,
            catalog,
            wal,
            active_tx: None,
            next_txid: 1,
            #[cfg(test)]
            inject_checkpoint_failures_remaining: 0,
        }
    }

    /// Execute a SQL string, handling BEGIN/COMMIT/ROLLBACK at the session level.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let stmt = parse_sql(sql).map_err(MuroError::Parse)?;

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
        tx.commit(&mut self.pager, &mut self.wal, catalog_root)?;
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
                tx.commit(&mut self.pager, &mut self.wal, catalog_root)?;
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
        // Take the transaction out temporarily
        let tx = self.active_tx.take().unwrap();
        let mut store = TxPageStore::new(tx, &mut self.pager);

        let result = execute_statement(stmt, &mut store, &mut self.catalog);

        // Put the transaction back
        self.active_tx = Some(store.into_tx());

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
        // Best-effort: commit already reached durable state in data file.
        // If WAL truncate fails, keep serving and rely on startup recovery path.
        if let Err((attempts, e)) = self.try_checkpoint_truncate_with_retry() {
            self.emit_checkpoint_warning("post-commit", attempts, &e);
        }
    }

    fn post_rollback_checkpoint(&mut self) {
        // Best-effort: rollback leaves no committed changes to preserve in WAL.
        if let Err((attempts, e)) = self.try_checkpoint_truncate_with_retry() {
            self.emit_checkpoint_warning("post-rollback", attempts, &e);
        }
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
            wal_size, 0,
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
}
