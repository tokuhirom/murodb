use crate::crypto::kdf;
use crate::crypto::suite::EncryptionSuite;
use crate::error::{MuroError, Result};
use crate::schema::catalog::SystemCatalog;
use crate::sql::ast::Statement;
use crate::sql::executor::{execute_statement, ExecResult, Row};
use crate::sql::parser::parse_sql;
use crate::sql::prepared::{contains_bind_params, PreparedStatement};
use crate::storage::pager::Pager;
use crate::tx::page_store::TxPageStore;
use crate::tx::transaction::Transaction;
use crate::types::Value;
use crate::wal::record::TxId;
use crate::wal::writer::WalWriter;
use checkpoint::CheckpointPolicy;

const CHECKPOINT_MAX_ATTEMPTS: usize = 2;
const DEFAULT_CHECKPOINT_TX_THRESHOLD: u64 = 1;
const DEFAULT_CHECKPOINT_WAL_BYTES_THRESHOLD: u64 = 0;
const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 0;
mod checkpoint;

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
    pub deferred_checkpoints: u64,
}

/// Backward-compatible alias.
pub type CheckpointStats = DatabaseStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub checkpoint_tx_threshold: u64,
    pub checkpoint_wal_bytes_threshold: u64,
    pub checkpoint_interval_ms: u64,
}

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
    checkpoint_policy: CheckpointPolicy,
    pending_checkpoint_ops: u64,
    last_checkpoint_at: std::time::Instant,
    #[cfg(test)]
    inject_checkpoint_failures_remaining: usize,
    #[cfg(test)]
    inject_wal_recreate_fail_once: bool,
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
            checkpoint_policy: CheckpointPolicy::from_env(),
            pending_checkpoint_ops: 0,
            last_checkpoint_at: std::time::Instant::now(),
            #[cfg(test)]
            inject_checkpoint_failures_remaining: 0,
            #[cfg(test)]
            inject_wal_recreate_fail_once: false,
        }
    }

    fn check_poisoned(&self) -> Result<()> {
        if let Some(ref msg) = self.poisoned {
            return Err(MuroError::SessionPoisoned(msg.clone()));
        }
        Ok(())
    }

    /// Parse SQL into a reusable prepared statement template.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        PreparedStatement::parse(sql)
    }

    /// Execute a SQL string, handling BEGIN/COMMIT/ROLLBACK at the session level.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
        if contains_bind_params(&stmt) {
            return Err(MuroError::Execution(
                "SQL contains bind parameters ('?'); use prepare()/execute_prepared()".into(),
            ));
        }
        self.execute_statement_with_session(&stmt)
    }

    /// Execute a prepared statement with bound values.
    pub fn execute_prepared(
        &mut self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<ExecResult> {
        let stmt = prepared.bind(params)?;
        self.execute_statement_with_session(&stmt)
    }

    fn execute_statement_with_session(&mut self, stmt: &Statement) -> Result<ExecResult> {
        // Stats queries are always allowed, even on poisoned sessions,
        // so operators can inspect counters after CommitInDoubt.
        match stmt {
            Statement::ShowCheckpointStats => return self.handle_show_checkpoint_stats(),
            Statement::ShowDatabaseStats => return self.handle_show_database_stats(),
            _ => {}
        }

        self.check_poisoned()?;
        self.refresh_from_disk_if_needed()?;

        match stmt {
            Statement::Begin => self.handle_begin(),
            Statement::Commit => self.handle_commit(),
            Statement::Rollback => self.handle_rollback(),
            Statement::SetRuntimeOption(set_stmt) => self.handle_set_runtime_option(set_stmt),
            _ => {
                if self.active_tx.is_some() {
                    self.execute_in_tx(stmt)
                } else {
                    // Auto-commit: wrap in an implicit transaction with WAL
                    self.execute_auto_commit(stmt)
                }
            }
        }
    }

    /// Execute a read-only SQL query and return rows.
    ///
    /// This path avoids auto-commit WAL writes for non-transactional reads.
    pub fn execute_read_only_query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let stmt = parse_sql(sql).map_err(MuroError::Parse)?;
        if contains_bind_params(&stmt) {
            return Err(MuroError::Execution(
                "SQL contains bind parameters ('?'); use prepare()/query_prepared()".into(),
            ));
        }
        self.execute_read_only_query_statement(&stmt)
    }

    /// Execute a prepared statement and return rows (read-only statements only).
    pub fn execute_read_only_prepared_query(
        &mut self,
        prepared: &PreparedStatement,
        params: &[Value],
    ) -> Result<Vec<Row>> {
        let stmt = prepared.bind(params)?;
        self.execute_read_only_query_statement(&stmt)
    }

    fn execute_read_only_query_statement(&mut self, stmt: &Statement) -> Result<Vec<Row>> {
        // Stats queries are always allowed, even on poisoned sessions.
        match stmt {
            Statement::ShowCheckpointStats => {
                return Self::rows_from_exec_result(self.handle_show_checkpoint_stats())
            }
            Statement::ShowDatabaseStats => {
                return Self::rows_from_exec_result(self.handle_show_database_stats())
            }
            _ => {}
        }

        self.check_poisoned()?;
        self.refresh_from_disk_if_needed()?;

        if !Self::is_read_only_statement(stmt) {
            return Err(MuroError::Execution(
                "Database::query accepts read-only SQL only; use execute() for writes".into(),
            ));
        }

        if self.active_tx.is_some() {
            Self::rows_from_exec_result(self.execute_in_tx(stmt))
        } else {
            // Read directly from pager/catalog without opening an implicit WAL transaction.
            Self::rows_from_exec_result(execute_statement(stmt, &mut self.pager, &mut self.catalog))
        }
    }

    fn rows_from_exec_result(result: Result<ExecResult>) -> Result<Vec<Row>> {
        match result? {
            ExecResult::Rows(rows) => Ok(rows),
            ExecResult::RowsAffected(_) | ExecResult::Ok => Err(MuroError::Execution(
                "Read-only query must return rows".into(),
            )),
        }
    }

    fn is_read_only_statement(stmt: &Statement) -> bool {
        match stmt {
            Statement::Select(_)
            | Statement::SetQuery(_)
            | Statement::ShowTables
            | Statement::ShowCreateTable(_)
            | Statement::Describe(_)
            | Statement::ShowCheckpointStats
            | Statement::ShowDatabaseStats => true,
            Statement::Explain(inner) => Self::is_read_only_statement(inner),
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::CreateFulltextIndex(_)
            | Statement::DropTable(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::RenameTable(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::AnalyzeTable(_)
            | Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::SetRuntimeOption(_) => false,
        }
    }

    fn refresh_from_disk_if_needed(&mut self) -> Result<()> {
        if self.active_tx.is_some() {
            return Ok(());
        }
        if self.pager.refresh_from_disk_if_changed()? {
            self.catalog = SystemCatalog::open(self.pager.catalog_root());
            self.next_txid = self.next_txid.max(self.pager.next_txid());
        }
        Ok(())
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

    /// Re-encrypt all pages with a new password-derived key.
    ///
    /// Must not be called inside an active transaction.
    pub fn rekey_with_password(&mut self, new_password: &str) -> Result<()> {
        self.check_poisoned()?;
        self.refresh_from_disk_if_needed()?;

        // Reject if inside an active transaction
        if self.active_tx.is_some() {
            return Err(MuroError::Execution(
                "REKEY cannot be used inside a transaction".into(),
            ));
        }

        // Reject if plaintext mode
        if self.pager.encryption_suite() == EncryptionSuite::Plaintext {
            return Err(MuroError::Execution(
                "REKEY is not supported for plaintext databases".into(),
            ));
        }

        // Checkpoint WAL to ensure all committed data is flushed to the DB file
        self.try_checkpoint_truncate_with_retry()
            .map_err(|(_, e)| {
                MuroError::Execution(format!("REKEY failed: WAL checkpoint failed: {}", e))
            })?;

        // Generate new salt and derive new key
        let new_salt = kdf::generate_salt();
        let new_key = kdf::derive_key(new_password.as_bytes(), &new_salt)?;

        // Re-encrypt all pages
        self.pager.rekey(&new_key, new_salt)?;

        // Recreate WAL writer with new key
        let wal_path = self.wal.wal_path().to_path_buf();
        #[cfg(test)]
        if self.inject_wal_recreate_fail_once {
            self.inject_wal_recreate_fail_once = false;
            let err = MuroError::Io(std::io::Error::other(
                "injected WAL recreate failure after rekey",
            ));
            let msg = format!(
                "session poisoned after rekey because WAL writer recreation failed: {}",
                err
            );
            self.poisoned = Some(msg.clone());
            return Err(MuroError::SessionPoisoned(msg));
        }

        self.wal = match WalWriter::create(&wal_path, &new_key) {
            Ok(wal) => wal,
            Err(e) => {
                let msg = format!(
                    "session poisoned after rekey because WAL writer recreation failed: {}",
                    e
                );
                self.poisoned = Some(msg.clone());
                return Err(MuroError::SessionPoisoned(msg));
            }
        };

        Ok(())
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

    /// Get a mutable reference to the WAL writer.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn wal_mut(&mut self) -> &mut WalWriter {
        &mut self.wal
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &SystemCatalog {
        &self.catalog
    }
}

#[cfg(test)]
mod tests;
