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
const DEFAULT_CHECKPOINT_TX_THRESHOLD: u64 = 1;
const DEFAULT_CHECKPOINT_WAL_BYTES_THRESHOLD: u64 = 0;
const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 0;

#[derive(Debug, Clone, Copy)]
struct CheckpointPolicy {
    tx_threshold: u64,
    wal_bytes_threshold: u64,
    interval_ms: u64,
}

impl CheckpointPolicy {
    fn from_env() -> Self {
        Self {
            tx_threshold: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_TX_THRESHOLD",
                DEFAULT_CHECKPOINT_TX_THRESHOLD,
                0,
            ),
            wal_bytes_threshold: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD",
                DEFAULT_CHECKPOINT_WAL_BYTES_THRESHOLD,
                0,
            ),
            interval_ms: parse_checkpoint_env_u64(
                "MURODB_CHECKPOINT_INTERVAL_MS",
                DEFAULT_CHECKPOINT_INTERVAL_MS,
                0,
            ),
        }
    }
}

fn parse_checkpoint_env_u64(name: &str, default: u64, min: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    match raw.parse::<u64>() {
        Ok(v) if v >= min => v,
        Ok(_) => {
            eprintln!(
                "WARNING: {} must be >= {}, using default {}",
                name, min, default
            );
            default
        }
        Err(_) => {
            eprintln!(
                "WARNING: {} must be an integer, using default {}",
                name, default
            );
            default
        }
    }
}

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
        self.refresh_from_disk_if_needed()?;

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

    /// Execute a read-only SQL query and return rows.
    ///
    /// This path avoids auto-commit WAL writes for non-transactional reads.
    pub fn execute_read_only_query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let stmt = parse_sql(sql).map_err(MuroError::Parse)?;

        // Stats queries are always allowed, even on poisoned sessions.
        match &stmt {
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

        if !Self::is_read_only_statement(&stmt) {
            return Err(MuroError::Execution(
                "Database::query accepts read-only SQL only; use execute() for writes".into(),
            ));
        }

        if self.active_tx.is_some() {
            Self::rows_from_exec_result(self.execute_in_tx(&stmt))
        } else {
            // Read directly from pager/catalog without opening an implicit WAL transaction.
            Self::rows_from_exec_result(execute_statement(
                &stmt,
                &mut self.pager,
                &mut self.catalog,
            ))
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
            | Statement::Rollback => false,
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

    fn post_commit_checkpoint(&mut self) {
        self.post_checkpoint("post-commit");
    }

    fn post_rollback_checkpoint(&mut self) {
        self.post_checkpoint("post-rollback");
    }

    // FIXME: Replace string phase labels with an enum to prevent typos.
    fn post_checkpoint(&mut self, phase: &str) {
        self.pending_checkpoint_ops = self.pending_checkpoint_ops.saturating_add(1);
        if !self.should_checkpoint_now() {
            self.stats.deferred_checkpoints += 1;
            return;
        }
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
            self.emit_checkpoint_warning(phase, attempts, &e);
            return;
        }
        self.pending_checkpoint_ops = 0;
        self.last_checkpoint_at = std::time::Instant::now();
    }

    fn should_checkpoint_now(&self) -> bool {
        if self.pending_checkpoint_ops == 0 {
            return false;
        }
        if self.checkpoint_policy.tx_threshold > 0 {
            if self.checkpoint_policy.tx_threshold <= 1 {
                return true;
            }
            if self.pending_checkpoint_ops >= self.checkpoint_policy.tx_threshold {
                return true;
            }
        }
        if self.checkpoint_policy.wal_bytes_threshold > 0 {
            if let Ok(size) = self.wal.file_size_bytes() {
                if size >= self.checkpoint_policy.wal_bytes_threshold {
                    return true;
                }
            }
        }
        if self.checkpoint_policy.interval_ms > 0
            && self.last_checkpoint_at.elapsed().as_millis() as u64
                >= self.checkpoint_policy.interval_ms
        {
            return true;
        }
        false
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
        let cache_hits = self.pager.cache_hits();
        let cache_misses = self.pager.cache_misses();
        let cache_total = cache_hits.saturating_add(cache_misses);
        let cache_hit_rate_pct = if cache_total == 0 {
            0.0
        } else {
            (cache_hits as f64 * 100.0) / (cache_total as f64)
        };
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
            stat_row(
                "deferred_checkpoints",
                stats.deferred_checkpoints.to_string(),
            ),
            stat_row(
                "checkpoint_pending_ops",
                self.pending_checkpoint_ops.to_string(),
            ),
            stat_row(
                "checkpoint_policy_tx_threshold",
                self.checkpoint_policy.tx_threshold.to_string(),
            ),
            stat_row(
                "checkpoint_policy_wal_bytes_threshold",
                self.checkpoint_policy.wal_bytes_threshold.to_string(),
            ),
            stat_row(
                "checkpoint_policy_interval_ms",
                self.checkpoint_policy.interval_ms.to_string(),
            ),
            stat_row("pager_cache_hits", cache_hits.to_string()),
            stat_row("pager_cache_misses", cache_misses.to_string()),
            stat_row(
                "pager_cache_hit_rate_pct",
                format!("{:.2}", cache_hit_rate_pct),
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

    #[cfg(test)]
    fn set_checkpoint_policy_for_test(
        &mut self,
        tx_threshold: u64,
        wal_threshold: u64,
        interval_ms: u64,
    ) {
        self.checkpoint_policy = CheckpointPolicy {
            tx_threshold,
            wal_bytes_threshold: wal_threshold,
            interval_ms,
        };
    }
}

#[cfg(test)]
mod tests;
