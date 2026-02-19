use crate::error::{MuroError, Result};
use crate::schema::catalog::SystemCatalog;
use crate::sql::ast::Statement;
use crate::sql::executor::{execute_statement, ExecResult};
use crate::sql::parser::parse_sql;
use crate::storage::pager::Pager;
use crate::tx::page_store::TxPageStore;
use crate::tx::transaction::Transaction;
use crate::wal::record::TxId;

/// A session that manages explicit transaction state.
///
/// - `BEGIN` starts a transaction (dirty-page buffering).
/// - `COMMIT` flushes dirty pages to disk.
/// - `ROLLBACK` discards dirty pages.
/// - Without `BEGIN`, statements execute in auto-commit mode (direct pager I/O).
pub struct Session {
    pager: Pager,
    catalog: SystemCatalog,
    active_tx: Option<Transaction>,
    next_txid: TxId,
}

impl Session {
    pub fn new(pager: Pager, catalog: SystemCatalog) -> Self {
        Session {
            pager,
            catalog,
            active_tx: None,
            next_txid: 1,
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
                    // Auto-commit: execute directly against pager
                    execute_statement(&stmt, &mut self.pager, &mut self.catalog)
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
        self.active_tx = Some(Transaction::begin(txid, 0));
        Ok(ExecResult::Ok)
    }

    fn handle_commit(&mut self) -> Result<ExecResult> {
        let mut tx = self
            .active_tx
            .take()
            .ok_or_else(|| MuroError::Transaction("No active transaction".into()))?;
        tx.commit_no_wal(&mut self.pager)?;
        // Update catalog root in pager header
        self.pager.set_catalog_root(self.catalog.root_page_id());
        self.pager.flush_meta()?;
        Ok(ExecResult::Ok)
    }

    fn handle_rollback(&mut self) -> Result<ExecResult> {
        let mut tx = self
            .active_tx
            .take()
            .ok_or_else(|| MuroError::Transaction("No active transaction".into()))?;
        tx.rollback_no_wal();
        // Reload catalog from disk since in-memory catalog may have been modified
        let catalog_root = self.pager.catalog_root();
        self.catalog = SystemCatalog::open(catalog_root);
        Ok(ExecResult::Ok)
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
}
