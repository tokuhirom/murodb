//! MuroDB: Encrypted Embedded SQL Database with B-Tree + FTS (Bigram)
//!
//! A single-file encrypted database with:
//! - AES-256-GCM-SIV transparent encryption
//! - B-tree based storage with PRIMARY KEY and UNIQUE indexes
//! - Full-text search with bigram tokenization
//! - WAL-based crash recovery
//! - Multiple readers / single writer concurrency

pub mod error;
pub mod types;
pub mod storage;
pub mod crypto;
pub mod wal;
pub mod btree;
pub mod schema;
pub mod tx;
pub mod sql;
pub mod fts;
pub mod concurrency;

use std::path::{Path, PathBuf};

use crate::concurrency::LockManager;
use crate::crypto::aead::MasterKey;
use crate::error::Result;
use crate::schema::catalog::SystemCatalog;
use crate::sql::executor::{ExecResult, Row};
use crate::storage::pager::Pager;

/// Main database handle.
pub struct Database {
    pager: Pager,
    catalog: SystemCatalog,
    lock_manager: LockManager,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl Database {
    /// Create a new database at the given path.
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let mut pager = Pager::create(path, master_key)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        let lock_manager = LockManager::new(path)?;
        pager.flush_meta()?;

        Ok(Database {
            pager,
            catalog,
            lock_manager,
            db_path: path.to_path_buf(),
        })
    }

    /// Open an existing database. `catalog_root` is the catalog B-tree root page ID.
    pub fn open(path: &Path, master_key: &MasterKey, catalog_root: u64) -> Result<Self> {
        let pager = Pager::open(path, master_key)?;
        let catalog = SystemCatalog::open(catalog_root);
        let lock_manager = LockManager::new(path)?;

        Ok(Database {
            pager,
            catalog,
            lock_manager,
            db_path: path.to_path_buf(),
        })
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let _guard = self.lock_manager.write_lock()?;
        sql::executor::execute(sql, &mut self.pager, &mut self.catalog)
    }

    /// Execute a SQL query and return rows.
    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let _guard = self.lock_manager.read_lock()?;
        match sql::executor::execute(sql, &mut self.pager, &mut self.catalog)? {
            ExecResult::Rows(rows) => Ok(rows),
            _ => Ok(Vec::new()),
        }
    }

    /// Get the catalog root page ID (needed for reopening).
    pub fn catalog_root(&self) -> u64 {
        self.catalog.root_page_id()
    }

    /// Flush all data to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.pager.flush_meta()
    }
}
