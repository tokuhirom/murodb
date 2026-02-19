//! MuroDB: Encrypted Embedded SQL Database with B-Tree + FTS (Bigram)
//!
//! A single-file encrypted database with:
//! - AES-256-GCM-SIV transparent encryption
//! - B-tree based storage with PRIMARY KEY and UNIQUE indexes
//! - Full-text search with bigram tokenization
//! - WAL-based crash recovery
//! - Multiple readers / single writer concurrency

pub mod btree;
pub mod concurrency;
pub mod crypto;
pub mod error;
pub mod fts;
pub mod schema;
pub mod sql;
pub mod storage;
pub mod tx;
pub mod types;
pub mod wal;

use std::path::{Path, PathBuf};

use crate::concurrency::LockManager;
use crate::crypto::aead::MasterKey;
use crate::crypto::kdf;
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
        pager.set_catalog_root(catalog.root_page_id());
        let lock_manager = LockManager::new(path)?;
        pager.flush_meta()?;

        Ok(Database {
            pager,
            catalog,
            lock_manager,
            db_path: path.to_path_buf(),
        })
    }

    /// Open an existing database.
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let pager = Pager::open(path, master_key)?;
        let catalog_root = pager.catalog_root();
        let catalog = SystemCatalog::open(catalog_root);
        let lock_manager = LockManager::new(path)?;

        Ok(Database {
            pager,
            catalog,
            lock_manager,
            db_path: path.to_path_buf(),
        })
    }

    /// Create a new database with a password.
    pub fn create_with_password(path: &Path, password: &str) -> Result<Self> {
        let salt = kdf::generate_salt();
        let master_key = kdf::derive_key(password.as_bytes(), &salt)?;
        let mut pager = Pager::create_with_salt(path, &master_key, salt)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        pager.set_catalog_root(catalog.root_page_id());
        let lock_manager = LockManager::new(path)?;
        pager.flush_meta()?;

        Ok(Database {
            pager,
            catalog,
            lock_manager,
            db_path: path.to_path_buf(),
        })
    }

    /// Open an existing database with a password.
    pub fn open_with_password(path: &Path, password: &str) -> Result<Self> {
        let salt = Pager::read_salt_from_file(path)?;
        let master_key = kdf::derive_key(password.as_bytes(), &salt)?;
        Self::open(path, &master_key)
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
        self.pager.set_catalog_root(self.catalog.root_page_id());
        self.pager.flush_meta()
    }
}
