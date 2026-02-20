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
use crate::sql::session::Session;
use crate::storage::pager::Pager;
use crate::wal::writer::WalWriter;

/// Main database handle.
pub struct Database {
    session: Session,
    lock_manager: LockManager,
    #[allow(dead_code)]
    master_key: MasterKey,
    #[allow(dead_code)]
    db_path: PathBuf,
}

fn wal_path(db_path: &Path) -> PathBuf {
    db_path.with_extension("wal")
}

fn truncate_wal_durably(wal_path: &Path) -> Result<()> {
    // Truncate and fsync WAL file so recovery effects become durable.
    let wal_file = std::fs::File::create(wal_path)?;
    wal_file.sync_all()?;

    // Best-effort directory fsync to persist metadata updates (size/truncate).
    if let Some(parent) = wal_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

impl Database {
    /// Create a new database at the given path.
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let mut pager = Pager::create(path, master_key)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        pager.set_catalog_root(catalog.root_page_id());
        pager.flush_meta()?;

        let wal = WalWriter::create(&wal_path(path), master_key)?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok(Database {
            session,
            lock_manager,
            master_key: master_key.clone(),
            db_path: path.to_path_buf(),
        })
    }

    /// Open an existing database.
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let wp = wal_path(path);

        // Run WAL recovery before opening
        if wp.exists() {
            crate::wal::recovery::recover(path, &wp, master_key)?;
            // Truncate WAL after successful recovery
            truncate_wal_durably(&wp)?;
        }

        let pager = Pager::open(path, master_key)?;
        let catalog_root = pager.catalog_root();
        let catalog = SystemCatalog::open(catalog_root);
        let wal = WalWriter::create(&wp, master_key)?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok(Database {
            session,
            lock_manager,
            master_key: master_key.clone(),
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
        pager.flush_meta()?;

        let wal = WalWriter::create(&wal_path(path), &master_key)?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok(Database {
            session,
            lock_manager,
            master_key,
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
        self.session.execute(sql)
    }

    /// Execute a SQL query and return rows.
    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let _guard = self.lock_manager.read_lock()?;
        match self.session.execute(sql)? {
            ExecResult::Rows(rows) => Ok(rows),
            _ => Ok(Vec::new()),
        }
    }

    /// Get the catalog root page ID (needed for reopening).
    pub fn catalog_root(&self) -> u64 {
        self.session.catalog().root_page_id()
    }

    /// Flush all data to disk.
    pub fn flush(&mut self) -> Result<()> {
        let catalog_root = self.session.catalog().root_page_id();
        let pager = self.session.pager_mut();
        pager.set_catalog_root(catalog_root);
        pager.flush_meta()
    }

    /// Create a `Session` that supports BEGIN/COMMIT/ROLLBACK.
    ///
    /// This consumes the Database and returns a Session. The Session owns the
    /// pager, catalog, and WAL writer, and manages explicit transaction state.
    pub fn into_session(self) -> Session {
        self.session
    }
}
