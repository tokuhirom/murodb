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
use std::time::{SystemTime, UNIX_EPOCH};

use crate::concurrency::LockManager;
use crate::crypto::aead::MasterKey;
use crate::crypto::kdf;
use crate::error::Result;
use crate::schema::catalog::SystemCatalog;
use crate::sql::executor::{ExecResult, Row};
use crate::sql::session::Session;
use crate::storage::pager::Pager;
use crate::wal::recovery::{RecoveryMode, RecoveryResult};
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

/// Best-effort directory fsync to persist metadata (new file, rename, truncate).
fn sync_dir(file_path: &Path) {
    if let Some(parent) = file_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
}

fn truncate_wal_durably(wal_path: &Path) -> Result<()> {
    // Truncate WAL to just the header and fsync so recovery effects become durable.
    use crate::wal::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};
    use std::io::Write;

    let mut wal_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(wal_path)?;
    // Write WAL header
    let mut header = [0u8; WAL_HEADER_SIZE];
    header[0..8].copy_from_slice(WAL_MAGIC);
    header[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    wal_file.write_all(&header)?;
    wal_file.sync_all()?;

    // Best-effort directory fsync to persist metadata updates (size/truncate).
    sync_dir(wal_path);
    Ok(())
}

fn quarantine_wal_durably(wal_path: &Path) -> Result<PathBuf> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();

    let mut dest = PathBuf::from(format!("{}.quarantine.{}.{}", wal_path.display(), ts, pid));
    let mut attempt = 0u32;
    while dest.exists() {
        attempt += 1;
        dest = PathBuf::from(format!(
            "{}.quarantine.{}.{}.{}",
            wal_path.display(),
            ts,
            pid,
            attempt
        ));
    }

    std::fs::rename(wal_path, &dest)?;
    sync_dir(wal_path);

    Ok(dest)
}

impl Database {
    /// Create a new database at the given path.
    pub fn create(path: &Path, master_key: &MasterKey) -> Result<Self> {
        let mut pager = Pager::create(path, master_key)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        pager.set_catalog_root(catalog.root_page_id());
        pager.flush_meta()?;

        // Directory fsync to persist the newly created DB file metadata
        sync_dir(path);

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
        Ok(Self::open_with_recovery_mode_and_report(path, master_key, RecoveryMode::Strict)?.0)
    }

    /// Open an existing database with configurable WAL recovery behavior.
    pub fn open_with_recovery_mode(
        path: &Path,
        master_key: &MasterKey,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Ok(Self::open_with_recovery_mode_and_report(path, master_key, recovery_mode)?.0)
    }

    /// Open an existing database with configurable WAL recovery behavior and return recovery report.
    pub fn open_with_recovery_mode_and_report(
        path: &Path,
        master_key: &MasterKey,
        recovery_mode: RecoveryMode,
    ) -> Result<(Self, Option<RecoveryResult>)> {
        let wp = wal_path(path);
        let mut recovery_report = None;

        // Run WAL recovery before opening
        if wp.exists() {
            let mut report =
                crate::wal::recovery::recover_with_mode(path, &wp, master_key, recovery_mode)?;
            if recovery_mode == RecoveryMode::Permissive && !report.skipped.is_empty() {
                let quarantine = quarantine_wal_durably(&wp)?;
                report.wal_quarantine_path = Some(quarantine.display().to_string());
            } else {
                // Truncate WAL after successful recovery
                truncate_wal_durably(&wp)?;
            }
            recovery_report = Some(report);
        }

        let pager = Pager::open(path, master_key)?;
        let catalog_root = pager.catalog_root();
        let catalog = SystemCatalog::open(catalog_root);
        let wal = WalWriter::create(&wp, master_key)?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok((
            Database {
                session,
                lock_manager,
                master_key: master_key.clone(),
                db_path: path.to_path_buf(),
            },
            recovery_report,
        ))
    }

    /// Create a new database with a password.
    pub fn create_with_password(path: &Path, password: &str) -> Result<Self> {
        let salt = kdf::generate_salt();
        let master_key = kdf::derive_key(password.as_bytes(), &salt)?;
        let mut pager = Pager::create_with_salt(path, &master_key, salt)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        pager.set_catalog_root(catalog.root_page_id());
        pager.flush_meta()?;

        // Directory fsync to persist the newly created DB file metadata
        sync_dir(path);

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

    /// Open an existing database with a password and configurable recovery behavior.
    pub fn open_with_password_and_recovery_mode(
        path: &Path,
        password: &str,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Ok(Self::open_with_password_and_recovery_mode_and_report(path, password, recovery_mode)?.0)
    }

    /// Open an existing database with a password, configurable recovery mode, and return recovery report.
    pub fn open_with_password_and_recovery_mode_and_report(
        path: &Path,
        password: &str,
        recovery_mode: RecoveryMode,
    ) -> Result<(Self, Option<RecoveryResult>)> {
        let salt = Pager::read_salt_from_file(path)?;
        let master_key = kdf::derive_key(password.as_bytes(), &salt)?;
        Self::open_with_recovery_mode_and_report(path, &master_key, recovery_mode)
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let _guard = self.lock_manager.write_lock()?;
        self.session.execute(sql)
    }

    /// Execute a SQL query and return rows.
    /// Uses a write lock because auto-commit SELECTs may write to WAL.
    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let _guard = self.lock_manager.write_lock()?;
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
