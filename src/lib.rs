//! MuroDB: Embedded SQL Database with B+Tree (no leaf links) + FTS (Bigram)
//!
//! A single-file database with:
//! - Pluggable at-rest mode (AES-256-GCM-SIV or plaintext)
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
use crate::crypto::suite::EncryptionSuite;
use crate::error::Result;
use crate::schema::catalog::SystemCatalog;
use crate::sql::executor::{ExecResult, Row};
use crate::sql::session::Session;
use crate::storage::pager::{read_rekey_marker, rekey_marker_path, unwrap_rekey_old_key, Pager};
use crate::wal::recovery::{RecoveryMode, RecoveryResult};
use crate::wal::writer::WalWriter;

/// Main database handle.
pub struct Database {
    session: Session,
    lock_manager: LockManager,
    #[allow(dead_code)]
    master_key: Option<MasterKey>,
    #[allow(dead_code)]
    db_path: PathBuf,
    #[allow(dead_code)]
    encryption_suite: EncryptionSuite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEncryption {
    Encrypted,
    Plaintext,
}

fn wal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_os_string();
    s.push(".wal");
    PathBuf::from(s)
}

/// Migrate legacy sidecar files that used `with_extension()` (which replaces
/// the extension) to the new append-suffix naming.
/// e.g. `mydb.wal` → `mydb.db.wal` when db_path is `mydb.db`.
fn migrate_legacy_sidecar_paths(db_path: &Path) {
    for suffix in &["wal", "lock"] {
        let legacy = db_path.with_extension(suffix);
        let new = {
            let mut s = db_path.as_os_str().to_os_string();
            s.push(".");
            s.push(suffix);
            PathBuf::from(s)
        };
        // Only migrate when the paths actually differ (i.e. db_path had an extension)
        // and the legacy file exists but the new one does not.
        if legacy != new && legacy.exists() && !new.exists() {
            let _ = std::fs::rename(&legacy, &new);
            sync_dir(&new);
        }
    }
}

/// Best-effort directory fsync to persist metadata (new file, rename, truncate).
fn sync_dir(file_path: &Path) {
    if let Some(parent) = file_path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
}

/// Durably reset the WAL to an empty state (header only) after successful recovery.
///
/// ## Durability guarantees
///
/// 1. The WAL file is truncated and rewritten with a valid header.
/// 2. `sync_all()` is called to flush both data and metadata to stable storage,
///    ensuring the truncated WAL survives a subsequent crash.
/// 3. A best-effort directory fsync is performed to persist the file metadata
///    change (size/inode update) on filesystems that require it (e.g. ext4).
///
/// ## Crash during truncation
///
/// If the process crashes *during* this function:
/// - Before `sync_all()`: The old WAL may still be intact on disk.
///   Recovery will simply replay it again on next open — this is idempotent
///   because WAL replay overwrites pages and metadata to the same values.
/// - After `sync_all()` but before directory fsync: The WAL file contents are
///   durable. On most filesystems the directory entry is already updated;
///   the directory fsync is a belt-and-suspenders measure.
fn truncate_wal_durably(wal_path: &Path) -> Result<()> {
    use crate::wal::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};
    use std::io::Write;

    let mut wal_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(wal_path)?;
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
            master_key: Some(master_key.clone()),
            db_path: path.to_path_buf(),
            encryption_suite: EncryptionSuite::Aes256GcmSiv,
        })
    }

    pub fn create_plaintext(path: &Path) -> Result<Self> {
        let mut pager = Pager::create_plaintext(path)?;
        let catalog = SystemCatalog::create(&mut pager)?;
        pager.set_catalog_root(catalog.root_page_id());
        pager.flush_meta()?;

        sync_dir(path);

        let wal = WalWriter::create_plaintext(&wal_path(path))?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok(Database {
            session,
            lock_manager,
            master_key: None,
            db_path: path.to_path_buf(),
            encryption_suite: EncryptionSuite::Plaintext,
        })
    }

    /// Open an existing database.
    pub fn open(path: &Path, master_key: &MasterKey) -> Result<Self> {
        Ok(Self::open_with_recovery_mode_and_report(path, master_key, RecoveryMode::Strict)?.0)
    }

    pub fn open_plaintext(path: &Path) -> Result<Self> {
        Ok(Self::open_plaintext_with_recovery_mode_and_report(path, RecoveryMode::Strict)?.0)
    }

    pub fn open_plaintext_with_recovery_mode(
        path: &Path,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Ok(Self::open_plaintext_with_recovery_mode_and_report(path, recovery_mode)?.0)
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
        migrate_legacy_sidecar_paths(path);
        let wp = wal_path(path);
        let mut recovery_report = None;

        // Run WAL recovery before opening
        if wp.exists() {
            let mut report = crate::wal::recovery::recover_with_mode_and_suite(
                path,
                &wp,
                EncryptionSuite::Aes256GcmSiv,
                Some(master_key),
                recovery_mode,
            )?;
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
                master_key: Some(master_key.clone()),
                db_path: path.to_path_buf(),
                encryption_suite: EncryptionSuite::Aes256GcmSiv,
            },
            recovery_report,
        ))
    }

    pub fn open_plaintext_with_recovery_mode_and_report(
        path: &Path,
        recovery_mode: RecoveryMode,
    ) -> Result<(Self, Option<RecoveryResult>)> {
        migrate_legacy_sidecar_paths(path);
        let wp = wal_path(path);
        let mut recovery_report = None;

        if wp.exists() {
            let mut report = crate::wal::recovery::recover_with_mode_and_suite(
                path,
                &wp,
                EncryptionSuite::Plaintext,
                None,
                recovery_mode,
            )?;
            if recovery_mode == RecoveryMode::Permissive && !report.skipped.is_empty() {
                let quarantine = quarantine_wal_durably(&wp)?;
                report.wal_quarantine_path = Some(quarantine.display().to_string());
            } else {
                truncate_wal_durably(&wp)?;
            }
            recovery_report = Some(report);
        }

        let pager = Pager::open_plaintext(path)?;
        let catalog_root = pager.catalog_root();
        let catalog = SystemCatalog::open(catalog_root);
        let wal = WalWriter::create_plaintext(&wp)?;
        let lock_manager = LockManager::new(path)?;
        let session = Session::new(pager, catalog, wal);

        Ok((
            Database {
                session,
                lock_manager,
                master_key: None,
                db_path: path.to_path_buf(),
                encryption_suite: EncryptionSuite::Plaintext,
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
            master_key: Some(master_key),
            db_path: path.to_path_buf(),
            encryption_suite: EncryptionSuite::Aes256GcmSiv,
        })
    }

    /// Open an existing database with a password.
    ///
    /// If a `.rekey` marker file exists (from a crashed rekey operation), this
    /// will attempt to complete the recovery before opening normally.
    pub fn open_with_password(path: &Path, password: &str) -> Result<Self> {
        Self::recover_interrupted_rekey(path, password)?;
        let info = Pager::read_encryption_info_from_file(path)?;
        if info.suite != EncryptionSuite::Aes256GcmSiv {
            return Err(crate::error::MuroError::Encryption(format!(
                "database uses {}; open with plaintext mode",
                info.suite.as_str()
            )));
        }
        let salt = info.salt;
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
        Self::recover_interrupted_rekey(path, password)?;
        let info = Pager::read_encryption_info_from_file(path)?;
        if info.suite != EncryptionSuite::Aes256GcmSiv {
            return Err(crate::error::MuroError::Encryption(format!(
                "database uses {}; open with plaintext mode",
                info.suite.as_str()
            )));
        }
        let salt = info.salt;
        let master_key = kdf::derive_key(password.as_bytes(), &salt)?;
        Self::open_with_recovery_mode_and_report(path, &master_key, recovery_mode)
    }

    /// Recover from a crashed rekey operation.
    ///
    /// If a `.rekey` marker file exists, this checks whether the rekey completed
    /// (header salt matches marker salt) or needs to be re-run.
    fn recover_interrupted_rekey(path: &Path, password: &str) -> Result<()> {
        let marker = rekey_marker_path(path);
        if !marker.exists() {
            return Ok(());
        }

        let marker_info = read_rekey_marker(&marker)?;
        let new_salt = marker_info.new_salt;
        let new_epoch = marker_info.new_epoch;

        // Read current DB header to check if rekey already completed
        let info = Pager::read_encryption_info_from_file(path)?;
        if info.salt == new_salt {
            // Rekey completed successfully, just remove stale marker
            let _ = std::fs::remove_file(&marker);
            return Ok(());
        }

        // Rekey was interrupted mid-way. We need to complete it.
        // Derive new key from password + marker's new salt.
        let new_key = kdf::derive_key(password.as_bytes(), &new_salt)?;

        // Derive old key from wrapped marker payload.
        let wrapped_old_key = marker_info.wrapped_old_key.ok_or_else(|| {
            crate::error::MuroError::Execution(
                "rekey recovery marker is missing wrapped old key; automatic recovery is unavailable".to_string(),
            )
        })?;
        let old_key = unwrap_rekey_old_key(&new_key, new_epoch, &wrapped_old_key)?;
        let old_epoch = new_epoch.saturating_sub(1);

        // Open the file directly for recovery
        let old_crypto =
            crate::crypto::suite::PageCipher::new(EncryptionSuite::Aes256GcmSiv, Some(&old_key))?;
        let new_crypto =
            crate::crypto::suite::PageCipher::new(EncryptionSuite::Aes256GcmSiv, Some(&new_key))?;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        // Re-read header for page_count
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut header = [0u8; 76];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header)?;
        let page_count = u64::from_le_bytes(header[36..44].try_into().unwrap());

        let page_size_on_disk =
            crate::storage::page::PAGE_SIZE + crate::crypto::aead::PageCrypto::overhead();

        for page_id in 0..page_count {
            let offset = 76 + page_id * page_size_on_disk as u64;
            file.seek(SeekFrom::Start(offset))?;
            let mut encrypted = vec![0u8; page_size_on_disk];
            file.read_exact(&mut encrypted)?;

            // Try decrypting with new key/epoch first (page already re-encrypted)
            let mut plaintext = [0u8; crate::storage::page::PAGE_SIZE];
            let decrypt_result =
                new_crypto.decrypt_into(page_id, new_epoch, &encrypted, &mut plaintext);

            if decrypt_result.is_err() {
                // Page was not yet re-encrypted; decrypt with old key/epoch
                let len =
                    old_crypto.decrypt_into(page_id, old_epoch, &encrypted, &mut plaintext)?;
                if len != crate::storage::page::PAGE_SIZE {
                    return Err(crate::error::MuroError::InvalidPage);
                }

                // Re-encrypt with new key/epoch
                let mut new_encrypted = vec![0u8; page_size_on_disk];
                new_crypto.encrypt_into(page_id, new_epoch, &plaintext, &mut new_encrypted)?;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&new_encrypted)?;
            }
        }

        file.sync_data()?;

        // Update header with new salt and epoch
        header[12..28].copy_from_slice(&new_salt);
        header[44..52].copy_from_slice(&new_epoch.to_le_bytes());
        let checksum = crate::wal::record::crc32(&header[0..72]);
        header[72..76].copy_from_slice(&checksum.to_le_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        file.sync_all()?;

        // Remove marker
        let _ = std::fs::remove_file(&marker);

        Ok(())
    }

    /// Execute a SQL statement. Returns the result.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        let _guard = self.lock_manager.write_lock()?;
        self.session.execute(sql)
    }

    /// Execute a read-only SQL query and return rows.
    ///
    /// This uses a shared lock so multiple readers can run concurrently.
    /// Non-read-only SQL returns an execution error.
    pub fn query(&mut self, sql: &str) -> Result<Vec<Row>> {
        let _guard = self.lock_manager.read_lock()?;
        self.session.execute_read_only_query(sql)
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
