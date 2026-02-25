/// Concurrency control: thread RwLock + process file lock.
///
/// Multiple readers, single writer model.
/// Thread-level: parking_lot::RwLock
/// Process-level: fs4 file lock
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fs4::fs_std::FileExt;
use parking_lot::RwLock;

use crate::error::{MuroError, Result};

/// Database lock manager combining thread-level and process-level locks.
pub struct LockManager {
    /// Thread-level RwLock for concurrent access within a single process.
    rw_lock: RwLock<()>,
    /// File used for process-level locking.
    lock_file: File,
}

impl LockManager {
    pub fn new(db_path: &Path) -> Result<Self> {
        let mut lock_os = db_path.as_os_str().to_os_string();
        lock_os.push(".lock");
        let lock_path = PathBuf::from(lock_os);
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        Ok(LockManager {
            rw_lock: RwLock::new(()),
            lock_file,
        })
    }

    /// Acquire a shared (read) lock.
    pub fn read_lock(&self) -> Result<ReadGuard<'_>> {
        self.read_lock_with_timeout(None)
    }

    /// Acquire a shared (read) lock with timeout.
    ///
    /// If `timeout` is `None`, this blocks until acquired.
    pub fn read_lock_with_timeout(&self, timeout: Option<Duration>) -> Result<ReadGuard<'_>> {
        let timeout_ms = timeout.map(|d| d.as_millis() as u64).unwrap_or(0);
        let thread_guard = if let Some(timeout) = timeout {
            self.rw_lock
                .try_read_for(timeout)
                .ok_or(MuroError::LockTimeout {
                    mode: "shared",
                    timeout_ms,
                })?
        } else {
            self.rw_lock.read()
        };

        if let Some(timeout) = timeout {
            let deadline = Instant::now() + timeout;
            loop {
                match self.lock_file.try_lock_shared() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(MuroError::LockTimeout {
                                mode: "shared",
                                timeout_ms,
                            });
                        }
                        let remaining = deadline.saturating_duration_since(now);
                        std::thread::sleep(std::cmp::min(Duration::from_millis(1), remaining));
                    }
                    Err(std::fs::TryLockError::Error(e)) => {
                        return Err(MuroError::Lock(format!(
                            "Failed to acquire shared file lock: {}",
                            e
                        )))
                    }
                }
            }
        } else {
            self.lock_file.lock_shared().map_err(|e| {
                MuroError::Lock(format!("Failed to acquire shared file lock: {}", e))
            })?;
        }

        Ok(ReadGuard {
            _thread_guard: thread_guard,
            lock_file: &self.lock_file,
        })
    }

    /// Acquire an exclusive (write) lock.
    pub fn write_lock(&self) -> Result<WriteGuard<'_>> {
        self.write_lock_with_timeout(None)
    }

    /// Acquire an exclusive (write) lock with timeout.
    ///
    /// If `timeout` is `None`, this blocks until acquired.
    pub fn write_lock_with_timeout(&self, timeout: Option<Duration>) -> Result<WriteGuard<'_>> {
        let timeout_ms = timeout.map(|d| d.as_millis() as u64).unwrap_or(0);
        let thread_guard = if let Some(timeout) = timeout {
            self.rw_lock
                .try_write_for(timeout)
                .ok_or(MuroError::LockTimeout {
                    mode: "exclusive",
                    timeout_ms,
                })?
        } else {
            self.rw_lock.write()
        };

        if let Some(timeout) = timeout {
            let deadline = Instant::now() + timeout;
            loop {
                match self.lock_file.try_lock_exclusive() {
                    Ok(()) => break,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        let now = Instant::now();
                        if now >= deadline {
                            return Err(MuroError::LockTimeout {
                                mode: "exclusive",
                                timeout_ms,
                            });
                        }
                        let remaining = deadline.saturating_duration_since(now);
                        std::thread::sleep(std::cmp::min(Duration::from_millis(1), remaining));
                    }
                    Err(e) => {
                        return Err(MuroError::Lock(format!(
                            "Failed to acquire exclusive file lock: {}",
                            e
                        )))
                    }
                }
            }
        } else {
            self.lock_file.lock_exclusive().map_err(|e| {
                MuroError::Lock(format!("Failed to acquire exclusive file lock: {}", e))
            })?;
        }

        Ok(WriteGuard {
            _thread_guard: thread_guard,
            lock_file: &self.lock_file,
        })
    }
}

pub struct ReadGuard<'a> {
    _thread_guard: parking_lot::RwLockReadGuard<'a, ()>,
    lock_file: &'a File,
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

pub struct WriteGuard<'a> {
    _thread_guard: parking_lot::RwLockWriteGuard<'a, ()>,
    lock_file: &'a File,
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_concurrent_readers() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        File::create(&db_path).unwrap();

        let lock_mgr = Arc::new(LockManager::new(&db_path).unwrap());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let lm = lock_mgr.clone();
                thread::spawn(move || {
                    let _guard = lm.read_lock().unwrap();
                    // Simulate read work
                    thread::sleep(std::time::Duration::from_millis(10));
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_writer_excludes_readers() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        File::create(&db_path).unwrap();

        let lock_mgr = Arc::new(LockManager::new(&db_path).unwrap());
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Writer
        let lm = lock_mgr.clone();
        let c = counter.clone();
        let writer = thread::spawn(move || {
            let _guard = lm.write_lock().unwrap();
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            thread::sleep(std::time::Duration::from_millis(50));
            c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

        // Small delay to let writer acquire lock first
        thread::sleep(std::time::Duration::from_millis(5));

        // Reader (should wait for writer)
        let lm = lock_mgr.clone();
        let c = counter.clone();
        let reader = thread::spawn(move || {
            let _guard = lm.read_lock().unwrap();
            // By the time we get the lock, writer should have incremented twice
            let val = c.load(std::sync::atomic::Ordering::SeqCst);
            assert!(val >= 2, "Reader got lock before writer finished: {}", val);
        });

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn test_read_lock_timeout_when_writer_is_held() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        File::create(&db_path).unwrap();

        let lock_mgr = LockManager::new(&db_path).unwrap();
        let _writer_guard = lock_mgr.write_lock().unwrap();

        let err = match lock_mgr.read_lock_with_timeout(Some(std::time::Duration::from_millis(20)))
        {
            Err(err) => err,
            Ok(_) => panic!("read lock should time out while writer lock is held"),
        };
        assert!(matches!(err, MuroError::LockTimeout { mode: "shared", .. }));
    }
}
