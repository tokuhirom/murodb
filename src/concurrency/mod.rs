/// Concurrency control: thread RwLock + process file lock.
///
/// Multiple readers, single writer model.
/// Thread-level: parking_lot::RwLock
/// Process-level: fs4 file lock

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs4::fs_std::FileExt;
use parking_lot::RwLock;

use crate::error::{MuroError, Result};

/// Database lock manager combining thread-level and process-level locks.
pub struct LockManager {
    /// Thread-level RwLock for concurrent access within a single process.
    rw_lock: RwLock<()>,
    /// File used for process-level locking.
    lock_file: File,
    #[allow(dead_code)]
    lock_path: PathBuf,
}

impl LockManager {
    pub fn new(db_path: &Path) -> Result<Self> {
        let lock_path = db_path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        Ok(LockManager {
            rw_lock: RwLock::new(()),
            lock_file,
            lock_path,
        })
    }

    /// Acquire a shared (read) lock.
    pub fn read_lock(&self) -> Result<ReadGuard<'_>> {
        let thread_guard = self.rw_lock.read();

        self.lock_file
            .lock_shared()
            .map_err(|e| MuroError::Lock(format!("Failed to acquire shared file lock: {}", e)))?;

        Ok(ReadGuard {
            _thread_guard: thread_guard,
            lock_file: &self.lock_file,
        })
    }

    /// Acquire an exclusive (write) lock.
    pub fn write_lock(&self) -> Result<WriteGuard<'_>> {
        let thread_guard = self.rw_lock.write();

        self.lock_file
            .lock_exclusive()
            .map_err(|e| MuroError::Lock(format!("Failed to acquire exclusive file lock: {}", e)))?;

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
}
