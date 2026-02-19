use murodb::concurrency::LockManager;
use std::fs::File;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_multiple_readers() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    File::create(&db_path).unwrap();

    let lm = Arc::new(LockManager::new(&db_path).unwrap());

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let lm = lm.clone();
            thread::spawn(move || {
                for _ in 0..10 {
                    let _guard = lm.read_lock().unwrap();
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_writer_serialization() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    File::create(&db_path).unwrap();

    let lm = Arc::new(LockManager::new(&db_path).unwrap());
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let lm = lm.clone();
            let c = counter.clone();
            thread::spawn(move || {
                for _ in 0..5 {
                    let _guard = lm.write_lock().unwrap();
                    let val = c.load(std::sync::atomic::Ordering::SeqCst);
                    // No other writer should be active, so increment should be safe
                    c.store(val + 1, std::sync::atomic::Ordering::SeqCst);
                    thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 20);
}
