/// Crash/kill durability stress test.
///
/// Spawns worker processes that perform randomized write workloads, kills them
/// at random points via SIGKILL, then verifies database invariants after WAL
/// recovery. This catches rare interleavings and filesystem timing effects that
/// deterministic crash tests miss.
///
/// Run: `cargo test --test crash_stress -- --harness --iterations 20 --seed 42`
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration;

use murodb::crypto::aead::MasterKey;
use murodb::types::Value;
use murodb::Database;

// ── PRNG (xorshift64) ──

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid zero state which is a fixed point for xorshift
        Self(if seed == 0 { 0xDEAD_BEEF } else { seed })
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn next_range(&mut self, max: u64) -> u64 {
        self.next() % max
    }
}

fn test_key_from_hex(hex: &str) -> MasterKey {
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    MasterKey::new(bytes)
}

fn test_key() -> MasterKey {
    MasterKey::new([0x42u8; 32])
}

fn key_hex() -> String {
    "42".repeat(32)
}

// ── Worker mode ──

fn run_worker(db_path: &Path, master_key: &MasterKey, seed: u64, journal_path: &Path) {
    let mut rng = Rng::new(seed);

    // Open (or create) the database
    let mut db = if db_path.exists() {
        Database::open(db_path, master_key).expect("worker: open failed")
    } else {
        Database::create(db_path, master_key).expect("worker: create failed")
    };

    // Ensure table exists (CREATE TABLE IF NOT EXISTS)
    db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, name VARCHAR(255))")
        .expect("worker: CREATE TABLE failed");

    // Open journal for append
    let mut journal = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal_path)
        .expect("worker: open journal failed");

    // Workload loop — runs until killed
    loop {
        let op = rng.next_range(100);
        let id = rng.next_range(100_000) as i64;
        let name = format!("name_{}", rng.next_range(1_000_000));

        let result = if op < 60 {
            // INSERT (60%)
            let sql = format!("INSERT INTO t (id, name) VALUES ({}, '{}')", id, name);
            match db.execute(&sql) {
                Ok(_) => {
                    // Record: INSERT id name
                    writeln!(journal, "INSERT {} {}", id, name).ok();
                    journal_fsync(&journal);
                    Ok(())
                }
                Err(_) => Ok(()), // duplicate key etc — ignore
            }
        } else if op < 80 {
            // DELETE (20%)
            let sql = format!("DELETE FROM t WHERE id = {}", id);
            match db.execute(&sql) {
                Ok(_) => {
                    writeln!(journal, "DELETE {}", id).ok();
                    journal_fsync(&journal);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else if op < 90 {
            // UPDATE (10%)
            let sql = format!("UPDATE t SET name = '{}' WHERE id = {}", name, id);
            match db.execute(&sql) {
                Ok(_) => {
                    writeln!(journal, "UPDATE {} {}", id, name).ok();
                    journal_fsync(&journal);
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            // SELECT (10%) — no journal entry
            let sql = format!("SELECT id, name FROM t WHERE id = {}", id);
            db.query(&sql).map(|_| ())
        };

        if let Err(e) = result {
            eprintln!("worker: op error (non-fatal): {}", e);
        }
    }
}

fn journal_fsync(file: &fs::File) {
    file.sync_all().expect("journal fsync failed");
}

// ── Journal parsing ──

/// Parse journal entries into a model of committed state.
/// Returns `HashMap<id, name>` for rows that should exist.
fn parse_journal(journal_path: &Path) -> HashMap<i64, String> {
    let mut state: HashMap<i64, String> = HashMap::new();

    if !journal_path.exists() {
        return state;
    }

    let file = fs::File::open(journal_path).expect("parse_journal: open failed");
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // truncated line from kill
        };
        let parts: Vec<&str> = line.splitn(3, ' ').collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "INSERT" if parts.len() >= 3 => {
                let id: i64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => continue, // truncated by kill
                };
                let name = parts[2].to_string();
                state.insert(id, name);
            }
            "DELETE" if parts.len() >= 2 => {
                let id: i64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                state.remove(&id);
            }
            "UPDATE" if parts.len() >= 3 => {
                let id: i64 = match parts[1].parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let name = parts[2].to_string();
                // UPDATE only affects existing rows
                if state.contains_key(&id) {
                    state.insert(id, name);
                }
            }
            _ => {} // skip malformed lines (possibly truncated by kill)
        }
    }

    state
}

// ── Invariant verification ──

fn verify_invariants(
    db_path: &Path,
    master_key: &MasterKey,
    committed: &HashMap<i64, String>,
    iteration: u64,
    seed: u64,
) {
    // 1. Database::open succeeds (WAL recovery works)
    let mut db = Database::open(db_path, master_key).unwrap_or_else(|e| {
        panic!(
            "FAIL iteration {}: Database::open failed: {}\n  Reproduce: --seed {} --iterations {}",
            iteration,
            e,
            seed,
            iteration + 1
        );
    });

    // 2. Full table scan succeeds
    let rows = db.query("SELECT id, name FROM t").unwrap_or_else(|e| {
        panic!(
            "FAIL iteration {}: SELECT failed: {}\n  Reproduce: --seed {} --iterations {}",
            iteration,
            e,
            seed,
            iteration + 1
        );
    });

    // Build map of actual DB contents
    let mut actual: HashMap<i64, String> = HashMap::new();
    for row in &rows {
        let id = match row.get("id") {
            Some(Value::Integer(v)) => *v,
            other => panic!(
                "FAIL iteration {}: unexpected id value: {:?}",
                iteration, other
            ),
        };
        let name = match row.get("name") {
            Some(Value::Varchar(v)) => v.clone(),
            other => panic!(
                "FAIL iteration {}: unexpected name value: {:?}",
                iteration, other
            ),
        };
        actual.insert(id, name);
    }

    // 3. All journal-committed rows must be visible with correct values
    for (id, expected_name) in committed {
        match actual.get(id) {
            Some(actual_name) => {
                assert_eq!(
                    actual_name, expected_name,
                    "FAIL iteration {}: row id={} has name='{}' but journal says '{}'\n  Reproduce: --seed {} --iterations {}",
                    iteration, id, actual_name, expected_name, seed, iteration + 1
                );
            }
            None => {
                panic!(
                    "FAIL iteration {}: committed row id={} missing from DB\n  Reproduce: --seed {} --iterations {}",
                    iteration, id, seed, iteration + 1
                );
            }
        }
    }

    // 4. Post-recovery INSERT + DELETE works (no page ID collision / corruption)
    let post_id = 999_999;
    db.execute(&format!(
        "INSERT INTO t (id, name) VALUES ({}, 'post_recovery')",
        post_id
    ))
    .unwrap_or_else(|e| {
        panic!(
            "FAIL iteration {}: post-recovery INSERT failed: {}\n  Reproduce: --seed {} --iterations {}",
            iteration, e, seed, iteration + 1
        );
    });

    db.execute(&format!("DELETE FROM t WHERE id = {}", post_id))
        .unwrap_or_else(|e| {
            panic!(
                "FAIL iteration {}: post-recovery DELETE failed: {}\n  Reproduce: --seed {} --iterations {}",
                iteration, e, seed, iteration + 1
            );
        });
}

// ── Kill timing strategy ──

fn kill_delay_ms(rng: &mut Rng, iteration: u64) -> u64 {
    match iteration % 5 {
        0 => 0,                          // immediate
        1 => rng.next_range(11),         // 0–10ms
        2 => rng.next_range(101),        // 0–100ms
        3 => 100 + rng.next_range(501),  // 100–600ms
        4 => 500 + rng.next_range(2001), // 500–2500ms
        _ => unreachable!(),
    }
}

// ── Failure artifact saving ──

fn save_failure_artifacts(
    artifact_dir: &Path,
    db_path: &Path,
    journal_path: &Path,
    iteration: u64,
    seed: u64,
) {
    let _ = fs::create_dir_all(artifact_dir);
    let _ = fs::copy(db_path, artifact_dir.join("stress.db"));
    // Also copy the WAL file if present
    let wal_path = db_path.with_extension("wal");
    if wal_path.exists() {
        let _ = fs::copy(&wal_path, artifact_dir.join("stress.wal"));
    }
    if journal_path.exists() {
        let _ = fs::copy(journal_path, artifact_dir.join("journal.log"));
    }
    let repro = format!(
        "cargo test --test crash_stress -- --harness --iterations {} --seed {}\nFailed at iteration {}",
        iteration + 1,
        seed,
        iteration
    );
    let _ = fs::write(artifact_dir.join("repro.txt"), repro);
    eprintln!("  Failure artifacts saved to: {}", artifact_dir.display());
}

// ── Harness mode ──

fn run_harness(iterations: u64, base_seed: u64) {
    eprintln!(
        "=== Crash stress test: {} iterations, base seed {} ===",
        iterations, base_seed
    );
    eprintln!(
        "Reproduce: cargo test --test crash_stress -- --harness --iterations {} --seed {}",
        iterations, base_seed
    );

    // Artifact directory for CI upload on failure
    let artifact_dir = PathBuf::from(
        env::var("CRASH_STRESS_ARTIFACT_DIR")
            .unwrap_or_else(|_| "/tmp/crash_stress_artifacts".to_string()),
    );

    let dir = tempfile::TempDir::new().expect("create tempdir");
    let db_path = dir.path().join("stress.db");
    let journal_path = dir.path().join("journal.log");
    let master_key = test_key();
    let hex = key_hex();

    // Create initial database
    {
        let mut db = Database::create(&db_path, &master_key).expect("create DB");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(255))")
            .expect("CREATE TABLE");
    }

    let exe = env::current_exe().expect("current_exe");

    for i in 0..iterations {
        let iter_seed = base_seed.wrapping_mul(6364136223846793005).wrapping_add(i);
        let mut rng = Rng::new(iter_seed);

        let delay = kill_delay_ms(&mut rng, i);

        // Spawn worker
        let mut child = Command::new(&exe)
            .arg("--worker")
            .arg("--db-path")
            .arg(&db_path)
            .arg("--key-hex")
            .arg(&hex)
            .arg("--seed")
            .arg(iter_seed.to_string())
            .arg("--journal")
            .arg(&journal_path)
            .spawn()
            .expect("spawn worker");

        // Wait, then kill
        if delay > 0 {
            thread::sleep(Duration::from_millis(delay));
        }
        let _ = child.kill(); // SIGKILL on Unix
        let status = child.wait().expect("wait for child");
        assert!(!status.success(), "worker should have been killed");

        // Parse journal and update committed state model
        let committed = parse_journal(&journal_path);

        // Verify invariants — on failure, save artifacts for CI
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            verify_invariants(&db_path, &master_key, &committed, i, base_seed);
        }));
        if let Err(payload) = result {
            save_failure_artifacts(&artifact_dir, &db_path, &journal_path, i, base_seed);
            std::panic::resume_unwind(payload);
        }

        if (i + 1) % 10 == 0 || i == iterations - 1 {
            eprintln!(
                "  iteration {}/{}: OK ({} committed rows, kill after {}ms)",
                i + 1,
                iterations,
                committed.len(),
                delay
            );
        }
    }

    eprintln!("=== All {} iterations passed ===", iterations);
}

// ── Argument parsing ──

struct Args {
    mode: Mode,
}

enum Mode {
    Harness {
        iterations: u64,
        seed: u64,
    },
    Worker {
        db_path: PathBuf,
        key_hex: String,
        seed: u64,
        journal: PathBuf,
    },
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();

    if args.iter().any(|a| a == "--harness") {
        let iterations = get_arg_value(&args, "--iterations")
            .map(|v| v.parse::<u64>().expect("invalid --iterations"))
            .unwrap_or(50);
        let seed = get_arg_value(&args, "--seed")
            .map(|v| v.parse::<u64>().expect("invalid --seed"))
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        Args {
            mode: Mode::Harness { iterations, seed },
        }
    } else if args.iter().any(|a| a == "--worker") {
        let db_path = PathBuf::from(
            get_arg_value(&args, "--db-path").expect("--db-path required for worker"),
        );
        let key_hex = get_arg_value(&args, "--key-hex")
            .expect("--key-hex required for worker")
            .to_string();
        let seed = get_arg_value(&args, "--seed")
            .expect("--seed required for worker")
            .parse::<u64>()
            .expect("invalid --seed");
        let journal = PathBuf::from(
            get_arg_value(&args, "--journal").expect("--journal required for worker"),
        );
        Args {
            mode: Mode::Worker {
                db_path,
                key_hex,
                seed,
                journal,
            },
        }
    } else {
        // When invoked by `cargo test` with no flags, run a small smoke test
        Args {
            mode: Mode::Harness {
                iterations: 5,
                seed: 12345,
            },
        }
    }
}

fn get_arg_value<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
}

// ── Entry point ──

fn main() {
    let args = parse_args();

    match args.mode {
        Mode::Harness { iterations, seed } => {
            run_harness(iterations, seed);
        }
        Mode::Worker {
            db_path,
            key_hex,
            seed,
            journal,
        } => {
            let master_key = test_key_from_hex(&key_hex);
            run_worker(&db_path, &master_key, seed, &journal);
        }
    }
}
