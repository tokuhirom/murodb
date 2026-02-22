use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{value_parser, Parser};
use murodb::crypto::aead::MasterKey;
use murodb::Database;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Parser, Debug)]
#[command(
    name = "murodb-bench",
    about = "Embedded DB benchmark for typical OLTP-style workloads"
)]
struct Cli {
    #[arg(long, default_value_t = 20_000, value_parser = value_parser!(u64).range(1..))]
    initial_rows: u64,

    #[arg(long, default_value_t = 20_000)]
    select_ops: u64,

    #[arg(long, default_value_t = 5_000)]
    update_ops: u64,

    #[arg(long, default_value_t = 5_000)]
    insert_ops: u64,

    #[arg(long, default_value_t = 2_000)]
    scan_ops: u64,

    #[arg(long, default_value_t = 10_000)]
    mixed_ops: u64,

    #[arg(long, default_value_t = 200)]
    warmup_ops: u64,

    #[arg(long, default_value_t = 500, value_parser = value_parser!(u64).range(1..))]
    batch_size: u64,

    #[arg(long)]
    keep_db: bool,
}

struct Stat {
    name: &'static str,
    ops: u64,
    elapsed: Duration,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

fn percentile_ms(samples_ns: &[u128], num: usize, den: usize) -> f64 {
    if samples_ns.is_empty() {
        return 0.0;
    }
    let mut sorted = samples_ns.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) * num) / den;
    sorted[idx] as f64 / 1_000_000.0
}

fn measure<F>(name: &'static str, ops: u64, mut op: F) -> Stat
where
    F: FnMut() -> usize,
{
    let mut latencies = Vec::with_capacity(ops as usize);
    let start = Instant::now();
    let mut blackhole: usize = 0;

    for _ in 0..ops {
        let t0 = Instant::now();
        blackhole ^= op();
        latencies.push(t0.elapsed().as_nanos());
    }

    std::hint::black_box(blackhole);
    let elapsed = start.elapsed();

    Stat {
        name,
        ops,
        elapsed,
        p50_ms: percentile_ms(&latencies, 50, 100),
        p95_ms: percentile_ms(&latencies, 95, 100),
        p99_ms: percentile_ms(&latencies, 99, 100),
    }
}

fn payload(id: u64, salt: u64) -> String {
    format!("p{:016x}_{:016x}", id, salt)
}

fn populate(db: &mut Database, initial_rows: u64, batch_size: u64) {
    let mut id = 1u64;
    while id <= initial_rows {
        let end = (id + batch_size - 1).min(initial_rows);
        db.execute("BEGIN").expect("BEGIN failed while populate");

        let mut values_sql = String::new();
        for current in id..=end {
            if !values_sql.is_empty() {
                values_sql.push(',');
            }
            let p = payload(current, 0);
            values_sql.push_str(&format!("({}, {}, '{}')", current, current, p));
        }

        let sql = format!("INSERT INTO kv VALUES {}", values_sql);
        db.execute(&sql)
            .expect("INSERT batch failed while populate");
        db.execute("COMMIT").expect("COMMIT failed while populate");

        id = end + 1;
    }
}

fn main() {
    let cli = Cli::parse();
    if cli.initial_rows == 0 {
        eprintln!("error: --initial-rows must be >= 1");
        std::process::exit(2);
    }
    if cli.batch_size == 0 {
        eprintln!("error: --batch-size must be >= 1");
        std::process::exit(2);
    }

    let seed = 0x5EED_u64;
    let mut rng = StdRng::seed_from_u64(seed);

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let db_path = PathBuf::from(format!(
        "/tmp/murodb_bench_{}_{}.db",
        std::process::id(),
        ts
    ));

    let master_key = MasterKey::new([0x42; 32]);
    let mut db = Database::create(&db_path, &master_key).expect("create db failed");
    db.execute("CREATE TABLE kv (id BIGINT PRIMARY KEY, v1 BIGINT, v2 VARCHAR)")
        .expect("create table failed");

    println!("== MuroDB Embedded Benchmark ==");
    println!("db_path={}", db_path.display());
    println!(
        "config: initial_rows={}, select_ops={}, update_ops={}, insert_ops={}, scan_ops={}, mixed_ops={}, warmup_ops={}, batch_size={}, rng_seed={}",
        cli.initial_rows,
        cli.select_ops,
        cli.update_ops,
        cli.insert_ops,
        cli.scan_ops,
        cli.mixed_ops,
        cli.warmup_ops,
        cli.batch_size,
        seed
    );

    let setup_start = Instant::now();
    populate(&mut db, cli.initial_rows, cli.batch_size);
    let setup_elapsed = setup_start.elapsed();
    println!(
        "setup_elapsed_ms={:.3}",
        setup_elapsed.as_secs_f64() * 1000.0
    );

    for _ in 0..cli.warmup_ops {
        let id = rng.gen_range(1..=cli.initial_rows);
        let sql = format!("SELECT * FROM kv WHERE id = {}", id);
        let rows = db.query(&sql).expect("warmup select failed");
        std::hint::black_box(rows.len());
    }

    let select_stat = measure("point_select_pk", cli.select_ops, || {
        let id = rng.gen_range(1..=cli.initial_rows);
        let sql = format!("SELECT * FROM kv WHERE id = {}", id);
        let rows = db.query(&sql).expect("point select failed");
        rows.len()
    });

    let update_stat = measure("point_update_pk", cli.update_ops, || {
        let id = rng.gen_range(1..=cli.initial_rows);
        let new_v1 = rng.gen_range(1..=10_000_000u64);
        let p = payload(id, new_v1);
        let sql = format!(
            "UPDATE kv SET v1 = {}, v2 = '{}' WHERE id = {}",
            new_v1, p, id
        );
        db.execute(&sql).expect("point update failed");
        1
    });

    let mut next_insert_id = cli.initial_rows + 1;
    let insert_stat = measure("insert_autocommit", cli.insert_ops, || {
        let id = next_insert_id;
        next_insert_id += 1;
        let p = payload(id, id ^ 0xABCD);
        let sql = format!("INSERT INTO kv VALUES ({}, {}, '{}')", id, id, p);
        db.execute(&sql).expect("insert failed");
        1
    });

    let total_rows_after_insert = next_insert_id - 1;
    let scan_stat = measure("range_scan_limit_100", cli.scan_ops, || {
        let max_start = total_rows_after_insert.saturating_sub(100).max(1);
        let start_id = rng.gen_range(1..=max_start);
        let sql = format!(
            "SELECT * FROM kv WHERE id >= {} ORDER BY id ASC LIMIT 100",
            start_id
        );
        let rows = db.query(&sql).expect("range scan failed");
        rows.len()
    });

    let mixed_start_rows = next_insert_id - 1;
    let mixed_stat = measure("mixed_80r_15u_5i", cli.mixed_ops, || {
        let dice = rng.gen_range(0..100u32);
        if dice < 80 {
            let id = rng.gen_range(1..=next_insert_id - 1);
            let sql = format!("SELECT * FROM kv WHERE id = {}", id);
            let rows = db.query(&sql).expect("mixed select failed");
            rows.len()
        } else if dice < 95 {
            let id = rng.gen_range(1..=next_insert_id - 1);
            let new_v1 = rng.gen_range(1..=10_000_000u64);
            let p = payload(id, new_v1 ^ 0x1111);
            let sql = format!(
                "UPDATE kv SET v1 = {}, v2 = '{}' WHERE id = {}",
                new_v1, p, id
            );
            db.execute(&sql).expect("mixed update failed");
            1
        } else {
            let id = next_insert_id;
            next_insert_id += 1;
            let p = payload(id, id ^ 0xBEEF);
            let sql = format!("INSERT INTO kv VALUES ({}, {}, '{}')", id, id, p);
            db.execute(&sql).expect("mixed insert failed");
            1
        }
    });

    println!();
    println!("name,ops,total_sec,ops_per_sec,p50_ms,p95_ms,p99_ms");
    for stat in [select_stat, update_stat, insert_stat, scan_stat, mixed_stat] {
        let total_sec = stat.elapsed.as_secs_f64();
        let ops_per_sec = if total_sec > 0.0 {
            stat.ops as f64 / total_sec
        } else {
            0.0
        };
        println!(
            "{},{},{:.6},{:.2},{:.4},{:.4},{:.4}",
            stat.name, stat.ops, total_sec, ops_per_sec, stat.p50_ms, stat.p95_ms, stat.p99_ms
        );
    }

    let final_rows = next_insert_id - 1;
    println!();
    println!(
        "rows: start={}, after_insert_phase={}, final={}",
        cli.initial_rows, mixed_start_rows, final_rows
    );

    if cli.keep_db {
        println!("kept_db_path={}", db_path.display());
    } else {
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("wal"));
    }
}
