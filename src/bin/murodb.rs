use std::path::PathBuf;
use std::process;

use clap::{Parser, ValueEnum};
use murodb::crypto::kdf;
use murodb::sql::executor::ExecResult;
use murodb::storage::pager::Pager;
use murodb::types::Value;
use murodb::wal::recovery::{inspect_wal, RecoveryMode};
use murodb::Database;

#[derive(Clone, Debug, ValueEnum)]
enum RecoveryModeArg {
    Strict,
    Permissive,
}

impl From<RecoveryModeArg> for RecoveryMode {
    fn from(value: RecoveryModeArg) -> Self {
        match value {
            RecoveryModeArg::Strict => RecoveryMode::Strict,
            RecoveryModeArg::Permissive => RecoveryMode::Permissive,
        }
    }
}

#[derive(Parser)]
#[command(name = "murodb", about = "MuroDB - Encrypted embedded SQL database")]
struct Cli {
    /// Path to the database file
    db_path: Option<PathBuf>,

    /// Execute SQL and exit
    #[arg(short = 'e')]
    execute: Option<String>,

    /// Create a new database
    #[arg(long)]
    create: bool,

    /// Password (if omitted, will prompt)
    #[arg(long)]
    password: Option<String>,

    /// WAL recovery behavior when opening existing DB
    #[arg(long, value_enum, default_value = "strict")]
    recovery_mode: RecoveryModeArg,

    /// Inspect WAL file consistency and exit (no DB replay)
    #[arg(long)]
    inspect_wal: Option<PathBuf>,
}

fn get_password(cli_password: &Option<String>) -> String {
    if let Some(pw) = cli_password {
        return pw.clone();
    }
    rpassword::read_password_from_tty(Some("Password: ")).unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to read password: {}", e);
        process::exit(1);
    })
}

fn format_rows(result: &ExecResult) -> String {
    match result {
        ExecResult::Rows(rows) => {
            if rows.is_empty() {
                return "Empty set".to_string();
            }

            // Get column names from first row
            let columns: Vec<&str> = rows[0]
                .values
                .iter()
                .map(|(name, _)| name.as_str())
                .collect();

            // Calculate column widths
            let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
            for row in rows {
                for (i, (_, val)) in row.values.iter().enumerate() {
                    let val_str = format_value(val);
                    if val_str.len() > widths[i] {
                        widths[i] = val_str.len();
                    }
                }
            }

            let mut out = String::new();

            // Header
            let separator: String = widths
                .iter()
                .map(|w| format!("+{}", "-".repeat(w + 2)))
                .collect::<String>()
                + "+";
            out.push_str(&separator);
            out.push('\n');

            let header: String = columns
                .iter()
                .zip(widths.iter())
                .map(|(name, w)| format!("| {:<width$} ", name, width = w))
                .collect::<String>()
                + "|";
            out.push_str(&header);
            out.push('\n');
            out.push_str(&separator);
            out.push('\n');

            // Rows
            for row in rows {
                let line: String = row
                    .values
                    .iter()
                    .zip(widths.iter())
                    .map(|((_, val), w)| format!("| {:<width$} ", format_value(val), width = w))
                    .collect::<String>()
                    + "|";
                out.push_str(&line);
                out.push('\n');
            }

            out.push_str(&separator);
            out.push('\n');
            out.push_str(&format!("{} row(s) in set", rows.len()));
            out
        }
        ExecResult::RowsAffected(n) => format!("Query OK, {} row(s) affected", n),
        ExecResult::Ok => "Query OK".to_string(),
    }
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Integer(n) => n.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::Varbinary(b) => format!("0x{}", hex_encode(b)),
        Value::Null => "NULL".to_string(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn execute_sql(db: &mut Database, sql: &str) {
    match db.execute(sql) {
        Ok(result) => println!("{}", format_rows(&result)),
        Err(e) => eprintln!("ERROR: {}", e),
    }
}

fn run_repl(db: &mut Database) {
    let mut rl = rustyline::DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to initialize REPL: {}", e);
        process::exit(1);
    });

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() {
            "murodb> "
        } else {
            "     -> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle exit commands
                if buffer.is_empty() && (trimmed == "quit" || trimmed == "exit") {
                    break;
                }

                if !buffer.is_empty() {
                    buffer.push(' ');
                }
                buffer.push_str(trimmed);

                // Check if statement is complete (ends with ;)
                if buffer.trim_end().ends_with(';') {
                    let sql = buffer.trim().to_string();
                    let _ = rl.add_history_entry(&sql);
                    execute_sql(db, &sql);
                    buffer.clear();
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                // Ctrl-C: clear current buffer
                buffer.clear();
                println!();
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                // Ctrl-D: exit
                break;
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
                break;
            }
        }
    }

    if let Err(e) = db.flush() {
        eprintln!("ERROR: Failed to flush database: {}", e);
    }
}

fn main() {
    let cli = Cli::parse();

    let password = get_password(&cli.password);
    let recovery_mode: RecoveryMode = cli.recovery_mode.clone().into();

    if let Some(wal_path) = &cli.inspect_wal {
        let db_path = cli.db_path.as_ref().unwrap_or_else(|| {
            eprintln!("ERROR: db_path is required with --inspect-wal");
            process::exit(1);
        });
        let salt = Pager::read_salt_from_file(db_path).unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to read DB salt: {}", e);
            process::exit(1);
        });
        let key = kdf::derive_key(password.as_bytes(), &salt).unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to derive key: {}", e);
            process::exit(1);
        });
        let report = inspect_wal(wal_path, &key, recovery_mode).unwrap_or_else(|e| {
            eprintln!("ERROR: WAL inspection failed: {}", e);
            process::exit(1);
        });

        println!("WAL inspection summary:");
        println!("  committed txs: {}", report.committed_txids.len());
        println!("  aborted txs: {}", report.aborted_txids.len());
        println!("  replayable pages: {}", report.pages_replayed);
        println!("  skipped malformed txs: {}", report.skipped.len());
        for skipped in &report.skipped {
            println!("  - txid {}: {}", skipped.txid, skipped.reason);
        }
        return;
    }

    let mut db = if cli.create {
        let db_path = cli.db_path.as_ref().unwrap_or_else(|| {
            eprintln!("ERROR: db_path is required unless --inspect-wal is used");
            process::exit(1);
        });
        if db_path.exists() {
            eprintln!("ERROR: File already exists: {}", db_path.display());
            process::exit(1);
        }
        Database::create_with_password(db_path, &password).unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to create database: {}", e);
            process::exit(1);
        })
    } else {
        let db_path = cli.db_path.as_ref().unwrap_or_else(|| {
            eprintln!("ERROR: db_path is required unless --inspect-wal is used");
            process::exit(1);
        });
        if !db_path.exists() {
            eprintln!("ERROR: Database file not found: {}", db_path.display());
            eprintln!("Use --create to create a new database");
            process::exit(1);
        }
        let (db, report) = Database::open_with_password_and_recovery_mode_and_report(
            db_path,
            &password,
            recovery_mode,
        )
        .unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to open database: {}", e);
            process::exit(1);
        });
        if recovery_mode == RecoveryMode::Permissive {
            if let Some(report) = &report {
                if !report.skipped.is_empty() {
                    eprintln!(
                        "WARNING: permissive recovery skipped {} malformed transaction(s)",
                        report.skipped.len()
                    );
                    if let Some(path) = &report.wal_quarantine_path {
                        eprintln!("  - quarantined WAL: {}", path);
                    }
                    for skipped in &report.skipped {
                        eprintln!("  - txid {}: {}", skipped.txid, skipped.reason);
                    }
                }
            }
        }
        db
    };

    if let Some(sql) = &cli.execute {
        execute_sql(&mut db, sql);
        if let Err(e) = db.flush() {
            eprintln!("ERROR: Failed to flush database: {}", e);
            process::exit(1);
        }
    } else {
        run_repl(&mut db);
    }
}
