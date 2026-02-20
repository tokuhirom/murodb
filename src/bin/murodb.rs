use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use murodb::crypto::kdf;
use murodb::sql::executor::ExecResult;
use murodb::storage::pager::Pager;
use murodb::types::Value;
use murodb::wal::recovery::{inspect_wal, RecoveryMode, RecoveryResult};
use murodb::Database;

const EXIT_OK: i32 = 0;
const EXIT_MALFORMED_DETECTED: i32 = 10;
const EXIT_FATAL_ERROR: i32 = 20;

#[derive(Clone, Debug, ValueEnum)]
enum RecoveryModeArg {
    Strict,
    Permissive,
}

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormatArg {
    Text,
    Json,
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

    /// Output format for inspection/reporting
    #[arg(long, value_enum, default_value = "text")]
    format: OutputFormatArg,
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

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn json_mode_str(mode: RecoveryMode) -> &'static str {
    match mode {
        RecoveryMode::Strict => "strict",
        RecoveryMode::Permissive => "permissive",
    }
}

fn inspect_success_exit_code(report: &RecoveryResult) -> i32 {
    if report.skipped.is_empty() {
        EXIT_OK
    } else {
        EXIT_MALFORMED_DETECTED
    }
}

#[derive(Clone, Copy, Debug)]
enum InspectFatalKind {
    MissingDbPath,
    ReadSalt,
    DeriveKey,
    InspectFailed,
}

impl InspectFatalKind {
    fn as_str(self) -> &'static str {
        match self {
            InspectFatalKind::MissingDbPath => "MISSING_DB_PATH",
            InspectFatalKind::ReadSalt => "READ_SALT_FAILED",
            InspectFatalKind::DeriveKey => "DERIVE_KEY_FAILED",
            InspectFatalKind::InspectFailed => "INSPECT_FAILED",
        }
    }
}

fn emit_inspect_json_success(mode: RecoveryMode, wal_path: &Path, report: &RecoveryResult) {
    println!("{}", build_inspect_json_success(mode, wal_path, report));
}

fn build_inspect_json_success(
    mode: RecoveryMode,
    wal_path: &Path,
    report: &RecoveryResult,
) -> String {
    let generated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let committed = report
        .committed_txids
        .iter()
        .map(|txid| txid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let aborted = report
        .aborted_txids
        .iter()
        .map(|txid| txid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let skipped = report
        .skipped
        .iter()
        .map(|s| {
            format!(
                "{{\"txid\":{},\"code\":\"{}\",\"reason\":\"{}\"}}",
                s.txid,
                s.code.as_str(),
                json_escape(&s.reason)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let quarantine = report
        .wal_quarantine_path
        .as_ref()
        .map(|p| format!("\"{}\"", json_escape(p)))
        .unwrap_or_else(|| "null".to_string());

    format!(
        "{{\"schema_version\":1,\"mode\":\"{}\",\"wal_path\":\"{}\",\"generated_at\":{},\"committed_txids\":[{}],\"aborted_txids\":[{}],\"pages_replayed\":{},\"skipped\":[{}],\"wal_quarantine_path\":{},\"status\":\"{}\",\"fatal_error\":null,\"fatal_error_code\":null,\"exit_code\":{}}}",
        json_mode_str(mode),
        json_escape(&wal_path.display().to_string()),
        generated_at,
        committed,
        aborted,
        report.pages_replayed,
        skipped,
        quarantine,
        if report.skipped.is_empty() {
            "ok"
        } else {
            "warning"
        },
        inspect_success_exit_code(report)
    )
}

fn emit_inspect_json_fatal(mode: RecoveryMode, wal_path: &Path, kind: InspectFatalKind, msg: &str) {
    println!("{}", build_inspect_json_fatal(mode, wal_path, kind, msg));
}

fn build_inspect_json_fatal(
    mode: RecoveryMode,
    wal_path: &Path,
    kind: InspectFatalKind,
    msg: &str,
) -> String {
    let generated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!(
        "{{\"schema_version\":1,\"mode\":\"{}\",\"wal_path\":\"{}\",\"generated_at\":{},\"committed_txids\":[],\"aborted_txids\":[],\"pages_replayed\":0,\"skipped\":[],\"wal_quarantine_path\":null,\"status\":\"fatal\",\"fatal_error\":\"{}\",\"fatal_error_code\":\"{}\",\"exit_code\":{}}}",
        json_mode_str(mode),
        json_escape(&wal_path.display().to_string()),
        generated_at,
        json_escape(msg),
        kind.as_str(),
        EXIT_FATAL_ERROR
    )
}

fn inspect_fatal_and_exit(
    format: &OutputFormatArg,
    mode: RecoveryMode,
    wal_path: &Path,
    kind: InspectFatalKind,
    msg: &str,
) -> ! {
    match format {
        OutputFormatArg::Text => eprintln!("ERROR: {}", msg),
        OutputFormatArg::Json => emit_inspect_json_fatal(mode, wal_path, kind, msg),
    }
    process::exit(EXIT_FATAL_ERROR);
}

#[cfg(test)]
mod tests {
    use super::*;
    use murodb::wal::recovery::{RecoverySkipCode, RecoverySkippedTx};

    #[test]
    fn inspect_json_success_has_null_fatal_error() {
        let wal_path = Path::new("/tmp/test.wal");
        let report = RecoveryResult {
            committed_txids: vec![1, 2],
            aborted_txids: vec![3],
            pages_replayed: 4,
            skipped: vec![RecoverySkippedTx {
                txid: 9,
                code: RecoverySkipCode::CommitWithoutMetaUpdate,
                reason: "missing meta".to_string(),
            }],
            wal_quarantine_path: Some("/tmp/test.wal.quarantine".to_string()),
        };

        let json = build_inspect_json_success(RecoveryMode::Permissive, wal_path, &report);
        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"mode\":\"permissive\""));
        assert!(json.contains("\"status\":\"warning\""));
        assert!(json.contains("\"fatal_error\":null"));
        assert!(json.contains("\"fatal_error_code\":null"));
        assert!(json.contains("\"code\":\"COMMIT_WITHOUT_META\""));
        assert!(json.contains("\"exit_code\":10"));
    }

    #[test]
    fn inspect_json_fatal_includes_error_message() {
        let wal_path = Path::new("/tmp/test.wal");
        let json = build_inspect_json_fatal(
            RecoveryMode::Strict,
            wal_path,
            InspectFatalKind::InspectFailed,
            "boom",
        );
        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"mode\":\"strict\""));
        assert!(json.contains("\"status\":\"fatal\""));
        assert!(json.contains("\"fatal_error\":\"boom\""));
        assert!(json.contains("\"fatal_error_code\":\"INSPECT_FAILED\""));
        assert!(json.contains("\"exit_code\":20"));
        assert!(json.contains("\"committed_txids\":[]"));
        assert!(json.contains("\"skipped\":[]"));
    }

    #[test]
    fn inspect_success_exit_code_is_zero_when_no_skipped() {
        let report = RecoveryResult {
            committed_txids: vec![1],
            aborted_txids: vec![],
            pages_replayed: 1,
            skipped: vec![],
            wal_quarantine_path: None,
        };
        assert_eq!(inspect_success_exit_code(&report), 0);
    }

    #[test]
    fn inspect_fatal_kind_codes_are_stable() {
        assert_eq!(InspectFatalKind::MissingDbPath.as_str(), "MISSING_DB_PATH");
        assert_eq!(InspectFatalKind::ReadSalt.as_str(), "READ_SALT_FAILED");
        assert_eq!(InspectFatalKind::DeriveKey.as_str(), "DERIVE_KEY_FAILED");
        assert_eq!(InspectFatalKind::InspectFailed.as_str(), "INSPECT_FAILED");
    }
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
            inspect_fatal_and_exit(
                &cli.format,
                recovery_mode,
                wal_path,
                InspectFatalKind::MissingDbPath,
                "db_path is required with --inspect-wal",
            );
        });
        let salt = Pager::read_salt_from_file(db_path).unwrap_or_else(|e| {
            inspect_fatal_and_exit(
                &cli.format,
                recovery_mode,
                wal_path,
                InspectFatalKind::ReadSalt,
                &format!("Failed to read DB salt: {}", e),
            );
        });
        let key = kdf::derive_key(password.as_bytes(), &salt).unwrap_or_else(|e| {
            inspect_fatal_and_exit(
                &cli.format,
                recovery_mode,
                wal_path,
                InspectFatalKind::DeriveKey,
                &format!("Failed to derive key: {}", e),
            );
        });
        let report = inspect_wal(wal_path, &key, recovery_mode).unwrap_or_else(|e| {
            inspect_fatal_and_exit(
                &cli.format,
                recovery_mode,
                wal_path,
                InspectFatalKind::InspectFailed,
                &format!("WAL inspection failed: {}", e),
            );
        });

        match cli.format {
            OutputFormatArg::Text => {
                println!("WAL inspection summary:");
                println!("  committed txs: {}", report.committed_txids.len());
                println!("  aborted txs: {}", report.aborted_txids.len());
                println!("  replayable pages: {}", report.pages_replayed);
                println!("  skipped malformed txs: {}", report.skipped.len());
                for skipped in &report.skipped {
                    println!(
                        "  - txid {} [{}]: {}",
                        skipped.txid,
                        skipped.code.as_str(),
                        skipped.reason
                    );
                }
            }
            OutputFormatArg::Json => {
                emit_inspect_json_success(recovery_mode, wal_path, &report);
            }
        }
        process::exit(inspect_success_exit_code(&report));
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
