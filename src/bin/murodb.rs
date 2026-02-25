use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Mutex};

use base64::Engine;
use clap::{Parser, ValueEnum};
use murodb::{
    Database, DatabaseEncryption, ExecResult, MuroError, QueryCancelHandle, RecoveryMode,
    SqlStatementClass, Value,
};

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

#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
enum EncryptionModeArg {
    Aes256GcmSiv,
    Off,
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
#[command(
    name = "murodb",
    about = "MuroDB - Encrypted embedded SQL database",
    long_about = "MuroDB command-line interface for creating, opening, and querying an encrypted embedded SQL database.\n\nPrimary modes:\n- REPL mode: run interactive SQL when no `-e` is provided.\n- One-shot mode: run a single SQL statement with `-e` and exit.\n- Create mode: initialize a new database file with `--create`.\n\nOutput behavior:\n- `--format text`: human-readable table output.\n- `--format json`: stable JSON envelope for programmatic parsing.\n\nEncryption behavior:\n- `--encryption aes256-gcm-siv` (default): requires password.\n- `--encryption off`: plaintext database file, no password needed.\n\nIf `--password` is omitted in encrypted mode, the CLI prompts on TTY.",
    after_long_help = "Examples:\n  murodb my.db\n  murodb my.db -e \"SELECT 1\"\n  murodb my.db --format json -e \"SELECT id, name FROM users\"\n  murodb my.db --create --encryption aes256-gcm-siv\n  murodb my.db --recovery-mode permissive\n\nDocumentation:\n  https://tokuhirom.github.io/murodb/"
)]
struct Cli {
    /// Path to the database file.
    ///
    /// If omitted, defaults to `muro.db` in the current working directory.
    db_path: Option<PathBuf>,

    /// Execute one SQL statement and exit (non-interactive mode).
    ///
    /// Example: `-e "SELECT * FROM t LIMIT 10"`.
    #[arg(short = 'e')]
    execute: Option<String>,

    /// Create a new database file before opening.
    ///
    /// Fails if the target path already exists.
    #[arg(long)]
    create: bool,

    /// Password used for key derivation in encrypted mode.
    ///
    /// If omitted and encryption is enabled, the CLI prompts on TTY.
    #[arg(long)]
    password: Option<String>,

    /// Encryption mode used for create/open.
    ///
    /// Must match the existing database encryption mode when opening.
    #[arg(long, value_enum, default_value = "aes256-gcm-siv")]
    encryption: EncryptionModeArg,

    /// WAL recovery behavior used when opening an existing database.
    ///
    /// `strict` aborts on malformed WAL records.
    /// `permissive` skips malformed records where recovery can continue.
    #[arg(long, value_enum, default_value = "strict")]
    recovery_mode: RecoveryModeArg,

    /// Output format for query results and errors.
    ///
    /// `text` is human-readable; `json` is intended for tool integration.
    #[arg(long, value_enum, default_value = "text")]
    format: OutputFormatArg,

    /// Lock wait timeout in milliseconds.
    ///
    /// `0` means wait indefinitely.
    #[arg(long, default_value_t = 0)]
    busy_timeout_ms: u64,

    /// Per-statement execution timeout in milliseconds.
    ///
    /// `0` means no timeout.
    #[arg(long, default_value_t = 0)]
    statement_timeout_ms: u64,
}

fn get_password(cli_password: &Option<String>) -> String {
    if let Some(pw) = cli_password {
        eprintln!(
            "WARNING: Passing passwords via --password can expose secrets in shell history and process lists. Prefer interactive prompt when possible."
        );
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

fn format_value_json(val: &Value) -> String {
    match val {
        Value::Integer(n) => n.to_string(),
        Value::Float(n) => {
            if n.is_finite() {
                n.to_string()
            } else {
                format!("\"{}\"", json_escape(&n.to_string()))
            }
        }
        Value::Date(n) => format!("\"{}\"", json_escape(&murodb::format_date(*n))),
        Value::DateTime(n) => format!("\"{}\"", json_escape(&format_datetime_iso8601(*n))),
        Value::Timestamp(n) => format!("\"{}\"", json_escape(&format_datetime_iso8601(*n))),
        Value::Varchar(s) => format!("\"{}\"", json_escape(s)),
        Value::Varbinary(b) => format!("\"{}\"", base64_encode(b)),
        Value::Decimal(d) => d.to_string(),
        Value::Uuid(b) => format!("\"{}\"", murodb::format_uuid(b)),
        Value::Null => "null".to_string(),
    }
}

fn format_datetime_iso8601(packed: i64) -> String {
    let s = murodb::format_datetime(packed);
    let mut out = String::with_capacity(s.len() + 1);
    if let Some((date, time)) = s.split_once(' ') {
        out.push_str(date);
        out.push('T');
        out.push_str(time);
    } else {
        out.push_str(&s);
    }
    out
}

fn format_rows_json(result: &ExecResult) -> String {
    match result {
        ExecResult::Rows(rows) => {
            let columns: Vec<&str> = rows
                .first()
                .map(|row| row.values.iter().map(|(name, _)| name.as_str()).collect())
                .unwrap_or_else(Vec::new);
            let columns_json = columns
                .iter()
                .map(|name| format!("\"{}\"", json_escape(name)))
                .collect::<Vec<_>>()
                .join(",");
            let rows_json = rows
                .iter()
                .map(|row| {
                    let values = row
                        .values
                        .iter()
                        .map(|(_, val)| format_value_json(val))
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("[{}]", values)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{{\"type\":\"rows\",\"columns\":[{}],\"rows\":[{}],\"row_count\":{}}}",
                columns_json,
                rows_json,
                rows.len()
            )
        }
        ExecResult::RowsAffected(n) => {
            format!("{{\"type\":\"rows_affected\",\"rows_affected\":{}}}", n)
        }
        ExecResult::Ok => "{\"type\":\"ok\"}".to_string(),
    }
}

fn format_error_json(msg: &str) -> String {
    format!(
        "{{\"type\":\"error\",\"message\":\"{}\"}}",
        json_escape(msg)
    )
}

fn format_value(val: &Value) -> String {
    match val {
        Value::Integer(n) => n.to_string(),
        Value::Float(n) => n.to_string(),
        Value::Date(n) => murodb::format_date(*n),
        Value::DateTime(n) => murodb::format_datetime(*n),
        Value::Timestamp(n) => murodb::format_datetime(*n),
        Value::Varchar(s) => s.clone(),
        Value::Varbinary(b) => format!("0x{}", hex_encode(b)),
        Value::Decimal(d) => d.to_string(),
        Value::Uuid(b) => murodb::format_uuid(b),
        Value::Null => "NULL".to_string(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[derive(Clone, Default)]
struct InterruptController {
    active_handle: Arc<Mutex<Option<QueryCancelHandle>>>,
}

impl InterruptController {
    fn install_sigint_handler(&self) {
        let active_handle = Arc::clone(&self.active_handle);
        if let Err(e) = ctrlc::set_handler(move || {
            let handle = match active_handle.lock() {
                Ok(guard) => guard.as_ref().cloned(),
                Err(_) => None,
            };
            if let Some(handle) = handle {
                let _ = handle.cancel();
            }
        }) {
            eprintln!("WARNING: failed to install Ctrl-C handler: {}", e);
        }
    }

    fn begin_statement(&self, handle: QueryCancelHandle) {
        if let Ok(mut slot) = self.active_handle.lock() {
            *slot = Some(handle);
        }
    }

    fn end_statement(&self) {
        if let Ok(mut slot) = self.active_handle.lock() {
            *slot = None;
        }
    }
}

fn execute_sql(
    db: &mut Database,
    sql: &str,
    format: &OutputFormatArg,
    in_explicit_tx: &mut bool,
    interrupts: &InterruptController,
) {
    let class = match Database::classify_sql(sql) {
        Ok(class) => class,
        Err(MuroError::Parse(e)) => {
            match format {
                OutputFormatArg::Text => eprintln!("ERROR: SQL parse error: {}", e),
                OutputFormatArg::Json => {
                    println!("{}", format_error_json(&format!("SQL parse error: {}", e)))
                }
            }
            return;
        }
        Err(e) => {
            match format {
                OutputFormatArg::Text => eprintln!("ERROR: {}", e),
                OutputFormatArg::Json => println!("{}", format_error_json(&e.to_string())),
            }
            return;
        }
    };

    interrupts.begin_statement(db.cancel_handle());
    let result = match class {
        SqlStatementClass::ReadOnly if !*in_explicit_tx => db.query(sql).map(ExecResult::Rows),
        SqlStatementClass::Begin
        | SqlStatementClass::Commit
        | SqlStatementClass::Rollback
        | SqlStatementClass::ReadOnly
        | SqlStatementClass::Write => db.execute(sql),
    };
    interrupts.end_statement();

    match result {
        Ok(result) => match format {
            OutputFormatArg::Text => {
                if matches!(class, SqlStatementClass::Begin) {
                    *in_explicit_tx = true;
                } else if matches!(
                    class,
                    SqlStatementClass::Commit | SqlStatementClass::Rollback
                ) {
                    *in_explicit_tx = false;
                }
                println!("{}", format_rows(&result));
            }
            OutputFormatArg::Json => {
                if matches!(class, SqlStatementClass::Begin) {
                    *in_explicit_tx = true;
                } else if matches!(
                    class,
                    SqlStatementClass::Commit | SqlStatementClass::Rollback
                ) {
                    *in_explicit_tx = false;
                }
                println!("{}", format_rows_json(&result));
            }
        },
        Err(e) => match format {
            OutputFormatArg::Text => eprintln!("ERROR: {}", e),
            OutputFormatArg::Json => println!("{}", format_error_json(&e.to_string())),
        },
    }
}

fn run_repl(db: &mut Database, format: &OutputFormatArg, interrupts: &InterruptController) {
    let mut rl = rustyline::DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to initialize REPL: {}", e);
        process::exit(1);
    });

    let mut buffer = String::new();
    let mut in_explicit_tx = false;
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
                    execute_sql(db, &sql, format, &mut in_explicit_tx, interrupts);
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

    let recovery_mode: RecoveryMode = cli.recovery_mode.clone().into();

    let mut db = if cli.create {
        let db_path = cli.db_path.as_ref().unwrap_or_else(|| {
            eprintln!("ERROR: db_path is required");
            process::exit(1);
        });
        if db_path.exists() {
            eprintln!("ERROR: File already exists: {}", db_path.display());
            process::exit(1);
        }
        match cli.encryption {
            EncryptionModeArg::Aes256GcmSiv => {
                let password = get_password(&cli.password);
                Database::create_with_password(db_path, &password).unwrap_or_else(|e| {
                    eprintln!("ERROR: Failed to create database: {}", e);
                    process::exit(1);
                })
            }
            EncryptionModeArg::Off => Database::create_plaintext(db_path).unwrap_or_else(|e| {
                eprintln!("ERROR: Failed to create plaintext database: {}", e);
                process::exit(1);
            }),
        }
    } else {
        let db_path = cli.db_path.as_ref().unwrap_or_else(|| {
            eprintln!("ERROR: db_path is required");
            process::exit(1);
        });
        if !db_path.exists() {
            eprintln!("ERROR: Database file not found: {}", db_path.display());
            eprintln!("Use --create to create a new database");
            process::exit(1);
        }
        let detected_mode = Database::read_encryption_mode(db_path).unwrap_or_else(|e| {
            eprintln!("ERROR: Failed to read database header: {}", e);
            process::exit(1);
        });
        let requested_mode = match cli.encryption {
            EncryptionModeArg::Aes256GcmSiv => DatabaseEncryption::Encrypted,
            EncryptionModeArg::Off => DatabaseEncryption::Plaintext,
        };
        if requested_mode != detected_mode {
            eprintln!(
                "ERROR: Encryption mode mismatch. DB is {}, but --encryption={} was requested.",
                match detected_mode {
                    DatabaseEncryption::Encrypted => "aes256-gcm-siv",
                    DatabaseEncryption::Plaintext => "off",
                },
                match cli.encryption {
                    EncryptionModeArg::Aes256GcmSiv => "aes256-gcm-siv",
                    EncryptionModeArg::Off => "off",
                }
            );
            process::exit(1);
        }
        let (db, report) = match detected_mode {
            DatabaseEncryption::Encrypted => {
                let password = get_password(&cli.password);
                Database::open_with_password_and_recovery_mode_and_report(
                    db_path,
                    &password,
                    recovery_mode,
                )
            }
            DatabaseEncryption::Plaintext => {
                Database::open_plaintext_with_recovery_mode_and_report(db_path, recovery_mode)
            }
        }
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

    db.set_busy_timeout_ms(cli.busy_timeout_ms);
    db.set_statement_timeout_ms(cli.statement_timeout_ms);

    let interrupts = InterruptController::default();
    interrupts.install_sigint_handler();

    if let Some(sql) = &cli.execute {
        let mut in_explicit_tx = false;
        execute_sql(&mut db, sql, &cli.format, &mut in_explicit_tx, &interrupts);
        if let Err(e) = db.flush() {
            eprintln!("ERROR: Failed to flush database: {}", e);
            process::exit(1);
        }
    } else {
        run_repl(&mut db, &cli.format, &interrupts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use murodb::Row;

    #[test]
    fn format_rows_json_empty() {
        let result = ExecResult::Rows(Vec::new());
        let json = format_rows_json(&result);
        assert_eq!(
            json,
            "{\"type\":\"rows\",\"columns\":[],\"rows\":[],\"row_count\":0}"
        );
    }

    #[test]
    fn format_rows_json_values() {
        let rows = vec![Row {
            values: vec![
                ("id".to_string(), Value::Integer(1)),
                ("name".to_string(), Value::Varchar("al\"ice".to_string())),
                ("blob".to_string(), Value::Varbinary(vec![0xab, 0xcd])),
                ("note".to_string(), Value::Null),
                ("created_at".to_string(), Value::DateTime(20240203112233)),
            ],
        }];
        let result = ExecResult::Rows(rows);
        let json = format_rows_json(&result);
        assert_eq!(
            json,
            "{\"type\":\"rows\",\"columns\":[\"id\",\"name\",\"blob\",\"note\",\"created_at\"],\"rows\":[[1,\"al\\\"ice\",\"q80=\",null,\"2024-02-03T11:22:33\"]],\"row_count\":1}"
        );
    }

    #[test]
    fn format_rows_json_rows_affected() {
        let result = ExecResult::RowsAffected(7);
        let json = format_rows_json(&result);
        assert_eq!(json, "{\"type\":\"rows_affected\",\"rows_affected\":7}");
    }

    #[test]
    fn format_rows_json_ok() {
        let result = ExecResult::Ok;
        let json = format_rows_json(&result);
        assert_eq!(json, "{\"type\":\"ok\"}");
    }

    #[test]
    fn format_error_json_escapes() {
        let json = format_error_json("bad \"sql\"\nline");
        assert_eq!(
            json,
            "{\"type\":\"error\",\"message\":\"bad \\\"sql\\\"\\nline\"}"
        );
    }

    #[test]
    fn format_value_plaintext_date_and_datetime() {
        assert_eq!(format_value(&Value::Date(20240203)), "2024-02-03");
        assert_eq!(
            format_value(&Value::DateTime(20240203112233)),
            "2024-02-03 11:22:33"
        );
    }
}
