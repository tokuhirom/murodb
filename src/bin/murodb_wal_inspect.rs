use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use murodb::crypto::kdf;
use murodb::crypto::suite::EncryptionSuite;
use murodb::storage::pager::Pager;
use murodb::wal::recovery::{inspect_wal, RecoveryMode, RecoveryResult};

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
#[command(name = "murodb-wal-inspect", about = "Inspect MuroDB WAL consistency")]
struct Cli {
    /// Path to the database file
    db_path: PathBuf,

    /// WAL file path (or quarantine file)
    #[arg(long, value_name = "PATH")]
    wal: PathBuf,

    /// Password (if omitted, will prompt)
    #[arg(long)]
    password: Option<String>,

    /// WAL recovery behavior used during inspection
    #[arg(long, value_enum, default_value = "strict")]
    recovery_mode: RecoveryModeArg,

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
    ReadSalt,
    DeriveKey,
    InspectFailed,
}

impl InspectFatalKind {
    fn as_str(self) -> &'static str {
        match self {
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
        if report.skipped.is_empty() { "ok" } else { "warning" },
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

fn main() {
    let cli = Cli::parse();

    let recovery_mode: RecoveryMode = cli.recovery_mode.clone().into();

    let info = Pager::read_encryption_info_from_file(&cli.db_path).unwrap_or_else(|e| {
        inspect_fatal_and_exit(
            &cli.format,
            recovery_mode,
            &cli.wal,
            InspectFatalKind::ReadSalt,
            &format!("Failed to read DB encryption info: {}", e),
        );
    });
    let report = match info.suite {
        EncryptionSuite::Aes256GcmSiv => {
            let password = get_password(&cli.password);
            let key = kdf::derive_key(password.as_bytes(), &info.salt).unwrap_or_else(|e| {
                inspect_fatal_and_exit(
                    &cli.format,
                    recovery_mode,
                    &cli.wal,
                    InspectFatalKind::DeriveKey,
                    &format!("Failed to derive key: {}", e),
                );
            });
            inspect_wal(&cli.wal, &key, recovery_mode)
        }
        EncryptionSuite::Plaintext => murodb::wal::recovery::inspect_wal_with_suite(
            &cli.wal,
            EncryptionSuite::Plaintext,
            None,
            recovery_mode,
        ),
    }
    .unwrap_or_else(|e| {
        inspect_fatal_and_exit(
            &cli.format,
            recovery_mode,
            &cli.wal,
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
            emit_inspect_json_success(recovery_mode, &cli.wal, &report);
        }
    }

    process::exit(inspect_success_exit_code(&report));
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
        assert_eq!(InspectFatalKind::ReadSalt.as_str(), "READ_SALT_FAILED");
        assert_eq!(InspectFatalKind::DeriveKey.as_str(), "DERIVE_KEY_FAILED");
        assert_eq!(InspectFatalKind::InspectFailed.as_str(), "INSPECT_FAILED");
    }

    #[test]
    fn recovery_skip_code_strings_are_stable() {
        assert_eq!(RecoverySkipCode::DuplicateBegin.as_str(), "DUPLICATE_BEGIN");
        assert_eq!(
            RecoverySkipCode::BeginAfterTerminal.as_str(),
            "BEGIN_AFTER_TERMINAL"
        );
        assert_eq!(
            RecoverySkipCode::PagePutBeforeBegin.as_str(),
            "PAGEPUT_BEFORE_BEGIN"
        );
        assert_eq!(
            RecoverySkipCode::PagePutAfterTerminal.as_str(),
            "PAGEPUT_AFTER_TERMINAL"
        );
        assert_eq!(
            RecoverySkipCode::MetaUpdateBeforeBegin.as_str(),
            "METAUPDATE_BEFORE_BEGIN"
        );
        assert_eq!(
            RecoverySkipCode::MetaUpdateAfterTerminal.as_str(),
            "METAUPDATE_AFTER_TERMINAL"
        );
        assert_eq!(
            RecoverySkipCode::CommitBeforeBegin.as_str(),
            "COMMIT_BEFORE_BEGIN"
        );
        assert_eq!(
            RecoverySkipCode::DuplicateTerminal.as_str(),
            "DUPLICATE_TERMINAL"
        );
        assert_eq!(
            RecoverySkipCode::CommitWithoutMetaUpdate.as_str(),
            "COMMIT_WITHOUT_META"
        );
        assert_eq!(
            RecoverySkipCode::CommitLsnMismatch.as_str(),
            "COMMIT_LSN_MISMATCH"
        );
        assert_eq!(
            RecoverySkipCode::AbortBeforeBegin.as_str(),
            "ABORT_BEFORE_BEGIN"
        );
    }

    #[test]
    fn inspect_json_fatal_all_variants_have_required_keys() {
        let wal_path = Path::new("/tmp/test.wal");
        let variants = [
            (InspectFatalKind::ReadSalt, "salt read failure"),
            (InspectFatalKind::DeriveKey, "key derivation failure"),
            (InspectFatalKind::InspectFailed, "inspection failure"),
        ];

        for (kind, msg) in &variants {
            let json = build_inspect_json_fatal(RecoveryMode::Strict, wal_path, *kind, msg);
            assert!(
                json.contains("\"schema_version\":1"),
                "missing schema_version for {:?}",
                kind
            );
            assert!(
                json.contains("\"status\":\"fatal\""),
                "missing status for {:?}",
                kind
            );
            assert!(
                json.contains("\"fatal_error\":"),
                "missing fatal_error for {:?}",
                kind
            );
            assert!(
                json.contains(&format!("\"fatal_error_code\":\"{}\"", kind.as_str())),
                "missing fatal_error_code for {:?}",
                kind
            );
            assert!(
                json.contains("\"exit_code\":20"),
                "missing exit_code for {:?}",
                kind
            );
        }
    }

    #[test]
    fn inspect_json_success_ok_status_when_no_skipped() {
        let wal_path = Path::new("/tmp/test.wal");
        let report = RecoveryResult {
            committed_txids: vec![1],
            aborted_txids: vec![],
            pages_replayed: 1,
            skipped: vec![],
            wal_quarantine_path: None,
        };

        let json = build_inspect_json_success(RecoveryMode::Strict, wal_path, &report);
        assert!(json.contains("\"status\":\"ok\""));
        assert!(json.contains("\"exit_code\":0"));
        assert!(json.contains("\"fatal_error\":null"));
        assert!(json.contains("\"fatal_error_code\":null"));
    }
}
