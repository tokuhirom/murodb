use std::path::PathBuf;
use std::process;

use clap::{Parser, ValueEnum};
use murodb::wal::recovery::RecoveryMode;
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
#[command(
    name = "murodb-rekey",
    about = "Re-encrypt a MuroDB database with a new password",
    long_about = "murodb-rekey opens an encrypted database with the current password and rotates it to a new password.\n\nPasswords are read from TTY prompts and are not accepted via command-line options.",
    after_long_help = "Example:\n  murodb-rekey my.db\n"
)]
struct Cli {
    /// Path to the database file.
    db_path: PathBuf,

    /// WAL recovery behavior used while opening.
    #[arg(long, value_enum, default_value = "strict")]
    recovery_mode: RecoveryModeArg,
}

fn prompt_password(prompt: &str) -> String {
    rpassword::read_password_from_tty(Some(prompt)).unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to read password: {}", e);
        process::exit(1);
    })
}

fn main() {
    let cli = Cli::parse();
    if !cli.db_path.exists() {
        eprintln!("ERROR: Database file not found: {}", cli.db_path.display());
        process::exit(1);
    }

    let current_password = prompt_password("Current password: ");
    let new_password = prompt_password("New password: ");
    let confirm_password = prompt_password("Confirm new password: ");
    if new_password != confirm_password {
        eprintln!("ERROR: New password confirmation does not match");
        process::exit(1);
    }
    if new_password.is_empty() {
        eprintln!("ERROR: New password must not be empty");
        process::exit(1);
    }

    let recovery_mode: RecoveryMode = cli.recovery_mode.into();
    let mut db = Database::open_with_password_and_recovery_mode(
        &cli.db_path,
        &current_password,
        recovery_mode,
    )
    .unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to open database: {}", e);
        process::exit(1);
    });

    db.rekey_with_password(&new_password).unwrap_or_else(|e| {
        eprintln!("ERROR: Rekey failed: {}", e);
        process::exit(1);
    });

    println!("Rekey completed.");
}
