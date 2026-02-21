# WAL Inspection

WAL inspection is a separate command so the main `murodb` CLI stays focused on queries.

## Basic usage

```bash
murodb-wal-inspect <database-file> --wal <WAL-PATH> [options]
```

## Options

| Option | Description |
|---|---|
| `--wal <PATH>` | WAL file path or quarantine file path |
| `--password <PW>` | Password (prompts if omitted) |
| `--recovery-mode <strict\|permissive>` | Recovery policy used during inspection |
| `--format <text\|json>` | Output format for inspection results |

## Examples

```bash
# Text output
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive

# JSON output (for automation)
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive --format json

# Inspect a quarantine WAL file
murodb-wal-inspect mydb.db --wal mydb.wal.quarantine.20240101_120000
```

## Exit codes

| Exit Code | Meaning |
|---|---|
| `0` | No malformed transactions detected |
| `10` | Malformed transactions detected (inspection succeeded) |
| `20` | Fatal error (decrypt/IO/strict failure, etc.) |

## JSON output

When using `--format json`, the output includes stable fields:

- `schema_version` - Schema version for the JSON format
- `mode` - Recovery mode used
- `wal_path` - Path to the WAL file
- `generated_at` - Timestamp of the inspection
- `status` - `ok`, `warning`, or `fatal`
- `exit_code` - Exit code
- `skipped[].code` - Machine-readable classification of skipped transactions
- `fatal_error` / `fatal_error_code` - Present on fatal failures
