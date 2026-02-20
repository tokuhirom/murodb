# CLI Options

## Basic usage

```bash
murodb <database-file> [options]
```

## Options

| Option | Description |
|---|---|
| `-e <SQL>` | Execute SQL and exit |
| `--create` | Create a new database |
| `--password <PW>` | Password (prompts if omitted) |
| `--recovery-mode <strict\|permissive>` | WAL recovery policy for open |
| `--inspect-wal <PATH>` | Analyze WAL consistency and exit |
| `--format <text\|json>` | Output format (mainly for `--inspect-wal`) |

## Examples

```bash
# Create a new database
murodb mydb.db --create -e "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR)"

# Insert data
murodb mydb.db -e "INSERT INTO t (id, name) VALUES (1, 'hello')"

# Query
murodb mydb.db -e "SELECT * FROM t"

# Interactive REPL
murodb mydb.db

# Open with permissive recovery mode
murodb mydb.db --recovery-mode permissive

# Inspect WAL consistency
murodb mydb.db --inspect-wal mydb.wal --recovery-mode permissive

# Inspect as JSON
murodb mydb.db --inspect-wal mydb.wal --recovery-mode permissive --format json
```

## `--inspect-wal` exit codes

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
