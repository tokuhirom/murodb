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

```

## WAL inspection

WAL inspection is handled by a dedicated command so the query CLI stays simple:

```bash
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive
```

See [WAL Inspection](wal-inspect.md) for exit codes and JSON schema.
