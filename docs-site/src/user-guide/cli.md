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
| `--format <text\|json>` | Output format for query results |

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

# JSON output for machine processing
murodb mydb.db --format json -e "SELECT * FROM t"

```

## Query routing behavior

The CLI parses each statement and routes execution automatically:

- Read-only statements (`SELECT`, `UNION`, `EXPLAIN SELECT`, `SHOW ...`, `DESCRIBE`) use the read path.
- `EXPLAIN UPDATE` / `EXPLAIN DELETE` are supported, but route according to the inner statement category.
- Write and transaction-control statements (`INSERT`, `UPDATE`, `DELETE`, DDL, `BEGIN`/`COMMIT`/`ROLLBACK`) use the write path.
- While an explicit transaction is active (`BEGIN` ... `COMMIT`/`ROLLBACK`), all statements (including `SELECT`) run with execute semantics.

## JSON output

When using `--format json`, results are emitted as a single JSON object per statement.

### Result envelope

- `type` - One of `rows`, `rows_affected`, `ok`, or `error`
- `columns` - Column names in result order (only for `rows`)
- `rows` - Array of row arrays in column order (only for `rows`)
- `row_count` - Number of rows (only for `rows`)
- `rows_affected` - Number of rows affected (only for `rows_affected`)
- `message` - Error message string (only for `error`)

Example:

```json
{"type":"rows","columns":["id","name"],"rows":[[1,"alice"]],"row_count":1}
```

### Types

#### INTEGER

Numbers are emitted as JSON numbers.

#### FLOAT

Finite values are emitted as JSON numbers. Non-finite values are emitted as JSON strings.

#### DATE

`YYYY-MM-DD`

#### DATETIME

ISO 8601 `YYYY-MM-DDTHH:MM:SS`

#### TIMESTAMP

ISO 8601 `YYYY-MM-DDTHH:MM:SS`

#### VARCHAR

JSON strings with standard escaping.

#### VARBINARY

Base64 string (standard alphabet with `=` padding), for example `q80=`.

#### NULL

`null`

## WAL inspection

WAL inspection is handled by a dedicated command so the query CLI stays simple:

```bash
murodb-wal-inspect mydb.db --wal mydb.wal --recovery-mode permissive
```

See [WAL Inspection](wal-inspect.md) for exit codes and JSON schema.
