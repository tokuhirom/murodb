# Runtime Configuration

MuroDB supports session-scoped runtime options for operational tuning.
This page explains what each option does, when to use it, and how to observe it.

## Scope and Behavior

- Scope: session-only
- Persistence: not persisted in the database file
- Update timing: immediate for subsequent operations in the same session
- Transaction rule: runtime `SET` is rejected inside explicit transactions (`BEGIN ... COMMIT/ROLLBACK`)

You can set runtime options with SQL:

```sql
SET checkpoint_tx_threshold = 8;
SET checkpoint_wal_bytes_threshold = 1048576;
SET checkpoint_interval_ms = 1000;
```

Or with Rust API:

```rust
use murodb::{Database, sql::session::RuntimeConfig};

let mut db = Database::open_plaintext("mydb.db".as_ref())?;
db.set_runtime_config(RuntimeConfig {
    checkpoint_tx_threshold: 8,
    checkpoint_wal_bytes_threshold: 1_048_576,
    checkpoint_interval_ms: 1_000,
})?;
let active = db.runtime_config()?;
```

## Available Options

### checkpoint_tx_threshold

- SQL name: `checkpoint_tx_threshold`
- Env default source: `MURODB_CHECKPOINT_TX_THRESHOLD`
- Default value: `1`
- Type/range: `u64` (`0` or greater)

Meaning:
- Trigger checkpoint after this many post-commit/post-rollback operations.
- `1` means checkpoint every commit/rollback.
- `0` disables the tx-count trigger.

Use when:
- You want to trade lower commit overhead for larger WAL windows.

### checkpoint_wal_bytes_threshold

- SQL name: `checkpoint_wal_bytes_threshold`
- Env default source: `MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD`
- Default value: `0` (disabled)
- Type/range: `u64` (`0` or greater)

Meaning:
- Trigger checkpoint when WAL file size reaches this threshold in bytes.
- `0` disables the size trigger.

Use when:
- You want an upper guardrail on WAL growth.

### checkpoint_interval_ms

- SQL name: `checkpoint_interval_ms`
- Env default source: `MURODB_CHECKPOINT_INTERVAL_MS`
- Default value: `0` (disabled)
- Type/range: `u64` (`0` or greater)

Meaning:
- Trigger checkpoint when elapsed time since the last successful checkpoint reaches this threshold.
- `0` disables the time trigger.

Use when:
- Workload has bursts and you want a time-based checkpoint cadence.

## Trigger Combination Rules

Checkpoint runs when any enabled trigger fires:

- tx-count trigger (`checkpoint_tx_threshold`)
- WAL-size trigger (`checkpoint_wal_bytes_threshold`)
- interval trigger (`checkpoint_interval_ms`)

## Validation and Errors

- Runtime values must be non-negative integers.
- Unknown option names return a deterministic parse error.
- Using runtime `SET` inside explicit transactions returns an execution error.

## Observability

Use:

```sql
SHOW DATABASE STATS;
```

Relevant fields:

- `checkpoint_policy_tx_threshold`
- `checkpoint_policy_wal_bytes_threshold`
- `checkpoint_policy_interval_ms`
- `deferred_checkpoints`
- `checkpoint_pending_ops`
- `failed_checkpoints`
- `wal_file_size_bytes`

See also:
- [Checkpoint Policy Tuning](checkpoint-policy.md)
- [Alerting & Monitoring](alerting.md)
