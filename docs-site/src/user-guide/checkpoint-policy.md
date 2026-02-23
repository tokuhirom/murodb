# Checkpoint Policy Tuning

MuroDB can defer WAL truncate (checkpoint) using policy thresholds.
This helps reduce commit-time overhead on write-heavy workloads.

## Configuration Knobs

Checkpoint policy can be configured in two ways:

- Environment variables (process default, applied at session construction)
- SQL runtime options (`SET ...`, session scope, non-persistent)

Environment variable names:

- `MURODB_CHECKPOINT_TX_THRESHOLD`
- `MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD`
- `MURODB_CHECKPOINT_INTERVAL_MS`

SQL runtime option names:

- `checkpoint_tx_threshold`
- `checkpoint_wal_bytes_threshold`
- `checkpoint_interval_ms`

Semantics:

- Checkpoint runs when **any** enabled trigger fires.
- `MURODB_CHECKPOINT_TX_THRESHOLD=1` (default): checkpoint every commit/rollback.
- `MURODB_CHECKPOINT_TX_THRESHOLD=0`: disable tx-count trigger.
- `*_WAL_BYTES_THRESHOLD=0` / `*_INTERVAL_MS=0`: disabled.
- Runtime `SET` values follow the same semantics, but are session-only and not persisted.

Runtime example:

```sql
SET checkpoint_tx_threshold = 8;
SET checkpoint_wal_bytes_threshold = 1048576;
SET checkpoint_interval_ms = 1000;
```

Rust API example:

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

## Recommended Starting Profiles

### Low-latency / conservative

```bash
MURODB_CHECKPOINT_TX_THRESHOLD=8
MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD=1048576
MURODB_CHECKPOINT_INTERVAL_MS=1000
```

Use this when you want modest commit-speed gains with tight WAL growth control.

### Write-throughput focused

```bash
MURODB_CHECKPOINT_TX_THRESHOLD=64
MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD=8388608
MURODB_CHECKPOINT_INTERVAL_MS=5000
```

Use this for update/insert-heavy workloads where throughput is prioritized over immediate WAL truncation.

### Time-driven only

```bash
MURODB_CHECKPOINT_TX_THRESHOLD=0
MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD=0
MURODB_CHECKPOINT_INTERVAL_MS=1000
```

Useful when transaction size/shape is bursty and you want predictable checkpoint cadence.

## Tuning Procedure

1. Start from the conservative profile.
2. Run workload benchmark and record throughput/latency.
3. Increase `MURODB_CHECKPOINT_TX_THRESHOLD` stepwise (for example: `8 -> 16 -> 32 -> 64`).
4. Keep safety bounds with either `MURODB_CHECKPOINT_WAL_BYTES_THRESHOLD` or `MURODB_CHECKPOINT_INTERVAL_MS`.
5. Stop increasing when throughput gain flattens or WAL/recovery cost becomes unacceptable.

## What to Monitor

Use:

```sql
SHOW DATABASE STATS;
```

Track at least:

- `failed_checkpoints`
- `deferred_checkpoints`
- `checkpoint_pending_ops`
- `checkpoint_policy_tx_threshold`
- `checkpoint_policy_wal_bytes_threshold`
- `checkpoint_policy_interval_ms`

And from filesystem:

```bash
ls -lh mydb.wal
```

## Guardrails

- `failed_checkpoints > 0` means truncate is failing; investigate disk I/O.
- `checkpoint_pending_ops` growing continuously with WAL size means policy is too loose for current workload.
- Larger deferred windows can increase restart recovery time because WAL replay work increases.
- Durability boundary is unchanged: commit durability still depends on WAL `sync`.
