# Formal Verification

MuroDB uses TLA+ to formally verify crash/recovery protocol invariants.

## Files

| File | Description |
|---|---|
| `specs/tla/CrashResilience.tla` | System model |
| `specs/tla/CrashResilience.cfg` | TLC configuration (small finite state space) |
| `specs/tla/CrashResilience.large.cfg` | Larger state space for deeper checking |
| `specs/tla/run_tlc.sh` | Helper script to run TLC |

## What is modeled

- **Transaction lifecycle**: `BeginTx`, `WritePage`, `SetMeta`, `DurableCommit`
- **Partial flush before crash**: `FlushSomeCommitted`
- **Crash and recovery**: `Crash`, `Recover`
- **WAL replay semantics** at transaction granularity:
  - Committed writes are recovered
  - Uncommitted writes are ignored
  - Metadata (`catalogRoot`, `pageCount`) is recovered from committed records

## Checked invariants

| Invariant | Description |
|---|---|
| `TypeInv` | Basic type safety of all state variables |
| `RecoveredSound` | After recovery, DB state equals replayed committed WAL state |
| `NoUncommittedInfluence` | Uncommitted transactions do not influence recovered state |
| `CommitRequiresMeta` | Committed transactions always have metadata update |
| `UniqueCommittedOrder` | Each transaction appears at most once in commit order |
| `FreelistPreserved` | After recovery, freelist ID equals the last committed freelist ID |

## Running TLC

### Prerequisites

- Java runtime
- `tla2tools.jar` (TLC model checker)

### Using Make

```bash
make tlc-tools   # Download tla2tools.jar
make tlc         # Run with small config (fast)
make tlc-large   # Run with large config (deeper)
```

### Manual

```bash
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
./specs/tla/run_tlc.sh
```

Or with the `tlc2` command:

```bash
tlc2 -config specs/tla/CrashResilience.cfg specs/tla/CrashResilience.tla
```

## Scope and Limitations

- This is an abstract model, not byte-level WAL frame parsing
- It does not model OS/filesystem durability anomalies directly
- It validates protocol-level invariants and crash/recovery semantics

## Correspondence with Implementation

See the [WAL & Crash Resilience](wal.md#tla-correspondence) page for a detailed mapping between TLA+ invariants and their implementation in code.
