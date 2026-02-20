# CrashResilience TLA+ Model

This folder contains a small TLA+ model for MuroDB crash resilience.

## Files

- `CrashResilience.tla`: system model
- `CrashResilience.cfg`: TLC configuration (small finite state space, default)
- `CrashResilience.large.cfg`: larger state space for deeper checking
- `run_tlc.sh`: helper to run TLC

## What is modeled

- Transaction lifecycle: `BeginTx`, `WritePage`, `SetMeta`, `DurableCommit`
- Optional partial data-file flush before crash (`FlushSomeCommitted`)
- Crash and restart recovery (`Crash`, `Recover`)
- WAL replay semantics at tx granularity:
  - committed writes are recovered
  - uncommitted writes are ignored
  - metadata (`catalogRoot`, `pageCount`) is recovered from committed records

## Checked invariants

- `TypeInv`: basic type safety of all state variables
- `RecoveredSound`: after recovery, DB state equals replayed committed WAL state
- `NoUncommittedInfluence`: uncommitted txs do not influence recovered state
- `CommitRequiresMeta`: committed tx always has metadata update
- `UniqueCommittedOrder`: each tx appears at most once in commit order

## Run TLC

Requirements:

1. Java (installed in this dev environment)
2. `tla2tools.jar` (TLC)

Example:

```bash
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
./specs/tla/run_tlc.sh
```

Or, from repo root:

```bash
make tlc-tools   # download tla2tools.jar via curl/wget
make tlc         # run TLC with specs/tla/CrashResilience.cfg (fast/small)
make tlc-large   # run TLC with specs/tla/CrashResilience.large.cfg
```

If your machine has `tlc2` command available, you can also run:

```bash
tlc2 -config specs/tla/CrashResilience.cfg specs/tla/CrashResilience.tla
```

## Scope and limitations

- This is an abstract model, not byte-level WAL frame parsing.
- It does not model OS/filesystem durability anomalies directly.
- It is intended to validate protocol-level invariants and crash/recovery semantics.
