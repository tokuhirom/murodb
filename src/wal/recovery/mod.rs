use std::collections::HashMap;
use std::path::Path;

use crate::crypto::aead::MasterKey;
use crate::crypto::suite::EncryptionSuite;
use crate::error::{MuroError, Result};
use crate::storage::page::{Page, PageId, PAGE_SIZE};
use crate::storage::pager::Pager;
use crate::wal::reader::WalReader;
use crate::wal::record::{TxId, WalRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    Strict,
    Permissive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySkipCode {
    DuplicateBegin,
    BeginAfterTerminal,
    PagePutBeforeBegin,
    PagePutAfterTerminal,
    MetaUpdateBeforeBegin,
    MetaUpdateAfterTerminal,
    CommitBeforeBegin,
    DuplicateTerminal,
    CommitWithoutMetaUpdate,
    CommitLsnMismatch,
    AbortBeforeBegin,
}

impl RecoverySkipCode {
    pub fn as_str(self) -> &'static str {
        match self {
            RecoverySkipCode::DuplicateBegin => "DUPLICATE_BEGIN",
            RecoverySkipCode::BeginAfterTerminal => "BEGIN_AFTER_TERMINAL",
            RecoverySkipCode::PagePutBeforeBegin => "PAGEPUT_BEFORE_BEGIN",
            RecoverySkipCode::PagePutAfterTerminal => "PAGEPUT_AFTER_TERMINAL",
            RecoverySkipCode::MetaUpdateBeforeBegin => "METAUPDATE_BEFORE_BEGIN",
            RecoverySkipCode::MetaUpdateAfterTerminal => "METAUPDATE_AFTER_TERMINAL",
            RecoverySkipCode::CommitBeforeBegin => "COMMIT_BEFORE_BEGIN",
            RecoverySkipCode::DuplicateTerminal => "DUPLICATE_TERMINAL",
            RecoverySkipCode::CommitWithoutMetaUpdate => "COMMIT_WITHOUT_META",
            RecoverySkipCode::CommitLsnMismatch => "COMMIT_LSN_MISMATCH",
            RecoverySkipCode::AbortBeforeBegin => "ABORT_BEFORE_BEGIN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxTerminalState {
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy)]
struct TxValidationState {
    seen_begin: bool,
    seen_meta_update: bool,
    terminal: Option<TxTerminalState>,
}

impl TxValidationState {
    fn new() -> Self {
        Self {
            seen_begin: false,
            seen_meta_update: false,
            terminal: None,
        }
    }
}

/// Recover the database from WAL.
/// Replays committed transactions, discards uncommitted ones.
/// Restores page data, catalog_root, and page_count metadata.
pub fn recover(db_path: &Path, wal_path: &Path, master_key: &MasterKey) -> Result<RecoveryResult> {
    recover_with_mode(db_path, wal_path, master_key, RecoveryMode::Strict)
}

/// Recover the database from WAL in a chosen mode.
///
/// - `Strict`: reject any WAL protocol inconsistency.
/// - `Permissive`: ignore malformed transactions and recover valid committed ones.
pub fn recover_with_mode(
    db_path: &Path,
    wal_path: &Path,
    master_key: &MasterKey,
    mode: RecoveryMode,
) -> Result<RecoveryResult> {
    recover_with_mode_and_suite(
        db_path,
        wal_path,
        EncryptionSuite::Aes256GcmSiv,
        Some(master_key),
        mode,
    )
}

pub fn recover_with_mode_and_suite(
    db_path: &Path,
    wal_path: &Path,
    suite: EncryptionSuite,
    master_key: Option<&MasterKey>,
    mode: RecoveryMode,
) -> Result<RecoveryResult> {
    recover_with_mode_internal(Some(db_path), wal_path, suite, master_key, mode, true)
}

/// Inspect WAL consistency without applying pages to a DB file.
///
/// This is useful for diagnostics and post-mortem analysis.
pub fn inspect_wal(
    wal_path: &Path,
    master_key: &MasterKey,
    mode: RecoveryMode,
) -> Result<RecoveryResult> {
    inspect_wal_with_suite(
        wal_path,
        EncryptionSuite::Aes256GcmSiv,
        Some(master_key),
        mode,
    )
}

pub fn inspect_wal_with_suite(
    wal_path: &Path,
    suite: EncryptionSuite,
    master_key: Option<&MasterKey>,
    mode: RecoveryMode,
) -> Result<RecoveryResult> {
    recover_with_mode_internal(None, wal_path, suite, master_key, mode, false)
}

fn recover_with_mode_internal(
    db_path: Option<&Path>,
    wal_path: &Path,
    suite: EncryptionSuite,
    master_key: Option<&MasterKey>,
    mode: RecoveryMode,
    apply_to_db: bool,
) -> Result<RecoveryResult> {
    if !wal_path.exists() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
            skipped: Vec::new(),
            wal_quarantine_path: None,
        });
    }

    let mut reader = WalReader::open_with_suite(wal_path, suite, master_key)?;
    let records = reader.read_all()?;

    if records.is_empty() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
            skipped: Vec::new(),
            wal_quarantine_path: None,
        });
    }

    // Phase 1: Validate WAL transaction lifecycle against TLA+ state machine.
    // Allowed transitions:
    //   Init -> Begin -> (PagePut | MetaUpdate)* -> (Commit | Abort)
    // No record is allowed after Commit/Abort for the same txid.
    let mut tx_states: HashMap<TxId, TxValidationState> = HashMap::new();
    let mut invalid_txs: HashMap<TxId, RecoverySkippedTx> = HashMap::new();

    let mut invalidate_or_err = |txid: TxId, code: RecoverySkipCode, msg: String| -> Result<()> {
        match mode {
            RecoveryMode::Strict => Err(MuroError::Wal(msg)),
            RecoveryMode::Permissive => {
                invalid_txs.entry(txid).or_insert(RecoverySkippedTx {
                    txid,
                    code,
                    reason: msg,
                });
                Ok(())
            }
        }
    };

    for (lsn, record) in &records {
        match record {
            WalRecord::Begin { txid } => {
                let state = tx_states
                    .entry(*txid)
                    .or_insert_with(TxValidationState::new);
                if state.seen_begin {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::DuplicateBegin,
                        format!("Duplicate Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::BeginAfterTerminal,
                        format!(
                            "Begin after terminal record for txid {} at LSN {}",
                            txid, lsn
                        ),
                    )?;
                    continue;
                }
                state.seen_begin = true;
            }
            WalRecord::PagePut { txid, .. } => {
                let state = tx_states
                    .entry(*txid)
                    .or_insert_with(TxValidationState::new);
                if !state.seen_begin {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::PagePutBeforeBegin,
                        format!("PagePut before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::PagePutAfterTerminal,
                        format!(
                            "PagePut after terminal record for txid {} at LSN {}",
                            txid, lsn
                        ),
                    )?;
                    continue;
                }
            }
            WalRecord::MetaUpdate { txid, .. } => {
                let state = tx_states
                    .entry(*txid)
                    .or_insert_with(TxValidationState::new);
                if !state.seen_begin {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::MetaUpdateBeforeBegin,
                        format!("MetaUpdate before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::MetaUpdateAfterTerminal,
                        format!(
                            "MetaUpdate after terminal record for txid {} at LSN {}",
                            txid, lsn
                        ),
                    )?;
                    continue;
                }
                state.seen_meta_update = true;
            }
            WalRecord::Commit {
                txid,
                lsn: commit_lsn,
            } => {
                let state = tx_states
                    .entry(*txid)
                    .or_insert_with(TxValidationState::new);
                if !state.seen_begin {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::CommitBeforeBegin,
                        format!("Commit before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::DuplicateTerminal,
                        format!("Duplicate terminal record for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if !state.seen_meta_update {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::CommitWithoutMetaUpdate,
                        format!("Commit without MetaUpdate for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if *commit_lsn != *lsn {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::CommitLsnMismatch,
                        format!(
                            "Commit LSN mismatch for txid {}: record lsn={}, declared lsn={}",
                            txid, lsn, commit_lsn
                        ),
                    )?;
                    continue;
                }
                state.terminal = Some(TxTerminalState::Committed);
            }
            WalRecord::Abort { txid } => {
                let state = tx_states
                    .entry(*txid)
                    .or_insert_with(TxValidationState::new);
                if !state.seen_begin {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::AbortBeforeBegin,
                        format!("Abort before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        RecoverySkipCode::DuplicateTerminal,
                        format!("Duplicate terminal record for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                state.terminal = Some(TxTerminalState::Aborted);
            }
        }
    }

    let terminal: HashMap<TxId, TxTerminalState> = tx_states
        .iter()
        .filter_map(|(txid, state)| {
            if invalid_txs.contains_key(txid) {
                None
            } else {
                state.terminal.map(|t| (*txid, t))
            }
        })
        .collect();

    // Phase 2: Collect the latest page data and metadata from committed transactions
    let mut page_updates: HashMap<PageId, Vec<u8>> = HashMap::new();
    let mut latest_catalog_root: Option<u64> = None;
    let mut latest_page_count: Option<u64> = None;
    let mut latest_freelist_page_id: Option<u64> = None;
    let mut latest_epoch: Option<u64> = None;

    for (_, record) in &records {
        match record {
            WalRecord::PagePut {
                txid,
                page_id,
                data,
            } => {
                if matches!(terminal.get(txid), Some(TxTerminalState::Committed)) {
                    page_updates.insert(*page_id, data.clone());
                }
            }
            WalRecord::MetaUpdate {
                txid,
                catalog_root,
                page_count,
                freelist_page_id,
                epoch,
            } => {
                if matches!(terminal.get(txid), Some(TxTerminalState::Committed)) {
                    latest_catalog_root = Some(*catalog_root);
                    latest_page_count = Some(*page_count);
                    latest_freelist_page_id = Some(*freelist_page_id);
                    latest_epoch = Some(*epoch);
                }
            }
            _ => {}
        }
    }

    // Phase 3: Validate/collect replayable page updates and optionally apply to DB.
    let mut pager = if apply_to_db {
        Some(Pager::open_with_suite(
            db_path.expect("db path required"),
            Some(suite),
            master_key,
        )?)
    } else {
        None
    };
    let mut pages_replayed = 0;

    for (&page_id, data) in &page_updates {
        if data.len() != PAGE_SIZE {
            match mode {
                RecoveryMode::Strict => {
                    return Err(MuroError::Wal(format!(
                        "Committed PagePut has invalid size for page {}: got {}, expected {}",
                        page_id,
                        data.len(),
                        PAGE_SIZE
                    )));
                }
                RecoveryMode::Permissive => continue,
            }
        }
        let mut page_data = [0u8; PAGE_SIZE];
        page_data.copy_from_slice(data);
        let page = Page::from_bytes(page_data);
        let embedded_page_id = page.page_id();
        if embedded_page_id != page_id {
            match mode {
                RecoveryMode::Strict => {
                    return Err(MuroError::Wal(format!(
                        "Committed PagePut page_id mismatch: record={}, embedded={}",
                        page_id, embedded_page_id
                    )));
                }
                RecoveryMode::Permissive => continue,
            }
        }
        if let Some(p) = pager.as_mut() {
            p.write_page(&page)?;
        }
        pages_replayed += 1;
    }

    // Phase 4: Restore metadata from WAL MetaUpdate records (DB apply mode only)
    if let Some(p) = pager.as_mut() {
        if let Some(catalog_root) = latest_catalog_root {
            p.set_catalog_root(catalog_root);
        }
        if let Some(page_count) = latest_page_count {
            // Only increase page_count, never decrease it
            if page_count > p.page_count() {
                p.set_page_count(page_count);
            }
        }
        if let Some(freelist_page_id) = latest_freelist_page_id {
            p.set_freelist_page_id(freelist_page_id);
        }
        if let Some(epoch) = latest_epoch {
            p.set_epoch(epoch);
        }

        // Also ensure page_count covers all replayed pages (fallback safety)
        for &page_id in page_updates.keys() {
            let needed = page_id + 1;
            if needed > p.page_count() {
                p.set_page_count(needed);
            }
        }

        p.flush_meta()?;
    }

    let mut committed_txids = terminal
        .iter()
        .filter_map(|(txid, state)| {
            if *state == TxTerminalState::Committed {
                Some(*txid)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    committed_txids.sort_unstable();

    let mut aborted_txids = terminal
        .iter()
        .filter_map(|(txid, state)| {
            if *state == TxTerminalState::Aborted {
                Some(*txid)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    aborted_txids.sort_unstable();

    Ok(RecoveryResult {
        committed_txids,
        aborted_txids,
        pages_replayed,
        skipped: {
            let mut skipped = invalid_txs.into_values().collect::<Vec<_>>();
            skipped.sort_by_key(|x| x.txid);
            skipped
        },
        wal_quarantine_path: None,
    })
}

/// Recover the database from WAL in permissive mode.
pub fn recover_permissive(
    db_path: &Path,
    wal_path: &Path,
    master_key: &MasterKey,
) -> Result<RecoveryResult> {
    recover_with_mode(db_path, wal_path, master_key, RecoveryMode::Permissive)
}

#[derive(Debug)]
pub struct RecoveryResult {
    /// Txids that reached `Commit`, sorted in ascending order.
    pub committed_txids: Vec<TxId>,
    /// Txids that reached `Abort`, sorted in ascending order.
    pub aborted_txids: Vec<TxId>,
    pub pages_replayed: usize,
    pub skipped: Vec<RecoverySkippedTx>,
    pub wal_quarantine_path: Option<String>,
}

#[derive(Debug)]
pub struct RecoverySkippedTx {
    pub txid: TxId,
    pub code: RecoverySkipCode,
    pub reason: String,
}

#[cfg(test)]
mod tests;
