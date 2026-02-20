use std::collections::HashMap;
use std::path::Path;

use crate::crypto::aead::MasterKey;
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
    if !wal_path.exists() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
            skipped: Vec::new(),
        });
    }

    let mut reader = WalReader::open(wal_path, master_key)?;
    let records = reader.read_all()?;

    if records.is_empty() {
        return Ok(RecoveryResult {
            committed_txids: Vec::new(),
            aborted_txids: Vec::new(),
            pages_replayed: 0,
            skipped: Vec::new(),
        });
    }

    // Phase 1: Validate WAL transaction lifecycle against TLA+ state machine.
    // Allowed transitions:
    //   Init -> Begin -> (PagePut | MetaUpdate)* -> (Commit | Abort)
    // No record is allowed after Commit/Abort for the same txid.
    let mut tx_states: HashMap<TxId, TxValidationState> = HashMap::new();
    let mut invalid_txs: HashMap<TxId, String> = HashMap::new();

    let mut invalidate_or_err = |txid: TxId, msg: String| -> Result<()> {
        match mode {
            RecoveryMode::Strict => Err(MuroError::Wal(msg)),
            RecoveryMode::Permissive => {
                invalid_txs.entry(txid).or_insert(msg);
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
                        format!("Duplicate Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
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
                        format!("PagePut before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
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
                        format!("MetaUpdate before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
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
                        format!("Commit before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
                        format!("Duplicate terminal record for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if !state.seen_meta_update {
                    invalidate_or_err(
                        *txid,
                        format!("Commit without MetaUpdate for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if *commit_lsn != *lsn {
                    invalidate_or_err(
                        *txid,
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
                        format!("Abort before Begin for txid {} at LSN {}", txid, lsn),
                    )?;
                    continue;
                }
                if state.terminal.is_some() {
                    invalidate_or_err(
                        *txid,
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
            } => {
                if matches!(terminal.get(txid), Some(TxTerminalState::Committed)) {
                    latest_catalog_root = Some(*catalog_root);
                    latest_page_count = Some(*page_count);
                }
            }
            _ => {}
        }
    }

    // Phase 3: Apply page updates to the database
    let mut pager = Pager::open(db_path, master_key)?;
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
        pager.write_page(&page)?;
        pages_replayed += 1;
    }

    // Phase 4: Restore metadata from WAL MetaUpdate records
    if let Some(catalog_root) = latest_catalog_root {
        pager.set_catalog_root(catalog_root);
    }
    if let Some(page_count) = latest_page_count {
        // Only increase page_count, never decrease it
        if page_count > pager.page_count() {
            pager.set_page_count(page_count);
        }
    }

    // Also ensure page_count covers all replayed pages (fallback safety)
    for &page_id in page_updates.keys() {
        let needed = page_id + 1;
        if needed > pager.page_count() {
            pager.set_page_count(needed);
        }
    }

    pager.flush_meta()?;

    Ok(RecoveryResult {
        committed_txids: terminal
            .iter()
            .filter_map(|(txid, state)| {
                if *state == TxTerminalState::Committed {
                    Some(*txid)
                } else {
                    None
                }
            })
            .collect(),
        aborted_txids: terminal
            .iter()
            .filter_map(|(txid, state)| {
                if *state == TxTerminalState::Aborted {
                    Some(*txid)
                } else {
                    None
                }
            })
            .collect(),
        pages_replayed,
        skipped: invalid_txs
            .into_iter()
            .map(|(txid, reason)| RecoverySkippedTx { txid, reason })
            .collect(),
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
    pub committed_txids: Vec<TxId>,
    pub aborted_txids: Vec<TxId>,
    pub pages_replayed: usize,
    pub skipped: Vec<RecoverySkippedTx>,
}

#[derive(Debug)]
pub struct RecoverySkippedTx {
    pub txid: TxId,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    #[test]
    fn test_recovery_committed_tx() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        // Create initial database
        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        // Write WAL with committed transaction
        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();

            // Write a full page of data
            let mut page = Page::new(1);
            page.insert_cell(b"recovered data").unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: page.data.to_vec(),
                })
                .unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 0,
                    page_count: 2,
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 3 })
                .unwrap();
            writer.sync().unwrap();
        }

        // Run recovery
        let result = recover(&db_path, &wal_path, &test_key()).unwrap();
        assert_eq!(result.committed_txids.len(), 1);
        assert_eq!(result.pages_replayed, 1);
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn test_recovery_uncommitted_tx_discarded() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        // Write WAL with uncommitted transaction (no Commit record = crash simulation)
        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            let page = Page::new(1);
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: page.data.to_vec(),
                })
                .unwrap();
            // No commit - simulating crash
            writer.sync().unwrap();
        }

        let result = recover(&db_path, &wal_path, &test_key()).unwrap();
        assert!(result.committed_txids.is_empty());
        assert_eq!(result.pages_replayed, 0);
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn test_recovery_no_wal() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        let result = recover(&db_path, &wal_path, &test_key()).unwrap();
        assert!(result.committed_txids.is_empty());
        assert_eq!(result.pages_replayed, 0);
        assert!(result.skipped.is_empty());
    }

    #[test]
    fn test_recovery_restores_page_count() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        // Create initial database, allocate a page so page_count=1
        let page_data;
        {
            let mut pager = Pager::create(&db_path, &test_key()).unwrap();
            let mut page = pager.allocate_page().unwrap();
            page.insert_cell(b"initial").unwrap();
            pager.write_page(&page).unwrap();
            pager.flush_meta().unwrap();

            // Create another page image for WAL (simulating a tx that wrote page 1)
            let mut p = Page::new(1);
            p.insert_cell(b"from wal").unwrap();
            page_data = p.data.to_vec();
        }

        // Write WAL with committed tx that updates page 1 and sets catalog_root=42
        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: page_data,
                })
                .unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 42,
                    page_count: 2,
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 3 })
                .unwrap();
            writer.sync().unwrap();
        }

        recover(&db_path, &wal_path, &test_key()).unwrap();

        // Verify metadata was restored
        let pager = Pager::open(&db_path, &test_key()).unwrap();
        assert!(pager.page_count() >= 2);
        assert_eq!(pager.catalog_root(), 42);
    }

    #[test]
    fn test_recovery_rejects_commit_lsn_mismatch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 0,
                    page_count: 1,
                })
                .unwrap();
            // Actual LSN here is 2, but declared as 999.
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 999 })
                .unwrap();
            writer.sync().unwrap();
        }

        let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
        match err {
            MuroError::Wal(msg) => assert!(msg.contains("Commit LSN mismatch")),
            other => panic!("Expected WAL error, got: {:?}", other),
        }
    }

    #[test]
    fn test_recovery_rejects_duplicate_terminal_record_for_tx() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 0,
                    data: Page::new(0).data.to_vec(),
                })
                .unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 0,
                    page_count: 1,
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 3 })
                .unwrap();
            // Conflicting terminal record should be rejected.
            writer.append(&WalRecord::Abort { txid: 1 }).unwrap();
            writer.sync().unwrap();
        }

        let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
        match err {
            MuroError::Wal(msg) => assert!(msg.contains("Duplicate terminal record")),
            other => panic!("Expected WAL error, got: {:?}", other),
        }
    }

    #[test]
    fn test_recovery_rejects_commit_without_meta_update() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 1 })
                .unwrap();
            writer.sync().unwrap();
        }

        let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
        match err {
            MuroError::Wal(msg) => assert!(msg.contains("Commit without MetaUpdate")),
            other => panic!("Expected WAL error, got: {:?}", other),
        }
    }

    #[test]
    fn test_recovery_rejects_pageput_before_begin() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 0,
                    data: Page::new(0).data.to_vec(),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
        match err {
            MuroError::Wal(msg) => assert!(msg.contains("PagePut before Begin")),
            other => panic!("Expected WAL error, got: {:?}", other),
        }
    }

    #[test]
    fn test_recovery_rejects_pageput_page_id_mismatch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();

            let page = Page::new(999);
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: page.data.to_vec(),
                })
                .unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 0,
                    page_count: 2,
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 3 })
                .unwrap();
            writer.sync().unwrap();
        }

        let err = recover(&db_path, &wal_path, &test_key()).unwrap_err();
        match err {
            MuroError::Wal(msg) => assert!(msg.contains("page_id mismatch")),
            other => panic!("Expected WAL error, got: {:?}", other),
        }
    }

    #[test]
    fn test_recovery_permissive_ignores_commit_without_meta() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 1 })
                .unwrap();
            writer.sync().unwrap();
        }

        let result =
            recover_with_mode(&db_path, &wal_path, &test_key(), RecoveryMode::Permissive).unwrap();
        assert_eq!(result.pages_replayed, 0);
        assert!(result.committed_txids.is_empty());
        assert!(result.aborted_txids.is_empty());
        assert_eq!(result.skipped.len(), 1);
        assert_eq!(result.skipped[0].txid, 1);
        assert!(result.skipped[0]
            .reason
            .contains("Commit without MetaUpdate"));
    }

    #[test]
    fn test_recovery_permissive_ignores_page_id_mismatch() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_path = dir.path().join("test.wal");

        {
            let _pager = Pager::create(&db_path, &test_key()).unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, &test_key()).unwrap();
            writer.append(&WalRecord::Begin { txid: 1 }).unwrap();
            let bad_page = Page::new(999);
            writer
                .append(&WalRecord::PagePut {
                    txid: 1,
                    page_id: 1,
                    data: bad_page.data.to_vec(),
                })
                .unwrap();
            writer
                .append(&WalRecord::MetaUpdate {
                    txid: 1,
                    catalog_root: 0,
                    page_count: 2,
                })
                .unwrap();
            writer
                .append(&WalRecord::Commit { txid: 1, lsn: 3 })
                .unwrap();
            writer.sync().unwrap();
        }

        let result =
            recover_with_mode(&db_path, &wal_path, &test_key(), RecoveryMode::Permissive).unwrap();
        assert_eq!(result.pages_replayed, 0);
        assert_eq!(result.committed_txids, vec![1]);
        assert!(result.skipped.is_empty());
    }
}
