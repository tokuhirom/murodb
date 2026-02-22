/// FTS index: commit-time update with pending buffer.
///
/// Uses a B-tree to store postings:
///   legacy key = term_id (HMAC-SHA256 of bigram), value = serialized PostingList
///   segmented keys = "__segmeta__"+term_id + "__segdata__"+term_id+segment_idx
/// Posting lists are segmented to keep each value below page-size limits.
///
/// Also stores statistics in the same B-tree:
///   key = b"__stats__"
///   value = FtsStats serialized
use crate::btree::ops::BTree;
use crate::crypto::hmac_util::hmac_term_id;
use crate::error::Result;
use crate::fts::postings::{Posting, PostingList};
use crate::fts::tokenizer::tokenize_bigram;
use crate::storage::page::PageId;
use crate::storage::page_store::PageStore;

/// FTS index handle.
pub struct FtsIndex {
    btree: BTree,
    term_key: [u8; 32],
}

/// FTS statistics for BM25 scoring.
#[derive(Debug, Clone, Default)]
pub struct FtsStats {
    pub total_docs: u64,
    pub total_tokens: u64,
}

impl FtsStats {
    pub fn avg_doc_len(&self) -> f64 {
        if self.total_docs == 0 {
            0.0
        } else {
            self.total_tokens as f64 / self.total_docs as f64
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.total_docs.to_le_bytes());
        buf.extend_from_slice(&self.total_tokens.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        Some(FtsStats {
            total_docs: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            total_tokens: u64::from_le_bytes(data[8..16].try_into().unwrap()),
        })
    }
}

const STATS_KEY: &[u8] = b"__stats__";
const SEG_META_PREFIX: &[u8] = b"__segmeta__";
const SEG_GEN_PREFIX: &[u8] = b"__seggen__";
const SEG_DATA_PREFIX: &[u8] = b"__segdata__";
const SEG_DATA_V2_PREFIX: &[u8] = b"__segv2__";
const SEG_META_V2_VERSION: u8 = 2;
const SEG_GC_HEAD_KEY: &[u8] = b"__seggc_head__";
const SEG_GC_TAIL_KEY: &[u8] = b"__seggc_tail__";
const SEG_GC_TASK_PREFIX: &[u8] = b"__seggc__";
// Keep payloads comfortably below page-cell limits even with key/cell overhead.
const MAX_SEGMENT_PAYLOAD_BYTES: usize = 3000;

#[derive(Clone, Copy)]
enum SegmentKeyFormat {
    LegacyU16,
    U32,
}

#[derive(Clone, Copy)]
enum SegmentMeta {
    V1 {
        seg_count: u32,
        key_format: SegmentKeyFormat,
    },
    V2 {
        generation: u32,
        seg_count: u32,
    },
}

#[derive(Clone, Copy)]
struct SegmentGcTask {
    tid: [u8; 32],
    old_meta: Option<SegmentMeta>,
    delete_legacy_single: bool,
}

/// Pending FTS operation (accumulated during a transaction).
#[derive(Debug, Clone)]
pub enum FtsPendingOp {
    Add { doc_id: u64, text: String },
    Remove { doc_id: u64, text: String },
}

impl FtsIndex {
    /// Create a new FTS index.
    pub fn create(pager: &mut impl PageStore, term_key: [u8; 32]) -> Result<Self> {
        let btree = BTree::create(pager)?;
        let mut index = FtsIndex { btree, term_key };

        // Initialize stats
        let stats = FtsStats::default();
        index.btree.insert(pager, STATS_KEY, &stats.serialize())?;

        Ok(index)
    }

    /// Open an existing FTS index.
    pub fn open(root_page_id: PageId, term_key: [u8; 32]) -> Self {
        FtsIndex {
            btree: BTree::open(root_page_id),
            term_key,
        }
    }

    pub fn root_page_id(&self) -> PageId {
        self.btree.root_page_id()
    }

    /// Get the term_id for a bigram.
    pub fn term_id(&self, bigram: &str) -> [u8; 32] {
        hmac_term_id(&self.term_key, bigram.as_bytes())
    }

    /// Get the posting list for a term.
    pub fn get_postings(&self, pager: &mut impl PageStore, term: &str) -> Result<PostingList> {
        let tid = self.term_id(term);
        self.load_postings_by_tid(pager, &tid)
    }

    /// Get FTS statistics.
    pub fn get_stats(&self, pager: &mut impl PageStore) -> Result<FtsStats> {
        match self.btree.search(pager, STATS_KEY)? {
            Some(data) => Ok(FtsStats::deserialize(&data).unwrap_or_default()),
            None => Ok(FtsStats::default()),
        }
    }

    /// Apply pending operations at commit time.
    pub fn apply_pending(
        &mut self,
        pager: &mut impl PageStore,
        ops: &[FtsPendingOp],
    ) -> Result<()> {
        let mut stats = self.get_stats(pager)?;

        for op in ops {
            match op {
                FtsPendingOp::Add { doc_id, text } => {
                    let tokens = tokenize_bigram(text);
                    let token_count = tokens.len();

                    // Group tokens by bigram text
                    let mut term_positions: std::collections::HashMap<String, Vec<u32>> =
                        std::collections::HashMap::new();
                    for token in &tokens {
                        term_positions
                            .entry(token.text.clone())
                            .or_default()
                            .push(token.position as u32);
                    }

                    // Update posting lists
                    for (term, positions) in &term_positions {
                        let tid = self.term_id(term);
                        let mut pl = self.load_postings_by_tid(pager, &tid)?;
                        pl.add(*doc_id, positions.clone());
                        self.store_postings_by_tid(pager, &tid, &pl)?;
                    }

                    stats.total_docs += 1;
                    stats.total_tokens += token_count as u64;
                }
                FtsPendingOp::Remove { doc_id, text } => {
                    let tokens = tokenize_bigram(text);
                    let token_count = tokens.len();

                    let mut seen_terms: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for token in &tokens {
                        if seen_terms.insert(token.text.clone()) {
                            let tid = self.term_id(&token.text);
                            let mut pl = self.load_postings_by_tid(pager, &tid)?;
                            if pl.df() == 0 {
                                continue;
                            }
                            pl.remove(*doc_id);
                            self.store_postings_by_tid(pager, &tid, &pl)?;
                        }
                    }

                    if stats.total_docs > 0 {
                        stats.total_docs -= 1;
                    }
                    stats.total_tokens = stats.total_tokens.saturating_sub(token_count as u64);
                }
            }
        }

        // Save updated stats
        self.btree.insert(pager, STATS_KEY, &stats.serialize())?;

        Ok(())
    }

    /// Build index from scratch for all documents.
    pub fn build_from_docs(
        &mut self,
        pager: &mut impl PageStore,
        docs: &[(u64, String)], // (doc_id, text)
    ) -> Result<()> {
        let ops: Vec<FtsPendingOp> = docs
            .iter()
            .map(|(doc_id, text)| FtsPendingOp::Add {
                doc_id: *doc_id,
                text: text.clone(),
            })
            .collect();
        self.apply_pending(pager, &ops)
    }

    /// Vacuum stale segmented payloads left behind by generation switching.
    /// Returns the number of GC tasks processed.
    pub fn vacuum_stale_segments(
        &mut self,
        pager: &mut impl PageStore,
        max_tasks: usize,
    ) -> Result<usize> {
        if max_tasks == 0 {
            return Ok(0);
        }

        let mut head = self.load_gc_counter(pager, SEG_GC_HEAD_KEY)?;
        let tail = self.load_gc_counter(pager, SEG_GC_TAIL_KEY)?;
        let mut processed = 0usize;
        while head < tail && processed < max_tasks {
            let task_key = seg_gc_task_key(head);
            if let Some(raw) = self.btree.search(pager, &task_key)? {
                let task = decode_segment_gc_task(&raw)?;
                self.delete_postings_from_meta(pager, &task.tid, task.old_meta)?;
                if task.delete_legacy_single && self.btree.search(pager, &task.tid)?.is_some() {
                    self.btree.delete(pager, &task.tid)?;
                }
                self.btree.delete(pager, &task_key)?;
            }
            head = head.saturating_add(1);
            processed = processed.saturating_add(1);
        }
        self.store_gc_counter(pager, SEG_GC_HEAD_KEY, head)?;
        if head >= tail {
            // Keep counters bounded once queue is drained.
            self.store_gc_counter(pager, SEG_GC_HEAD_KEY, 0)?;
            self.store_gc_counter(pager, SEG_GC_TAIL_KEY, 0)?;
        }
        Ok(processed)
    }

    fn load_postings_by_tid(
        &self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
    ) -> Result<PostingList> {
        if let Some(meta) = self.btree.search(pager, &seg_meta_key(tid))? {
            let meta = decode_segment_meta(&meta)?;
            let mut merged = PostingList::new();
            let seg_count = match meta {
                SegmentMeta::V1 { seg_count, .. } => seg_count,
                SegmentMeta::V2 { seg_count, .. } => seg_count,
            };
            for seg_idx in 0..seg_count {
                let key = segment_payload_key(tid, meta, seg_idx)?;
                let data = self.btree.search(pager, &key)?.ok_or_else(|| {
                    crate::error::MuroError::Corruption("missing segmented posting payload".into())
                })?;
                let segment = PostingList::deserialize(&data).ok_or_else(|| {
                    crate::error::MuroError::Corruption("failed to deserialize posting list".into())
                })?;
                merged.merge(&segment);
            }
            Ok(merged)
        } else {
            match self.btree.search(pager, tid)? {
                Some(data) => PostingList::deserialize(&data).ok_or_else(|| {
                    crate::error::MuroError::Corruption("failed to deserialize posting list".into())
                }),
                None => Ok(PostingList::new()),
            }
        }
    }

    fn store_postings_by_tid(
        &mut self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        pl: &PostingList,
    ) -> Result<()> {
        if pl.df() == 0 {
            return self.delete_postings_by_tid(pager, tid);
        }

        let old_meta = self
            .btree
            .search(pager, &seg_meta_key(tid))?
            .map(|meta| decode_segment_meta(&meta))
            .transpose()?;
        let had_legacy_single = self.btree.search(pager, tid)?.is_some();
        let last_generation_from_meta = old_meta
            .and_then(|meta| match meta {
                SegmentMeta::V2 { generation, .. } => Some(generation),
                SegmentMeta::V1 { .. } => None,
            })
            .unwrap_or(0);
        let last_generation = std::cmp::max(
            last_generation_from_meta,
            self.load_term_generation_counter(pager, tid)?,
        );
        let new_generation = last_generation.checked_add(1).ok_or_else(|| {
            crate::error::MuroError::Execution("segment generation exceeds u32 range".into())
        })?;

        let segments = split_postings_into_segments(pl, MAX_SEGMENT_PAYLOAD_BYTES)?;
        let seg_count_usize = segments.len();
        let seg_count = u32::try_from(seg_count_usize).map_err(|_| {
            crate::error::MuroError::Execution("segmented posting count exceeds u32 range".into())
        })?;
        for (idx, seg_pl) in segments.iter().enumerate() {
            let seg_idx = u32::try_from(idx).map_err(|_| {
                crate::error::MuroError::Execution("segment index exceeds u32 range".into())
            })?;
            let key = seg_data_key_v2(tid, new_generation, seg_idx);
            self.btree.insert(pager, &key, &seg_pl.serialize())?;
        }

        let mut meta_buf = Vec::with_capacity(9);
        meta_buf.push(SEG_META_V2_VERSION);
        meta_buf.extend_from_slice(&new_generation.to_le_bytes());
        meta_buf.extend_from_slice(&seg_count.to_le_bytes());
        self.btree.insert(pager, &seg_meta_key(tid), &meta_buf)?;
        self.store_term_generation_counter(pager, tid, new_generation)?;

        if old_meta.is_some() || had_legacy_single {
            let task = SegmentGcTask {
                tid: *tid,
                old_meta,
                delete_legacy_single: had_legacy_single,
            };
            self.enqueue_segment_gc_task(pager, task)?;
        }
        Ok(())
    }

    fn delete_postings_by_tid(&mut self, pager: &mut impl PageStore, tid: &[u8; 32]) -> Result<()> {
        if let Some(meta) = self.btree.search(pager, &seg_meta_key(tid))? {
            let decoded = decode_segment_meta(&meta)?;
            self.delete_postings_from_meta(pager, tid, Some(decoded))?;
            self.btree.delete(pager, &seg_meta_key(tid))?;
        }
        if self.btree.search(pager, tid)?.is_some() {
            self.btree.delete(pager, tid)?;
        }
        Ok(())
    }

    fn delete_postings_from_meta(
        &mut self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        meta: Option<SegmentMeta>,
    ) -> Result<()> {
        let Some(meta) = meta else {
            return Ok(());
        };
        let seg_count = match meta {
            SegmentMeta::V1 { seg_count, .. } => seg_count,
            SegmentMeta::V2 { seg_count, .. } => seg_count,
        };
        for seg_idx in 0..seg_count {
            let key = segment_payload_key(tid, meta, seg_idx)?;
            if self.btree.search(pager, &key)?.is_some() {
                self.btree.delete(pager, &key)?;
            }
        }
        Ok(())
    }

    fn enqueue_segment_gc_task(
        &mut self,
        pager: &mut impl PageStore,
        task: SegmentGcTask,
    ) -> Result<()> {
        let tail = self.load_gc_counter(pager, SEG_GC_TAIL_KEY)?;
        self.btree
            .insert(pager, &seg_gc_task_key(tail), &encode_segment_gc_task(task))?;
        self.store_gc_counter(pager, SEG_GC_TAIL_KEY, tail.saturating_add(1))?;
        Ok(())
    }

    fn load_gc_counter(&self, pager: &mut impl PageStore, key: &[u8]) -> Result<u64> {
        match self.btree.search(pager, key)? {
            Some(raw) if raw.len() == 8 => Ok(u64::from_le_bytes([
                raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
            ])),
            Some(_) => Err(crate::error::MuroError::Corruption(
                "invalid segmented GC counter".into(),
            )),
            None => Ok(0),
        }
    }

    fn store_gc_counter(
        &mut self,
        pager: &mut impl PageStore,
        key: &[u8],
        value: u64,
    ) -> Result<()> {
        self.btree.insert(pager, key, &value.to_le_bytes())
    }

    fn load_term_generation_counter(
        &self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
    ) -> Result<u32> {
        match self.btree.search(pager, &seg_generation_key(tid))? {
            Some(raw) if raw.len() == 4 => Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]])),
            Some(_) => Err(crate::error::MuroError::Corruption(
                "invalid segmented generation counter".into(),
            )),
            None => Ok(0),
        }
    }

    fn store_term_generation_counter(
        &mut self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        generation: u32,
    ) -> Result<()> {
        self.btree
            .insert(pager, &seg_generation_key(tid), &generation.to_le_bytes())
    }
}

fn posting_list_encoded_len(postings: &[Posting]) -> usize {
    PostingList {
        postings: postings.to_vec(),
    }
    .serialize()
    .len()
}

fn split_posting_into_sized_entries(
    posting: &Posting,
    max_payload_bytes: usize,
) -> Result<Vec<Posting>> {
    let single_len = posting_list_encoded_len(std::slice::from_ref(posting));
    if single_len <= max_payload_bytes {
        return Ok(vec![posting.clone()]);
    }

    let mut out = Vec::new();
    let mut start = 0usize;
    while start < posting.positions.len() {
        let mut low = 1usize;
        let mut high = posting.positions.len() - start;
        let mut best = 0usize;
        while low <= high {
            let mid = low + (high - low) / 2;
            let candidate = Posting {
                doc_id: posting.doc_id,
                positions: posting.positions[start..start + mid].to_vec(),
            };
            if posting_list_encoded_len(std::slice::from_ref(&candidate)) <= max_payload_bytes {
                best = mid;
                low = mid + 1;
            } else if mid == 1 {
                break;
            } else {
                high = mid - 1;
            }
        }
        if best == 0 {
            return Err(crate::error::MuroError::Execution(
                "posting entry cannot fit into segmented payload".into(),
            ));
        }
        out.push(Posting {
            doc_id: posting.doc_id,
            positions: posting.positions[start..start + best].to_vec(),
        });
        start += best;
    }
    Ok(out)
}

fn split_postings_into_segments(
    pl: &PostingList,
    max_payload_bytes: usize,
) -> Result<Vec<PostingList>> {
    let mut expanded = Vec::new();
    for posting in &pl.postings {
        expanded.extend(split_posting_into_sized_entries(
            posting,
            max_payload_bytes,
        )?);
    }

    let mut segments: Vec<PostingList> = Vec::new();
    let mut current = PostingList::new();
    for posting in expanded {
        let mut trial = current.postings.clone();
        trial.push(posting.clone());
        if posting_list_encoded_len(&trial) <= max_payload_bytes {
            current.postings.push(posting);
            continue;
        }

        if current.postings.is_empty() {
            return Err(crate::error::MuroError::Execution(
                "posting entry cannot fit into empty segment".into(),
            ));
        }
        segments.push(current);
        current = PostingList {
            postings: vec![posting],
        };
    }
    if !current.postings.is_empty() {
        segments.push(current);
    }
    Ok(segments)
}

fn seg_meta_key(tid: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_META_PREFIX.len() + tid.len());
    key.extend_from_slice(SEG_META_PREFIX);
    key.extend_from_slice(tid);
    key
}

fn seg_data_key(tid: &[u8; 32], idx: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_DATA_PREFIX.len() + tid.len() + 4);
    key.extend_from_slice(SEG_DATA_PREFIX);
    key.extend_from_slice(tid);
    key.extend_from_slice(&idx.to_le_bytes());
    key
}

fn seg_generation_key(tid: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_GEN_PREFIX.len() + tid.len());
    key.extend_from_slice(SEG_GEN_PREFIX);
    key.extend_from_slice(tid);
    key
}

fn seg_data_key_legacy_u16(tid: &[u8; 32], idx: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_DATA_PREFIX.len() + tid.len() + 2);
    key.extend_from_slice(SEG_DATA_PREFIX);
    key.extend_from_slice(tid);
    key.extend_from_slice(&idx.to_le_bytes());
    key
}

fn seg_data_key_v2(tid: &[u8; 32], generation: u32, idx: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_DATA_V2_PREFIX.len() + tid.len() + 8);
    key.extend_from_slice(SEG_DATA_V2_PREFIX);
    key.extend_from_slice(tid);
    key.extend_from_slice(&generation.to_le_bytes());
    key.extend_from_slice(&idx.to_le_bytes());
    key
}

fn seg_gc_task_key(seq: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_GC_TASK_PREFIX.len() + 8);
    key.extend_from_slice(SEG_GC_TASK_PREFIX);
    key.extend_from_slice(&seq.to_be_bytes());
    key
}

fn segment_payload_key(tid: &[u8; 32], meta: SegmentMeta, seg_idx: u32) -> Result<Vec<u8>> {
    match meta {
        SegmentMeta::V1 {
            key_format: SegmentKeyFormat::LegacyU16,
            ..
        } => Ok(seg_data_key_legacy_u16(
            tid,
            u16::try_from(seg_idx).map_err(|_| {
                crate::error::MuroError::Corruption(
                    "legacy segmented posting index exceeds u16 range".into(),
                )
            })?,
        )),
        SegmentMeta::V1 {
            key_format: SegmentKeyFormat::U32,
            ..
        } => Ok(seg_data_key(tid, seg_idx)),
        SegmentMeta::V2 { generation, .. } => Ok(seg_data_key_v2(tid, generation, seg_idx)),
    }
}

fn decode_segment_meta(meta: &[u8]) -> Result<SegmentMeta> {
    match meta.len() {
        9 if meta[0] == SEG_META_V2_VERSION => Ok(SegmentMeta::V2 {
            generation: u32::from_le_bytes([meta[1], meta[2], meta[3], meta[4]]),
            seg_count: u32::from_le_bytes([meta[5], meta[6], meta[7], meta[8]]),
        }),
        // Backward compatibility with early segmented format.
        2 => Ok(SegmentMeta::V1 {
            seg_count: u16::from_le_bytes([meta[0], meta[1]]) as u32,
            key_format: SegmentKeyFormat::LegacyU16,
        }),
        4 => Ok(SegmentMeta::V1 {
            seg_count: u32::from_le_bytes([meta[0], meta[1], meta[2], meta[3]]),
            key_format: SegmentKeyFormat::U32,
        }),
        _ => Err(crate::error::MuroError::Corruption(
            "invalid segmented posting metadata".into(),
        )),
    }
}

fn encode_segment_gc_task(task: SegmentGcTask) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 1 + 32 + 1 + 8);
    out.push(1); // task format version
    out.push(u8::from(task.delete_legacy_single));
    out.extend_from_slice(&task.tid);
    match task.old_meta {
        None => out.push(0),
        Some(SegmentMeta::V1 {
            seg_count,
            key_format: SegmentKeyFormat::LegacyU16,
        }) => {
            out.push(1);
            out.extend_from_slice(&(seg_count as u16).to_le_bytes());
        }
        Some(SegmentMeta::V1 {
            seg_count,
            key_format: SegmentKeyFormat::U32,
        }) => {
            out.push(2);
            out.extend_from_slice(&seg_count.to_le_bytes());
        }
        Some(SegmentMeta::V2 {
            generation,
            seg_count,
        }) => {
            out.push(3);
            out.extend_from_slice(&generation.to_le_bytes());
            out.extend_from_slice(&seg_count.to_le_bytes());
        }
    }
    out
}

fn decode_segment_gc_task(raw: &[u8]) -> Result<SegmentGcTask> {
    if raw.len() < 35 || raw[0] != 1 {
        return Err(crate::error::MuroError::Corruption(
            "invalid segmented GC task".into(),
        ));
    }
    let delete_legacy_single = raw[1] != 0;
    let mut tid = [0u8; 32];
    tid.copy_from_slice(&raw[2..34]);
    let tag = raw[34];
    let old_meta = match tag {
        0 => None,
        1 if raw.len() == 37 => Some(SegmentMeta::V1 {
            seg_count: u16::from_le_bytes([raw[35], raw[36]]) as u32,
            key_format: SegmentKeyFormat::LegacyU16,
        }),
        2 if raw.len() == 39 => Some(SegmentMeta::V1 {
            seg_count: u32::from_le_bytes([raw[35], raw[36], raw[37], raw[38]]),
            key_format: SegmentKeyFormat::U32,
        }),
        3 if raw.len() == 43 => Some(SegmentMeta::V2 {
            generation: u32::from_le_bytes([raw[35], raw[36], raw[37], raw[38]]),
            seg_count: u32::from_le_bytes([raw[39], raw[40], raw[41], raw[42]]),
        }),
        _ => {
            return Err(crate::error::MuroError::Corruption(
                "invalid segmented GC task payload".into(),
            ))
        }
    };
    Ok(SegmentGcTask {
        tid,
        old_meta,
        delete_legacy_single,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::storage::pager::Pager;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    fn term_key() -> [u8; 32] {
        [0x55u8; 32]
    }

    #[test]
    fn test_fts_index_add_and_search() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();

        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let ops = vec![
            FtsPendingOp::Add {
                doc_id: 1,
                text: "東京タワー".to_string(),
            },
            FtsPendingOp::Add {
                doc_id: 2,
                text: "東京スカイツリー".to_string(),
            },
        ];
        idx.apply_pending(&mut pager, &ops).unwrap();

        // Both documents should match "東京"
        let pl = idx.get_postings(&mut pager, "東京").unwrap();
        assert_eq!(pl.df(), 2);

        // Only doc 1 should match "タワ"
        let pl = idx.get_postings(&mut pager, "タワ").unwrap();
        assert_eq!(pl.df(), 1);
        assert_eq!(pl.get(1).unwrap().positions, vec![2]);

        let stats = idx.get_stats(&mut pager).unwrap();
        assert_eq!(stats.total_docs, 2);
    }

    #[test]
    fn test_fts_index_remove() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();

        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        idx.apply_pending(
            &mut pager,
            &[FtsPendingOp::Add {
                doc_id: 1,
                text: "東京タワー".to_string(),
            }],
        )
        .unwrap();

        idx.apply_pending(
            &mut pager,
            &[FtsPendingOp::Remove {
                doc_id: 1,
                text: "東京タワー".to_string(),
            }],
        )
        .unwrap();

        let pl = idx.get_postings(&mut pager, "東京").unwrap();
        assert_eq!(pl.df(), 0);

        let stats = idx.get_stats(&mut pager).unwrap();
        assert_eq!(stats.total_docs, 0);
    }

    #[test]
    fn test_build_from_docs() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();

        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let docs = vec![
            (1, "東京タワーは有名です".to_string()),
            (2, "京都の寺院が美しい".to_string()),
            (3, "東京の夜景が綺麗".to_string()),
        ];
        idx.build_from_docs(&mut pager, &docs).unwrap();

        let stats = idx.get_stats(&mut pager).unwrap();
        assert_eq!(stats.total_docs, 3);

        // "東京" should match docs 1 and 3
        let pl = idx.get_postings(&mut pager, "東京").unwrap();
        assert_eq!(pl.df(), 2);
    }

    #[test]
    fn test_fts_large_posting_list_is_segmented_without_pageoverflow() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();

        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
        let mut ops = Vec::new();
        for doc_id in 1..=80u64 {
            ops.push(FtsPendingOp::Add {
                doc_id,
                text: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            });
        }
        idx.apply_pending(&mut pager, &ops).unwrap();

        let pl = idx.get_postings(&mut pager, "aa").unwrap();
        assert_eq!(pl.df(), 80);
    }

    #[test]
    fn test_store_postings_splits_large_single_doc_positions_without_pageoverflow() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
        let tid = idx.term_id("東京");

        let mut pl = PostingList::new();
        let positions: Vec<u32> = (0..5000u32).collect();
        pl.add(1, positions.clone());
        idx.store_postings_by_tid(&mut pager, &tid, &pl).unwrap();

        let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
        assert_eq!(loaded.df(), 1);
        assert_eq!(loaded.get(1).unwrap().positions, positions);

        let meta = idx
            .btree
            .search(&mut pager, &seg_meta_key(&tid))
            .unwrap()
            .unwrap();
        match decode_segment_meta(&meta).unwrap() {
            SegmentMeta::V2 { seg_count, .. } => assert!(seg_count > 1),
            SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
        }
    }

    #[test]
    fn test_get_postings_reads_legacy_single_value_format() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let tid = idx.term_id("東京");
        let mut pl = PostingList::new();
        pl.add(1, vec![0, 2]);
        pl.add(3, vec![1]);
        idx.btree.insert(&mut pager, &tid, &pl.serialize()).unwrap();

        let loaded = idx.get_postings(&mut pager, "東京").unwrap();
        assert_eq!(loaded.df(), 2);
        assert_eq!(loaded.get(1).unwrap().positions, vec![0, 2]);
        assert_eq!(loaded.get(3).unwrap().positions, vec![1]);
    }

    #[test]
    fn test_get_postings_reads_legacy_segmented_u16_format() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let tid = idx.term_id("東京");
        let mut seg0 = PostingList::new();
        seg0.add(1, vec![0]);
        let mut seg1 = PostingList::new();
        seg1.add(2, vec![1, 2]);

        idx.btree
            .insert(&mut pager, &seg_meta_key(&tid), &(2u16).to_le_bytes())
            .unwrap();
        idx.btree
            .insert(
                &mut pager,
                &seg_data_key_legacy_u16(&tid, 0),
                &seg0.serialize(),
            )
            .unwrap();
        idx.btree
            .insert(
                &mut pager,
                &seg_data_key_legacy_u16(&tid, 1),
                &seg1.serialize(),
            )
            .unwrap();

        let loaded = idx.get_postings(&mut pager, "東京").unwrap();
        assert_eq!(loaded.df(), 2);
        assert_eq!(loaded.get(1).unwrap().positions, vec![0]);
        assert_eq!(loaded.get(2).unwrap().positions, vec![1, 2]);
    }

    #[test]
    fn test_store_postings_migrates_to_segment_v2_without_losing_data() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let tid = idx.term_id("東京");
        let mut old_seg = PostingList::new();
        old_seg.add(1, vec![0]);
        idx.btree
            .insert(&mut pager, &seg_meta_key(&tid), &(1u32).to_le_bytes())
            .unwrap();
        idx.btree
            .insert(&mut pager, &seg_data_key(&tid, 0), &old_seg.serialize())
            .unwrap();

        let mut new_pl = PostingList::new();
        new_pl.add(7, vec![3, 5]);
        idx.store_postings_by_tid(&mut pager, &tid, &new_pl)
            .unwrap();

        let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
        assert_eq!(loaded.df(), 1);
        assert_eq!(loaded.get(7).unwrap().positions, vec![3, 5]);

        let meta = idx
            .btree
            .search(&mut pager, &seg_meta_key(&tid))
            .unwrap()
            .unwrap();
        match decode_segment_meta(&meta).unwrap() {
            SegmentMeta::V2 {
                generation,
                seg_count,
            } => {
                assert_eq!(generation, 1);
                assert_eq!(seg_count, 1);
            }
            SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
        }
    }

    #[test]
    fn test_vacuum_stale_segments_removes_previous_generation_payload() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
        let tid = idx.term_id("東京");

        let mut pl1 = PostingList::new();
        pl1.add(1, vec![0]);
        idx.store_postings_by_tid(&mut pager, &tid, &pl1).unwrap();

        let mut pl2 = PostingList::new();
        pl2.add(2, vec![1]);
        idx.store_postings_by_tid(&mut pager, &tid, &pl2).unwrap();

        assert!(idx
            .btree
            .search(&mut pager, &seg_data_key_v2(&tid, 1, 0))
            .unwrap()
            .is_some());

        let processed = idx.vacuum_stale_segments(&mut pager, 16).unwrap();
        assert!(processed >= 1);
        assert!(idx
            .btree
            .search(&mut pager, &seg_data_key_v2(&tid, 1, 0))
            .unwrap()
            .is_none());
        assert!(idx
            .btree
            .search(&mut pager, &seg_data_key_v2(&tid, 2, 0))
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_vacuum_stale_segments_removes_legacy_single_value() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
        let tid = idx.term_id("東京");

        let mut legacy = PostingList::new();
        legacy.add(1, vec![0]);
        idx.btree
            .insert(&mut pager, &tid, &legacy.serialize())
            .unwrap();

        let mut next = PostingList::new();
        next.add(5, vec![3]);
        idx.store_postings_by_tid(&mut pager, &tid, &next).unwrap();

        assert!(idx.btree.search(&mut pager, &tid).unwrap().is_some());
        let processed = idx.vacuum_stale_segments(&mut pager, 16).unwrap();
        assert!(processed >= 1);
        assert!(idx.btree.search(&mut pager, &tid).unwrap().is_none());
    }

    #[test]
    fn test_generation_does_not_reuse_after_delete_then_readd() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();
        let tid = idx.term_id("東京");

        let mut pl1 = PostingList::new();
        pl1.add(1, vec![0]);
        idx.store_postings_by_tid(&mut pager, &tid, &pl1).unwrap(); // gen=1

        let mut pl2 = PostingList::new();
        pl2.add(2, vec![1]);
        idx.store_postings_by_tid(&mut pager, &tid, &pl2).unwrap(); // gen=2, queue old gen=1 GC

        let empty = PostingList::new();
        idx.store_postings_by_tid(&mut pager, &tid, &empty).unwrap(); // delete term + meta

        let mut pl3 = PostingList::new();
        pl3.add(3, vec![2]);
        idx.store_postings_by_tid(&mut pager, &tid, &pl3).unwrap(); // must become gen=3 (not reused 1)

        let meta = idx
            .btree
            .search(&mut pager, &seg_meta_key(&tid))
            .unwrap()
            .unwrap();
        match decode_segment_meta(&meta).unwrap() {
            SegmentMeta::V2 {
                generation,
                seg_count,
            } => {
                assert_eq!(generation, 3);
                assert_eq!(seg_count, 1);
            }
            SegmentMeta::V1 { .. } => panic!("expected v2 segment metadata"),
        }

        let processed = idx.vacuum_stale_segments(&mut pager, 64).unwrap();
        assert!(processed >= 1);

        let loaded = idx.load_postings_by_tid(&mut pager, &tid).unwrap();
        assert_eq!(loaded.df(), 1);
        assert_eq!(loaded.get(3).unwrap().positions, vec![2]);
    }
}
