/// FTS index: commit-time update with pending buffer.
///
/// Uses a B-tree to store postings:
///   legacy key = term_id (HMAC-SHA256 of bigram), value = serialized PostingList
///   segmented keys = "__segmeta__"+term_id + "__segdata__"+term_id+segment_idx
/// Large segment payloads spill to overflow page chains via "__segovf__" keys.
///
/// Also stores statistics in the same B-tree:
///   key = b"__stats__"
///   value = FtsStats serialized
use crate::btree::ops::BTree;
use crate::crypto::hmac_util::hmac_term_id;
use crate::error::Result;
use crate::fts::postings::{Posting, PostingList};
use crate::fts::tokenizer::tokenize_bigram;
use crate::storage::page::{Page, PageId, PAGE_HEADER_SIZE, PAGE_SIZE};
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
const SEG_OVERFLOW_V2_PREFIX: &[u8] = b"__segovf__";
const SEG_META_V2_VERSION: u8 = 2;
const SEG_GC_HEAD_KEY: &[u8] = b"__seggc_head__";
const SEG_GC_TAIL_KEY: &[u8] = b"__seggc_tail__";
const SEG_GC_TASK_PREFIX: &[u8] = b"__seggc__";
// Keep inline payloads comfortably below page-cell limits even with key/cell overhead.
const MAX_SEGMENT_INLINE_BYTES: usize = 3000;
// Logical segment size target before falling back to overflow pages.
const MAX_SEGMENT_PAYLOAD_BYTES: usize = 64 * 1024;
const OVERFLOW_PAGE_MAGIC: &[u8; 4] = b"OFG1";
const OVERFLOW_PAGE_META_BYTES: usize = 4 + 8 + 2; // magic + next_page_id + chunk_len
const OVERFLOW_PAGE_CHUNK_BYTES: usize = PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_PAGE_META_BYTES;

#[derive(Clone, Copy)]
struct SegmentOverflowRef {
    first_page_id: PageId,
    total_len: u32,
    page_count: u32,
}

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
                let data = self.load_segment_payload(pager, tid, meta, seg_idx)?;
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
            self.store_segment_payload(pager, tid, new_generation, seg_idx, &seg_pl.serialize())?;
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
            self.delete_segment_payload(pager, tid, meta, seg_idx)?;
        }
        Ok(())
    }

    fn load_segment_payload(
        &self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        meta: SegmentMeta,
        seg_idx: u32,
    ) -> Result<Vec<u8>> {
        let key = segment_payload_key(tid, meta, seg_idx)?;
        if let Some(data) = self.btree.search(pager, &key)? {
            return Ok(data);
        }

        if let SegmentMeta::V2 { generation, .. } = meta {
            let overflow_key = seg_overflow_key_v2(tid, generation, seg_idx);
            if let Some(raw_ref) = self.btree.search(pager, &overflow_key)? {
                let overflow_ref = decode_overflow_ref(&raw_ref)?;
                return read_overflow_chain(pager, overflow_ref);
            }
        }

        Err(crate::error::MuroError::Corruption(
            "missing segmented posting payload".into(),
        ))
    }

    fn store_segment_payload(
        &mut self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        generation: u32,
        seg_idx: u32,
        payload: &[u8],
    ) -> Result<()> {
        let data_key = seg_data_key_v2(tid, generation, seg_idx);
        let overflow_key = seg_overflow_key_v2(tid, generation, seg_idx);
        if payload.len() <= MAX_SEGMENT_INLINE_BYTES {
            self.btree.insert(pager, &data_key, payload)?;
            return Ok(());
        }

        let overflow_ref = write_overflow_chain(pager, payload)?;
        self.btree
            .insert(pager, &overflow_key, &encode_overflow_ref(overflow_ref))?;
        Ok(())
    }

    fn delete_segment_payload(
        &mut self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
        meta: SegmentMeta,
        seg_idx: u32,
    ) -> Result<()> {
        let key = segment_payload_key(tid, meta, seg_idx)?;
        if self.btree.search(pager, &key)?.is_some() {
            self.btree.delete(pager, &key)?;
        }

        if let SegmentMeta::V2 { generation, .. } = meta {
            let overflow_key = seg_overflow_key_v2(tid, generation, seg_idx);
            if let Some(raw_ref) = self.btree.search(pager, &overflow_key)? {
                let overflow_ref = decode_overflow_ref(&raw_ref)?;
                free_overflow_chain(pager, overflow_ref)?;
                self.btree.delete(pager, &overflow_key)?;
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

fn seg_overflow_key_v2(tid: &[u8; 32], generation: u32, idx: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_OVERFLOW_V2_PREFIX.len() + tid.len() + 8);
    key.extend_from_slice(SEG_OVERFLOW_V2_PREFIX);
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

fn encode_overflow_ref(overflow_ref: SegmentOverflowRef) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&overflow_ref.first_page_id.to_le_bytes());
    out[8..12].copy_from_slice(&overflow_ref.total_len.to_le_bytes());
    out[12..16].copy_from_slice(&overflow_ref.page_count.to_le_bytes());
    out
}

fn decode_overflow_ref(raw: &[u8]) -> Result<SegmentOverflowRef> {
    if raw.len() != 16 {
        return Err(crate::error::MuroError::Corruption(
            "invalid overflow reference payload".into(),
        ));
    }
    let first_page_id = u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]);
    let total_len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
    let page_count = u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]);
    if page_count == 0 {
        return Err(crate::error::MuroError::Corruption(
            "overflow reference page_count must be > 0".into(),
        ));
    }
    Ok(SegmentOverflowRef {
        first_page_id,
        total_len,
        page_count,
    })
}

fn write_overflow_chain(pager: &mut impl PageStore, payload: &[u8]) -> Result<SegmentOverflowRef> {
    if payload.is_empty() {
        return Err(crate::error::MuroError::Execution(
            "overflow payload cannot be empty".into(),
        ));
    }
    let page_count_usize = payload.len().div_ceil(OVERFLOW_PAGE_CHUNK_BYTES);
    let page_count = u32::try_from(page_count_usize)
        .map_err(|_| crate::error::MuroError::Execution("overflow chain too long".into()))?;
    let total_len = u32::try_from(payload.len())
        .map_err(|_| crate::error::MuroError::Execution("overflow payload too large".into()))?;

    let mut page_ids = Vec::with_capacity(page_count_usize);
    for _ in 0..page_count_usize {
        let page = pager.allocate_page()?;
        page_ids.push(page.page_id());
    }

    for (i, &page_id) in page_ids.iter().enumerate() {
        let next_page_id = page_ids.get(i + 1).copied().unwrap_or(0);
        let mut page = Page::new(page_id);
        let start = i * OVERFLOW_PAGE_CHUNK_BYTES;
        let end = std::cmp::min(start + OVERFLOW_PAGE_CHUNK_BYTES, payload.len());
        let chunk = &payload[start..end];
        let chunk_len = u16::try_from(chunk.len())
            .map_err(|_| crate::error::MuroError::Execution("overflow chunk too large".into()))?;

        let base = PAGE_HEADER_SIZE;
        page.data[base..base + 4].copy_from_slice(OVERFLOW_PAGE_MAGIC);
        page.data[base + 4..base + 12].copy_from_slice(&next_page_id.to_le_bytes());
        page.data[base + 12..base + 14].copy_from_slice(&chunk_len.to_le_bytes());
        page.data[base + 14..base + 14 + chunk.len()].copy_from_slice(chunk);
        pager.write_page(&page)?;
    }

    Ok(SegmentOverflowRef {
        first_page_id: page_ids[0],
        total_len,
        page_count,
    })
}

fn read_overflow_chain(
    pager: &mut impl PageStore,
    overflow_ref: SegmentOverflowRef,
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(overflow_ref.total_len as usize);
    let mut visited = std::collections::HashSet::new();
    let mut current = overflow_ref.first_page_id;
    let mut read_pages = 0u32;

    while current != 0 {
        if !visited.insert(current) {
            return Err(crate::error::MuroError::Corruption(
                "overflow chain cycle detected".into(),
            ));
        }
        let page = pager.read_page(current)?;
        let base = PAGE_HEADER_SIZE;
        if &page.data[base..base + 4] != OVERFLOW_PAGE_MAGIC {
            return Err(crate::error::MuroError::Corruption(
                "invalid overflow page magic".into(),
            ));
        }
        let next_page_id = u64::from_le_bytes([
            page.data[base + 4],
            page.data[base + 5],
            page.data[base + 6],
            page.data[base + 7],
            page.data[base + 8],
            page.data[base + 9],
            page.data[base + 10],
            page.data[base + 11],
        ]);
        let chunk_len = u16::from_le_bytes([page.data[base + 12], page.data[base + 13]]) as usize;
        if chunk_len > OVERFLOW_PAGE_CHUNK_BYTES {
            return Err(crate::error::MuroError::Corruption(
                "overflow chunk length exceeds page capacity".into(),
            ));
        }
        out.extend_from_slice(&page.data[base + 14..base + 14 + chunk_len]);
        read_pages = read_pages.saturating_add(1);
        current = next_page_id;
    }

    if read_pages != overflow_ref.page_count || out.len() != overflow_ref.total_len as usize {
        return Err(crate::error::MuroError::Corruption(
            "overflow chain length mismatch".into(),
        ));
    }
    Ok(out)
}

fn free_overflow_chain(pager: &mut impl PageStore, overflow_ref: SegmentOverflowRef) -> Result<()> {
    let mut visited = std::collections::HashSet::new();
    let mut current = overflow_ref.first_page_id;
    let mut freed_pages = 0u32;

    while current != 0 {
        if !visited.insert(current) {
            return Err(crate::error::MuroError::Corruption(
                "overflow chain cycle detected while free".into(),
            ));
        }
        let page = pager.read_page(current)?;
        let base = PAGE_HEADER_SIZE;
        if &page.data[base..base + 4] != OVERFLOW_PAGE_MAGIC {
            return Err(crate::error::MuroError::Corruption(
                "invalid overflow page magic while free".into(),
            ));
        }
        let next_page_id = u64::from_le_bytes([
            page.data[base + 4],
            page.data[base + 5],
            page.data[base + 6],
            page.data[base + 7],
            page.data[base + 8],
            page.data[base + 9],
            page.data[base + 10],
            page.data[base + 11],
        ]);
        pager.free_page(current);
        freed_pages = freed_pages.saturating_add(1);
        current = next_page_id;
    }

    if freed_pages != overflow_ref.page_count {
        return Err(crate::error::MuroError::Corruption(
            "overflow chain page_count mismatch while free".into(),
        ));
    }
    Ok(())
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
mod tests;
