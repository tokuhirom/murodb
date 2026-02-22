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
use crate::fts::postings::PostingList;
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
const SEG_DATA_PREFIX: &[u8] = b"__segdata__";
const MAX_POSTINGS_PER_SEGMENT: usize = 16;

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

    fn load_postings_by_tid(
        &self,
        pager: &mut impl PageStore,
        tid: &[u8; 32],
    ) -> Result<PostingList> {
        if let Some(meta) = self.btree.search(pager, &seg_meta_key(tid))? {
            if meta.len() != 2 {
                return Err(crate::error::MuroError::Corruption(
                    "invalid segmented posting metadata".into(),
                ));
            }
            let seg_count = u16::from_le_bytes([meta[0], meta[1]]);
            let mut merged = PostingList::new();
            for seg_idx in 0..seg_count {
                let key = seg_data_key(tid, seg_idx);
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
        self.delete_postings_by_tid(pager, tid)?;
        if pl.df() == 0 {
            return Ok(());
        }

        let seg_count = pl.postings.len().div_ceil(MAX_POSTINGS_PER_SEGMENT) as u16;
        for (idx, chunk) in pl.postings.chunks(MAX_POSTINGS_PER_SEGMENT).enumerate() {
            let seg_pl = PostingList {
                postings: chunk.to_vec(),
            };
            let key = seg_data_key(tid, idx as u16);
            self.btree.insert(pager, &key, &seg_pl.serialize())?;
        }
        self.btree
            .insert(pager, &seg_meta_key(tid), &seg_count.to_le_bytes())?;
        Ok(())
    }

    fn delete_postings_by_tid(&mut self, pager: &mut impl PageStore, tid: &[u8; 32]) -> Result<()> {
        if let Some(meta) = self.btree.search(pager, &seg_meta_key(tid))? {
            if meta.len() != 2 {
                return Err(crate::error::MuroError::Corruption(
                    "invalid segmented posting metadata".into(),
                ));
            }
            let seg_count = u16::from_le_bytes([meta[0], meta[1]]);
            for seg_idx in 0..seg_count {
                let key = seg_data_key(tid, seg_idx);
                self.btree.delete(pager, &key)?;
            }
            self.btree.delete(pager, &seg_meta_key(tid))?;
        }
        if self.btree.search(pager, tid)?.is_some() {
            self.btree.delete(pager, tid)?;
        }
        Ok(())
    }
}

fn seg_meta_key(tid: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_META_PREFIX.len() + tid.len());
    key.extend_from_slice(SEG_META_PREFIX);
    key.extend_from_slice(tid);
    key
}

fn seg_data_key(tid: &[u8; 32], idx: u16) -> Vec<u8> {
    let mut key = Vec::with_capacity(SEG_DATA_PREFIX.len() + tid.len() + 2);
    key.extend_from_slice(SEG_DATA_PREFIX);
    key.extend_from_slice(tid);
    key.extend_from_slice(&idx.to_le_bytes());
    key
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
}
