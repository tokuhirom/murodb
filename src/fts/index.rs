/// FTS index: commit-time update with pending buffer.
///
/// Uses a B-tree to store postings:
///   key = term_id (HMAC-SHA256 of bigram)
///   value = serialized PostingList
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
use crate::storage::pager::Pager;

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

/// Pending FTS operation (accumulated during a transaction).
#[derive(Debug, Clone)]
pub enum FtsPendingOp {
    Add { doc_id: u64, text: String },
    Remove { doc_id: u64, text: String },
}

impl FtsIndex {
    /// Create a new FTS index.
    pub fn create(pager: &mut Pager, term_key: [u8; 32]) -> Result<Self> {
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
    pub fn get_postings(&self, pager: &mut Pager, term: &str) -> Result<PostingList> {
        let tid = self.term_id(term);
        match self.btree.search(pager, &tid)? {
            Some(data) => Ok(PostingList::deserialize(&data).unwrap_or_default()),
            None => Ok(PostingList::new()),
        }
    }

    /// Get FTS statistics.
    pub fn get_stats(&self, pager: &mut Pager) -> Result<FtsStats> {
        match self.btree.search(pager, STATS_KEY)? {
            Some(data) => Ok(FtsStats::deserialize(&data).unwrap_or_default()),
            None => Ok(FtsStats::default()),
        }
    }

    /// Apply pending operations at commit time.
    pub fn apply_pending(
        &mut self,
        pager: &mut Pager,
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
                        let mut pl = match self.btree.search(pager, &tid)? {
                            Some(data) => PostingList::deserialize(&data).unwrap_or_default(),
                            None => PostingList::new(),
                        };
                        pl.add(*doc_id, positions.clone());
                        self.btree.insert(pager, &tid, &pl.serialize())?;
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
                            if let Some(data) = self.btree.search(pager, &tid)? {
                                let mut pl =
                                    PostingList::deserialize(&data).unwrap_or_default();
                                pl.remove(*doc_id);
                                if pl.df() == 0 {
                                    self.btree.delete(pager, &tid)?;
                                } else {
                                    self.btree.insert(pager, &tid, &pl.serialize())?;
                                }
                            }
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
        pager: &mut Pager,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
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

        idx.apply_pending(&mut pager, &[FtsPendingOp::Add {
            doc_id: 1,
            text: "東京タワー".to_string(),
        }]).unwrap();

        idx.apply_pending(&mut pager, &[FtsPendingOp::Remove {
            doc_id: 1,
            text: "東京タワー".to_string(),
        }]).unwrap();

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
}
