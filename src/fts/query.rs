/// FTS query evaluation: NATURAL and BOOLEAN mode.
///
/// NATURAL: bigram tokenize query → look up postings → BM25 score
/// BOOLEAN: parse +term, -term, "phrase" → evaluate constraints
use std::collections::HashSet;

use crate::error::Result;
use crate::fts::index::FtsIndex;
use crate::fts::postings::PostingList;
use crate::fts::scoring::bm25_score;
use crate::fts::tokenizer::tokenize_bigram;
use crate::storage::page_store::PageStore;

/// FTS search result for a single document.
#[derive(Debug, Clone)]
pub struct FtsResult {
    pub doc_id: u64,
    pub score: f64,
}

/// Execute a NATURAL LANGUAGE MODE query.
pub fn query_natural(
    fts_index: &FtsIndex,
    pager: &mut impl PageStore,
    query: &str,
) -> Result<Vec<FtsResult>> {
    let query_tokens = tokenize_bigram(query);
    if query_tokens.is_empty() {
        return Ok(Vec::new());
    }

    let stats = fts_index.get_stats(pager)?;
    let avg_doc_len = stats.avg_doc_len();

    // Get posting lists for each query term
    let mut term_postings: Vec<(String, PostingList)> = Vec::new();
    let mut seen_terms: HashSet<String> = HashSet::new();

    for token in &query_tokens {
        if seen_terms.insert(token.text.clone()) {
            let pl = fts_index.get_postings(pager, &token.text)?;
            term_postings.push((token.text.clone(), pl));
        }
    }

    // Collect all matching doc_ids
    let mut doc_ids: HashSet<u64> = HashSet::new();
    for (_, pl) in &term_postings {
        for posting in &pl.postings {
            doc_ids.insert(posting.doc_id);
        }
    }

    // Score each document
    let mut results: Vec<FtsResult> = Vec::new();
    let doc_freqs: Vec<u64> = term_postings.iter().map(|(_, pl)| pl.df() as u64).collect();

    for doc_id in &doc_ids {
        let term_freqs: Vec<u32> = term_postings
            .iter()
            .map(|(_, pl)| {
                pl.get(*doc_id)
                    .map(|p| p.positions.len() as u32)
                    .unwrap_or(0)
            })
            .collect();

        // Approximate doc_len as sum of all term frequencies
        let doc_len: u32 = term_freqs.iter().sum::<u32>().max(1);

        let score = bm25_score(
            &term_freqs,
            doc_len,
            avg_doc_len.max(1.0),
            stats.total_docs,
            &doc_freqs,
        );

        if score > 0.0 {
            results.push(FtsResult {
                doc_id: *doc_id,
                score,
            });
        }
    }

    // Sort by score descending
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(results)
}

/// Parsed boolean query element.
#[derive(Debug)]
enum BooleanTerm {
    Must(String),    // +term
    MustNot(String), // -term
    Phrase(String),  // "..."
    Should(String),  // plain term (implicit AND in MVP)
}

/// Execute a BOOLEAN MODE query.
pub fn query_boolean(
    fts_index: &FtsIndex,
    pager: &mut impl PageStore,
    query: &str,
) -> Result<Vec<FtsResult>> {
    let terms = parse_boolean_query(query);

    if terms.is_empty() {
        return Ok(Vec::new());
    }

    // Collect candidate doc_ids from all positive terms
    let mut must_docs: Option<HashSet<u64>> = None;
    let mut must_not_docs: HashSet<u64> = HashSet::new();
    let mut should_docs: HashSet<u64> = HashSet::new();

    for term in &terms {
        match term {
            BooleanTerm::Must(t) | BooleanTerm::Should(t) => {
                let bigrams = tokenize_bigram(t);
                let mut term_docs: HashSet<u64> = HashSet::new();
                for bg in &bigrams {
                    let pl = fts_index.get_postings(pager, &bg.text)?;
                    for posting in &pl.postings {
                        term_docs.insert(posting.doc_id);
                    }
                }
                if matches!(term, BooleanTerm::Must(_)) {
                    must_docs = Some(match must_docs {
                        Some(existing) => existing.intersection(&term_docs).cloned().collect(),
                        None => term_docs,
                    });
                } else {
                    should_docs.extend(term_docs);
                }
            }
            BooleanTerm::MustNot(t) => {
                let bigrams = tokenize_bigram(t);
                for bg in &bigrams {
                    let pl = fts_index.get_postings(pager, &bg.text)?;
                    for posting in &pl.postings {
                        must_not_docs.insert(posting.doc_id);
                    }
                }
            }
            BooleanTerm::Phrase(phrase) => {
                let matching = find_phrase_matches(fts_index, pager, phrase)?;
                let phrase_set: HashSet<u64> = matching.into_iter().collect();
                must_docs = Some(match must_docs {
                    Some(existing) => existing.intersection(&phrase_set).cloned().collect(),
                    None => phrase_set,
                });
            }
        }
    }

    // Combine: start with must_docs or should_docs
    let candidate_docs: HashSet<u64> = match must_docs {
        Some(m) => m,
        None => should_docs,
    };

    // Remove must_not_docs
    let final_docs: Vec<u64> = candidate_docs
        .into_iter()
        .filter(|d| !must_not_docs.contains(d))
        .collect();

    // For boolean mode, score is less important; use simple TF-based scoring
    let results: Vec<FtsResult> = final_docs
        .into_iter()
        .map(|doc_id| FtsResult { doc_id, score: 1.0 })
        .collect();

    Ok(results)
}

/// Parse a boolean query string into terms.
fn parse_boolean_query(query: &str) -> Vec<BooleanTerm> {
    let mut terms = Vec::new();
    let mut chars = query.chars().peekable();
    let mut current = String::new();
    let mut modifier: Option<char> = None;

    while let Some(&ch) = chars.peek() {
        match ch {
            '+' | '-' if current.is_empty() => {
                modifier = Some(ch);
                chars.next();
            }
            '"' => {
                chars.next();
                let mut phrase = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '"' {
                        chars.next();
                        break;
                    }
                    phrase.push(c);
                    chars.next();
                }
                if !phrase.is_empty() {
                    terms.push(BooleanTerm::Phrase(phrase));
                }
                modifier = None;
            }
            ' ' => {
                if !current.is_empty() {
                    let term = std::mem::take(&mut current);
                    match modifier {
                        Some('+') => terms.push(BooleanTerm::Must(term)),
                        Some('-') => terms.push(BooleanTerm::MustNot(term)),
                        _ => terms.push(BooleanTerm::Should(term)),
                    }
                    modifier = None;
                }
                chars.next();
            }
            _ => {
                current.push(ch);
                chars.next();
            }
        }
    }

    if !current.is_empty() {
        match modifier {
            Some('+') => terms.push(BooleanTerm::Must(current)),
            Some('-') => terms.push(BooleanTerm::MustNot(current)),
            _ => terms.push(BooleanTerm::Should(current)),
        }
    }

    terms
}

/// Find documents matching a phrase (consecutive bigrams).
fn find_phrase_matches(
    fts_index: &FtsIndex,
    pager: &mut impl PageStore,
    phrase: &str,
) -> Result<Vec<u64>> {
    let bigrams = tokenize_bigram(phrase);
    if bigrams.is_empty() {
        return Ok(Vec::new());
    }

    // Get posting lists for all bigrams
    let mut postings: Vec<PostingList> = Vec::new();
    for bg in &bigrams {
        let pl = fts_index.get_postings(pager, &bg.text)?;
        postings.push(pl);
    }

    // Find documents present in all posting lists
    if postings.is_empty() {
        return Ok(Vec::new());
    }

    let first_docs: HashSet<u64> = postings[0].postings.iter().map(|p| p.doc_id).collect();

    let mut candidate_docs: HashSet<u64> = first_docs;
    for pl in &postings[1..] {
        let docs: HashSet<u64> = pl.postings.iter().map(|p| p.doc_id).collect();
        candidate_docs = candidate_docs.intersection(&docs).cloned().collect();
    }

    // For each candidate, verify phrase by checking consecutive positions
    let mut matching = Vec::new();

    for doc_id in &candidate_docs {
        // Get positions for each bigram in this document
        let positions: Vec<Vec<u32>> = postings
            .iter()
            .map(|pl| {
                pl.get(*doc_id)
                    .map(|p| p.positions.clone())
                    .unwrap_or_default()
            })
            .collect();

        // Check if there exist positions where bigram_i appears at pos, bigram_{i+1} at pos+1, etc.
        if check_consecutive_positions(&positions) {
            matching.push(*doc_id);
        }
    }

    Ok(matching)
}

/// Check if there's a sequence of consecutive positions across the position arrays.
fn check_consecutive_positions(positions: &[Vec<u32>]) -> bool {
    if positions.is_empty() {
        return false;
    }
    if positions.len() == 1 {
        return !positions[0].is_empty();
    }

    // For each position in the first bigram, check if consecutive positions exist
    for &start_pos in &positions[0] {
        let mut found = true;
        for (i, pos_list) in positions.iter().enumerate().skip(1) {
            let expected = start_pos + i as u32;
            if !pos_list.contains(&expected) {
                found = false;
                break;
            }
        }
        if found {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aead::MasterKey;
    use crate::fts::index::FtsPendingOp;
    use crate::storage::pager::Pager;
    use tempfile::TempDir;

    fn test_key() -> MasterKey {
        MasterKey::new([0x42u8; 32])
    }

    fn term_key() -> [u8; 32] {
        [0x55u8; 32]
    }

    fn setup_index(docs: &[(u64, &str)]) -> (Pager, FtsIndex, TempDir) {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("test.db");
        let mut pager = Pager::create(&db_path, &test_key()).unwrap();
        let mut idx = FtsIndex::create(&mut pager, term_key()).unwrap();

        let ops: Vec<FtsPendingOp> = docs
            .iter()
            .map(|(id, text)| FtsPendingOp::Add {
                doc_id: *id,
                text: text.to_string(),
            })
            .collect();
        idx.apply_pending(&mut pager, &ops).unwrap();

        (pager, idx, dir)
    }

    #[test]
    fn test_natural_query() {
        let (mut pager, idx, _dir) = setup_index(&[
            (1, "東京タワーは東京の名所です"),
            (2, "京都の寺院が美しい"),
            (3, "東京スカイツリーも人気"),
        ]);

        let results = query_natural(&idx, &mut pager, "東京タワー").unwrap();
        assert!(!results.is_empty());
        // Doc 1 should rank highest (contains both 東京 and タワー)
        assert_eq!(results[0].doc_id, 1);
    }

    #[test]
    fn test_boolean_must_not() {
        let (mut pager, idx, _dir) = setup_index(&[
            (1, "東京タワーの夜景"),
            (2, "東京の混雑した街"),
            (3, "大阪の夜景が綺麗"),
        ]);

        let results = query_boolean(&idx, &mut pager, "+東京 -混雑").unwrap();
        let doc_ids: HashSet<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(doc_ids.contains(&1));
        assert!(!doc_ids.contains(&2)); // excluded by -混雑
    }

    #[test]
    fn test_phrase_search() {
        let (mut pager, idx, _dir) = setup_index(&[
            (1, "東京タワーは有名"),
            (2, "タワー東京ではない"), // "東京タワー" is NOT a substring here
            (3, "東京タワーに行きたい"),
        ]);

        let results = query_boolean(&idx, &mut pager, "\"東京タワー\"").unwrap();
        let doc_ids: HashSet<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(doc_ids.contains(&1));
        assert!(doc_ids.contains(&3));
        // Doc 2 has the bigrams but not in consecutive order for "東京タワー"
    }

    #[test]
    fn test_empty_query() {
        let (mut pager, idx, _dir) = setup_index(&[(1, "test")]);
        let results = query_natural(&idx, &mut pager, "").unwrap();
        assert!(results.is_empty());
    }
}
