/// BM25 scoring for FTS.
///
/// BM25(D, Q) = Σ IDF(qi) · (f(qi, D) · (k1 + 1)) / (f(qi, D) + k1 · (1 - b + b · |D| / avgdl))

const K1: f64 = 1.2;
const B: f64 = 0.75;

/// Calculate IDF for a term.
/// IDF(q) = ln((N - n(q) + 0.5) / (n(q) + 0.5) + 1)
pub fn idf(total_docs: u64, doc_freq: u64) -> f64 {
    let n = total_docs as f64;
    let nq = doc_freq as f64;
    ((n - nq + 0.5) / (nq + 0.5) + 1.0).ln()
}

/// Calculate BM25 score for a document.
///
/// - term_freqs: frequency of each query term in the document
/// - doc_len: total token count in the document
/// - avg_doc_len: average document length across all documents
/// - total_docs: total number of documents
/// - doc_freqs: document frequency for each query term
pub fn bm25_score(
    term_freqs: &[u32],
    doc_len: u32,
    avg_doc_len: f64,
    total_docs: u64,
    doc_freqs: &[u64],
) -> f64 {
    let dl = doc_len as f64;
    let mut score = 0.0;

    for (i, &tf) in term_freqs.iter().enumerate() {
        if tf == 0 || i >= doc_freqs.len() {
            continue;
        }

        let tf_f = tf as f64;
        let term_idf = idf(total_docs, doc_freqs[i]);
        let numerator = tf_f * (K1 + 1.0);
        let denominator = tf_f + K1 * (1.0 - B + B * dl / avg_doc_len);
        score += term_idf * numerator / denominator;
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idf() {
        let val = idf(100, 10);
        assert!(val > 0.0);
        // Rare terms should have higher IDF
        assert!(idf(100, 1) > idf(100, 50));
    }

    #[test]
    fn test_bm25_higher_tf_higher_score() {
        let score_low = bm25_score(&[1], 100, 100.0, 1000, &[10]);
        let score_high = bm25_score(&[5], 100, 100.0, 1000, &[10]);
        assert!(score_high > score_low);
    }

    #[test]
    fn test_bm25_rarer_term_higher_score() {
        let score_common = bm25_score(&[3], 100, 100.0, 1000, &[500]);
        let score_rare = bm25_score(&[3], 100, 100.0, 1000, &[5]);
        assert!(score_rare > score_common);
    }
}
