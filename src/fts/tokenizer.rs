/// FTS tokenizer: NFKC normalization + bigram (n=2).
///
/// Input: "東京タワー" → ["東京", "京タ", "タワ", "ワー"]
use unicode_normalization::UnicodeNormalization;

/// Token with its position in the document.
#[derive(Debug, Clone, PartialEq)]
pub struct FtsToken {
    pub text: String,
    pub position: usize,
    pub byte_offset: usize,
}

/// Tokenize text into bigrams after NFKC normalization.
pub fn tokenize_bigram(text: &str) -> Vec<FtsToken> {
    // NFKC normalize
    let normalized: String = text.nfkc().collect();

    let chars: Vec<char> = normalized.chars().collect();
    if chars.len() < 2 {
        return Vec::new();
    }

    let mut tokens = Vec::new();
    let mut byte_offset = 0;

    for (i, window) in chars.windows(2).enumerate() {
        let bigram: String = window.iter().collect();
        tokens.push(FtsToken {
            text: bigram,
            position: i,
            byte_offset,
        });
        byte_offset += window[0].len_utf8();
    }

    tokens
}

/// Tokenize a query string, handling bigrams from the normalized text.
/// Same as document tokenization for NATURAL mode.
pub fn tokenize_query(query: &str) -> Vec<String> {
    let tokens = tokenize_bigram(query);
    tokens.into_iter().map(|t| t.text).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bigram_japanese() {
        let tokens = tokenize_bigram("東京タワー");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["東京", "京タ", "タワ", "ワー"]);
    }

    #[test]
    fn test_bigram_positions() {
        let tokens = tokenize_bigram("東京タワー");
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[1].position, 1);
        assert_eq!(tokens[2].position, 2);
        assert_eq!(tokens[3].position, 3);
    }

    #[test]
    fn test_nfkc_normalization() {
        // Fullwidth characters should be normalized
        let tokens = tokenize_bigram("ＡＢＣ");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["AB", "BC"]);
    }

    #[test]
    fn test_short_text() {
        assert!(tokenize_bigram("a").is_empty());
        assert!(tokenize_bigram("").is_empty());
    }

    #[test]
    fn test_mixed_text() {
        let tokens = tokenize_bigram("日本語abc");
        assert_eq!(tokens.len(), 5); // 日本, 本語, 語a, ab, bc
    }

    #[test]
    fn test_query_tokenize() {
        let tokens = tokenize_query("東京タワー");
        assert_eq!(tokens, vec!["東京", "京タ", "タワ", "ワー"]);
    }
}
