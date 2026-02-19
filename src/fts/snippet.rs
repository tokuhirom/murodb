/// FTS snippet: extract text around match positions with highlighting.
/// Option B: local scan approach (MVP).
use unicode_normalization::UnicodeNormalization;

/// Generate a snippet from text with highlighted matches.
///
/// - text: the full document text
/// - query: the search query
/// - pre_tag: tag before matched text (e.g., "<mark>")
/// - post_tag: tag after matched text (e.g., "</mark>")
/// - context_chars: number of characters to show around the match
pub fn fts_snippet(
    text: &str,
    query: &str,
    pre_tag: &str,
    post_tag: &str,
    context_chars: usize,
) -> String {
    let normalized_text: String = text.nfkc().collect();
    let normalized_query: String = query.nfkc().collect();

    // Remove boolean operators and quotes from query
    let clean_query = clean_query_string(&normalized_query);

    if clean_query.is_empty() {
        return truncate_text(&normalized_text, context_chars * 2);
    }

    // Find the first occurrence of the query text in the document
    if let Some(pos) = normalized_text.find(&clean_query) {
        return build_snippet(
            &normalized_text,
            pos,
            clean_query.len(),
            pre_tag,
            post_tag,
            context_chars,
        );
    }

    // If full query not found, try to find individual bigrams
    let chars: Vec<char> = clean_query.chars().collect();
    if chars.len() >= 2 {
        let first_bigram: String = chars[..2].iter().collect();
        if let Some(pos) = normalized_text.find(&first_bigram) {
            // Try to find the longest matching substring
            let mut match_len = first_bigram.len();
            for end in (3..=chars.len()).rev() {
                let substr: String = chars[..end].iter().collect();
                if normalized_text[pos..].starts_with(&substr) {
                    match_len = substr.len();
                    break;
                }
            }
            return build_snippet(
                &normalized_text,
                pos,
                match_len,
                pre_tag,
                post_tag,
                context_chars,
            );
        }
    }

    // No match found, return beginning of text
    truncate_text(&normalized_text, context_chars * 2)
}

fn build_snippet(
    text: &str,
    match_start: usize,
    match_len: usize,
    pre_tag: &str,
    post_tag: &str,
    context_chars: usize,
) -> String {
    let text_chars: Vec<char> = text.chars().collect();

    // Convert byte offsets to char offsets
    let mut char_start = 0;
    let mut byte_count = 0;
    for (i, ch) in text_chars.iter().enumerate() {
        if byte_count >= match_start {
            char_start = i;
            break;
        }
        byte_count += ch.len_utf8();
    }

    let mut char_end = char_start;
    byte_count = 0;
    for (i, ch) in text_chars[char_start..].iter().enumerate() {
        byte_count += ch.len_utf8();
        if byte_count >= match_len {
            char_end = char_start + i + 1;
            break;
        }
    }

    let snippet_start = char_start.saturating_sub(context_chars);
    let snippet_end = (char_end + context_chars).min(text_chars.len());

    let mut result = String::new();

    if snippet_start > 0 {
        result.push_str("...");
    }

    // Before match
    let before: String = text_chars[snippet_start..char_start].iter().collect();
    result.push_str(&before);

    // Match with tags
    result.push_str(pre_tag);
    let matched: String = text_chars[char_start..char_end].iter().collect();
    result.push_str(&matched);
    result.push_str(post_tag);

    // After match
    let after: String = text_chars[char_end..snippet_end].iter().collect();
    result.push_str(&after);

    if snippet_end < text_chars.len() {
        result.push_str("...");
    }

    result
}

fn clean_query_string(query: &str) -> String {
    let mut result = String::new();
    let mut in_quote = false;
    let mut at_term_start = true;

    for ch in query.chars() {
        match ch {
            '"' => {
                in_quote = !in_quote;
            }
            '+' | '-' if !in_quote && at_term_start => {
                // Skip boolean operators at the start of terms
            }
            ' ' => {
                if !result.is_empty() && !result.ends_with(' ') {
                    result.push(' ');
                }
                at_term_start = true;
            }
            _ => {
                result.push(ch);
                at_term_start = false;
            }
        }
    }

    result.trim().to_string()
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        text.to_string()
    } else {
        let truncated: String = chars[..max_chars].iter().collect();
        format!("{}...", truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snippet_basic() {
        let text = "東京タワーは東京の有名な観光スポットです";
        let snippet = fts_snippet(text, "東京タワー", "<b>", "</b>", 5);
        assert!(snippet.contains("<b>東京タワー</b>"));
    }

    #[test]
    fn test_snippet_with_context() {
        let text = "今日は天気がいいので東京タワーに行きました。とても楽しかったです。";
        let snippet = fts_snippet(text, "東京タワー", "<mark>", "</mark>", 5);
        assert!(snippet.contains("<mark>東京タワー</mark>"));
        assert!(snippet.contains("...")); // should have ellipsis since match is in middle
    }

    #[test]
    fn test_snippet_boolean_query() {
        let text = "東京タワーの夜景が綺麗です";
        let snippet = fts_snippet(text, "\"東京タワー\"", "<b>", "</b>", 10);
        assert!(snippet.contains("<b>東京タワー</b>"));
    }

    #[test]
    fn test_snippet_no_match() {
        let text = "大阪城が立派です";
        let snippet = fts_snippet(text, "東京タワー", "<b>", "</b>", 10);
        // Should return beginning of text
        assert!(snippet.contains("大阪"));
    }

    #[test]
    fn test_clean_query() {
        assert_eq!(clean_query_string("\"東京タワー\""), "東京タワー");
        assert_eq!(clean_query_string("+東京 -混雑"), "東京 混雑");
    }
}
