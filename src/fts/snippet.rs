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
    let offsets = CharOffsetMap::new(text);
    let char_start = offsets.byte_to_char_index(match_start);
    let char_end = offsets.byte_to_char_index(match_start.saturating_add(match_len));

    let snippet_start = char_start.saturating_sub(context_chars);
    let snippet_end = (char_end + context_chars).min(offsets.char_len());
    let snippet_start_b = offsets.char_to_byte(snippet_start);
    let char_start_b = offsets.char_to_byte(char_start);
    let char_end_b = offsets.char_to_byte(char_end);
    let snippet_end_b = offsets.char_to_byte(snippet_end);

    let mut result = String::new();

    if snippet_start > 0 {
        result.push_str("...");
    }

    // Before match
    result.push_str(&text[snippet_start_b..char_start_b]);

    // Match with tags
    result.push_str(pre_tag);
    result.push_str(&text[char_start_b..char_end_b]);
    result.push_str(post_tag);

    // After match
    result.push_str(&text[char_end_b..snippet_end_b]);

    if snippet_end < offsets.char_len() {
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
    let offsets = CharOffsetMap::new(text);
    if offsets.char_len() <= max_chars {
        text.to_string()
    } else {
        let cut = offsets.char_to_byte(max_chars);
        let truncated = &text[..cut];
        format!("{}...", truncated)
    }
}

/// Maps UTF-8 byte offsets to character offsets and back.
struct CharOffsetMap {
    /// Byte offset for each char index; includes trailing `text.len()` sentinel.
    char_to_byte: Vec<usize>,
}

impl CharOffsetMap {
    fn new(text: &str) -> Self {
        let mut char_to_byte = Vec::with_capacity(text.len() + 1);
        for (b, _) in text.char_indices() {
            char_to_byte.push(b);
        }
        char_to_byte.push(text.len());
        Self { char_to_byte }
    }

    fn char_len(&self) -> usize {
        self.char_to_byte.len().saturating_sub(1)
    }

    fn char_to_byte(&self, char_idx: usize) -> usize {
        let idx = char_idx.min(self.char_len());
        self.char_to_byte[idx]
    }

    fn byte_to_char_index(&self, byte_offset: usize) -> usize {
        let b = byte_offset.min(self.char_to_byte[self.char_len()]);
        match self.char_to_byte.binary_search(&b) {
            Ok(i) => i.min(self.char_len()),
            Err(i) => i.saturating_sub(1).min(self.char_len()),
        }
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

    #[test]
    fn test_snippet_handles_multibyte_boundary_without_splitting() {
        let text = "a😀b😀c";
        let snippet = fts_snippet(text, "😀b😀", "<b>", "</b>", 1);
        assert!(snippet.contains("<b>😀b😀</b>"));
    }
}
