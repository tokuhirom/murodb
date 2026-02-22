use std::time::{Duration, Instant};

use murodb::fts::snippet::fts_snippet;
use unicode_normalization::UnicodeNormalization;

#[derive(Clone, Copy)]
struct Stat {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    total_ms: f64,
}

fn percentile_us(samples_ns: &[u128], num: usize, den: usize) -> f64 {
    if samples_ns.is_empty() {
        return 0.0;
    }
    let mut sorted = samples_ns.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() - 1) * num) / den;
    sorted[idx] as f64 / 1_000.0
}

fn measure<F>(iters: usize, mut f: F) -> Stat
where
    F: FnMut() -> usize,
{
    let mut latencies = Vec::with_capacity(iters);
    let start = Instant::now();
    let mut blackhole = 0usize;
    for _ in 0..iters {
        let t0 = Instant::now();
        blackhole ^= f();
        latencies.push(t0.elapsed().as_nanos());
    }
    std::hint::black_box(blackhole);
    let elapsed = start.elapsed();
    Stat {
        p50_us: percentile_us(&latencies, 50, 100),
        p95_us: percentile_us(&latencies, 95, 100),
        p99_us: percentile_us(&latencies, 99, 100),
        total_ms: elapsed.as_secs_f64() * 1_000.0,
    }
}

fn clean_query_string(query: &str) -> String {
    let mut result = String::new();
    let mut in_quote = false;
    let mut at_term_start = true;

    for ch in query.chars() {
        match ch {
            '"' => in_quote = !in_quote,
            '+' | '-' if !in_quote && at_term_start => {}
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

fn truncate_text_legacy(text: &str, max_chars: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max_chars {
        text.to_string()
    } else {
        let truncated: String = chars[..max_chars].iter().collect();
        format!("{}...", truncated)
    }
}

fn build_snippet_legacy(
    text: &str,
    match_start: usize,
    match_len: usize,
    pre_tag: &str,
    post_tag: &str,
    context_chars: usize,
) -> String {
    let text_chars: Vec<char> = text.chars().collect();

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

    let before: String = text_chars[snippet_start..char_start].iter().collect();
    result.push_str(&before);

    result.push_str(pre_tag);
    let matched: String = text_chars[char_start..char_end].iter().collect();
    result.push_str(&matched);
    result.push_str(post_tag);

    let after: String = text_chars[char_end..snippet_end].iter().collect();
    result.push_str(&after);

    if snippet_end < text_chars.len() {
        result.push_str("...");
    }
    result
}

fn fts_snippet_legacy(
    text: &str,
    query: &str,
    pre_tag: &str,
    post_tag: &str,
    context_chars: usize,
) -> String {
    let normalized_text: String = text.nfkc().collect();
    let normalized_query: String = query.nfkc().collect();
    let clean_query = clean_query_string(&normalized_query);
    if clean_query.is_empty() {
        return truncate_text_legacy(&normalized_text, context_chars * 2);
    }
    if let Some(pos) = normalized_text.find(&clean_query) {
        return build_snippet_legacy(
            &normalized_text,
            pos,
            clean_query.len(),
            pre_tag,
            post_tag,
            context_chars,
        );
    }
    let chars: Vec<char> = clean_query.chars().collect();
    if chars.len() >= 2 {
        let first_bigram: String = chars[..2].iter().collect();
        if let Some(pos) = normalized_text.find(&first_bigram) {
            let mut match_len = first_bigram.len();
            for end in (3..=chars.len()).rev() {
                let substr: String = chars[..end].iter().collect();
                if normalized_text[pos..].starts_with(&substr) {
                    match_len = substr.len();
                    break;
                }
            }
            return build_snippet_legacy(
                &normalized_text,
                pos,
                match_len,
                pre_tag,
                post_tag,
                context_chars,
            );
        }
    }
    truncate_text_legacy(&normalized_text, context_chars * 2)
}

fn build_doc(repeat: usize) -> String {
    let chunk = "東京タワーの夜景は本当に美しいです。京都の寺院も素晴らしい。";
    let mut s = chunk.repeat(repeat);
    s.push_str(" 終端キーワード_スニペット計測");
    s
}

fn bench_case(name: &str, text: &str, query: &str, context: usize, iters: usize) {
    let new_stat = measure(iters, || {
        fts_snippet(text, query, "<mark>", "</mark>", context).len()
    });
    let old_stat = measure(iters, || {
        fts_snippet_legacy(text, query, "<mark>", "</mark>", context).len()
    });
    let speedup = if new_stat.p50_us > 0.0 {
        old_stat.p50_us / new_stat.p50_us
    } else {
        0.0
    };

    let normalized_text: String = text.nfkc().collect();
    let char_count = normalized_text.chars().count();
    let offset_entries = char_count + 1;
    let approx_offset_map_bytes = offset_entries * std::mem::size_of::<usize>();

    println!(
        "{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{}",
        name,
        iters,
        old_stat.p50_us,
        old_stat.p95_us,
        old_stat.p99_us,
        new_stat.p50_us,
        new_stat.p95_us,
        new_stat.p99_us,
        speedup,
        new_stat.total_ms,
        approx_offset_map_bytes
    );
}

fn main() {
    let _warmup = Duration::from_millis(10);
    let short = build_doc(20); // ~1k chars
    let medium = build_doc(200); // ~10k chars
    let long = build_doc(2000); // ~100k chars
    let query_tail = "終端キーワード_スニペット計測";

    println!("name,iters,legacy_p50_us,legacy_p95_us,legacy_p99_us,new_p50_us,new_p95_us,new_p99_us,speedup_p50,total_ms_new,approx_offset_map_bytes");
    bench_case("snippet_short_tail_hit", &short, query_tail, 30, 2_000);
    bench_case("snippet_medium_tail_hit", &medium, query_tail, 30, 2_000);
    bench_case("snippet_long_tail_hit", &long, query_tail, 30, 500);
}
