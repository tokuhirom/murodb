pub(super) fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_inner(&s_chars, &p_chars)
}

fn like_match_inner(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }

    match p[0] {
        '%' => {
            // % matches zero or more characters
            // Try matching the rest of the pattern at every position
            for i in 0..=s.len() {
                if like_match_inner(&s[i..], &p[1..]) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            if s.is_empty() {
                false
            } else {
                like_match_inner(&s[1..], &p[1..])
            }
        }
        c => {
            if s.is_empty() {
                false
            } else if s[0] == c {
                like_match_inner(&s[1..], &p[1..])
            } else {
                false
            }
        }
    }
}
