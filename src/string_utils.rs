//! UTF-8-safe string truncation utilities

/// Safely truncate a string to a maximum number of CHARACTERS (not bytes).
///
/// Respects UTF-8 character boundaries - never panics on multi-byte characters.
///
/// # Performance
/// * O(n) where n = max_chars (iterates through characters)
/// * Zero allocation - returns slice of original string
#[inline]
#[allow(dead_code)]
pub fn safe_truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((byte_idx, _)) => &s[..byte_idx],
    }
}

/// Find a safe byte index for truncation, preferring word boundaries.
///
/// Returns byte index of the last whitespace/punctuation within the first
/// `max_chars` characters. If no boundary found, returns byte index of
/// the `max_chars`-th character (or string length if shorter).
pub fn safe_truncate_word_boundary(s: &str, max_chars: usize) -> usize {
    let max_byte_idx = s
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(s.len());

    s[..max_byte_idx]
        .rfind(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
        .unwrap_or(max_byte_idx)
}
