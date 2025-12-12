//! UTF-8-safe string truncation utilities

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
