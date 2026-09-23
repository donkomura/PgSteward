const LISTEN: &str = "listen";

/// Whether any statement in this text registers a listener.
///
/// The answer has to be the same one the server would give, so the scan walks
/// the text the way the lexer does: a `LISTEN` inside a literal or a comment is
/// not a statement, and a statement starts at the beginning of the text or
/// after a semicolon that is itself outside a literal and a comment.
///
/// Anything this misses is a statement whose first word is not written as a
/// word, which PostgreSQL would reject as a syntax error anyway.
#[must_use]
pub fn has_listen(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut at = 0;
    let mut head = true;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte == b'-' && bytes.get(at + 1) == Some(&b'-') {
            at = end_of_line_comment(bytes, at);
        } else if byte == b'/' && bytes.get(at + 1) == Some(&b'*') {
            at = end_of_block_comment(bytes, at);
        } else if byte == b';' {
            head = true;
            at += 1;
        } else if byte.is_ascii_whitespace() {
            at += 1;
        } else if byte == b'\'' {
            at = end_of_quoted(bytes, at, escapes_backslashes(bytes, at));
            head = false;
        } else if byte == b'"' {
            at = end_of_quoted(bytes, at, false);
            head = false;
        } else if let Some(end) = end_of_dollar_quoted(bytes, at) {
            at = end;
            head = false;
        } else if is_word(byte) {
            let end = end_of_word(bytes, at);
            if head && sql[at..end].eq_ignore_ascii_case(LISTEN) {
                return true;
            }
            head = false;
            at = end;
        } else {
            head = false;
            at += 1;
        }
    }
    false
}

fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte >= 0x80
}

fn end_of_word(bytes: &[u8], at: usize) -> usize {
    let mut end = at;
    while end < bytes.len() && is_word(bytes[end]) {
        end += 1;
    }
    end
}

fn end_of_line_comment(bytes: &[u8], at: usize) -> usize {
    bytes[at..]
        .iter()
        .position(|byte| *byte == b'\n')
        .map_or(bytes.len(), |offset| at + offset + 1)
}

fn end_of_block_comment(bytes: &[u8], at: usize) -> usize {
    let mut depth = 0usize;
    let mut cursor = at;
    while cursor + 1 < bytes.len() {
        match (bytes[cursor], bytes[cursor + 1]) {
            (b'/', b'*') => {
                depth += 1;
                cursor += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                cursor += 2;
                if depth == 0 {
                    return cursor;
                }
            }
            _ => cursor += 1,
        }
    }
    bytes.len()
}

/// A literal that spells `E'…'` reads a backslash as an escape, and a plain
/// `'…'` does not, which is the `standard_conforming_strings` PostgreSQL has
/// defaulted to since 9.1.
fn escapes_backslashes(bytes: &[u8], quote: usize) -> bool {
    let Some(prefix) = quote.checked_sub(1) else {
        return false;
    };
    if !matches!(bytes[prefix], b'e' | b'E') {
        return false;
    }
    prefix
        .checked_sub(1)
        .is_none_or(|before| !is_word(bytes[before]))
}

fn end_of_quoted(bytes: &[u8], at: usize, escapes: bool) -> usize {
    let quote = bytes[at];
    let mut cursor = at + 1;
    while cursor < bytes.len() {
        if escapes && bytes[cursor] == b'\\' {
            cursor += 2;
        } else if bytes[cursor] != quote {
            cursor += 1;
        } else if bytes.get(cursor + 1) == Some(&quote) {
            cursor += 2;
        } else {
            return cursor + 1;
        }
    }
    bytes.len()
}

fn end_of_dollar_quoted(bytes: &[u8], at: usize) -> Option<usize> {
    if bytes[at] != b'$' {
        return None;
    }
    let mut end = at + 1;
    while end < bytes.len() && is_tag(bytes[end], end == at + 1) {
        end += 1;
    }
    if bytes.get(end) != Some(&b'$') {
        return None;
    }
    let tag = &bytes[at..=end];
    let body = end + 1;
    bytes[body..]
        .windows(tag.len())
        .position(|window| window == tag)
        .map_or(Some(bytes.len()), |offset| Some(body + offset + tag.len()))
}

fn is_tag(byte: u8, first: bool) -> bool {
    byte == b'_' || byte >= 0x80 || byte.is_ascii_alphabetic() || (!first && byte.is_ascii_digit())
}
