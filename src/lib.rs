/// A knowledge tile — the fundamental unit imported from external formats.
#[derive(Debug, Clone, PartialEq)]
pub struct Tile {
    pub question: String,
    pub answer: String,
    pub tags: Vec<String>,
    pub domain: String,
}

impl Tile {
    pub fn new(question: impl Into<String>, answer: impl Into<String>) -> Self {
        Self {
            question: question.into(),
            answer: answer.into(),
            tags: vec![],
            domain: String::new(),
        }
    }
}

/// Import tiles from Markdown.
/// Each `##` header becomes a tile question; the body until the next header is the answer.
/// Tags: words in [brackets] in the body.
/// Domain: first bracketed word if present, otherwise empty.
pub fn import_markdown(md: &str) -> Vec<Tile> {
    let mut tiles: Vec<Tile> = Vec::new();
    let mut current_question: Option<String> = None;
    let mut current_body_lines: Vec<String> = Vec::new();

    let flush = |question: &str, body_lines: &[String], tiles: &mut Vec<Tile>| {
        let answer = body_lines.join("\n").trim().to_string();
        let tags = extract_brackets(&answer);
        let domain = tags.first().cloned().unwrap_or_default();
        tiles.push(Tile {
            question: question.to_string(),
            answer,
            tags,
            domain,
        });
    };

    for line in md.lines() {
        if let Some(q) = line.strip_prefix("## ") {
            if let Some(prev_q) = current_question.take() {
                flush(&prev_q, &current_body_lines, &mut tiles);
                current_body_lines.clear();
            }
            current_question = Some(q.trim().to_string());
        } else if current_question.is_some() {
            current_body_lines.push(line.to_string());
        }
    }

    if let Some(q) = current_question {
        flush(&q, &current_body_lines, &mut tiles);
    }

    tiles
}

/// Extract all [bracketed] words/phrases from a string.
fn extract_brackets(text: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        rest = &rest[open + 1..];
        if let Some(close) = rest.find(']') {
            let tag = rest[..close].trim().to_string();
            if !tag.is_empty() {
                tags.push(tag);
            }
            rest = &rest[close + 1..];
        } else {
            break;
        }
    }
    tags
}

/// Import tiles from JSON.
/// Expects a JSON array: [{"question":"...","answer":"...","tags":["..."],"domain":"..."}]
/// Silently skips malformed entries. Tags and domain are optional (default: empty).
/// Pure Rust, no serde — parse manually using basic string operations.
pub fn import_json(json: &str) -> Vec<Tile> {
    let json = json.trim();

    // Must start with '[' and end with ']'
    if !json.starts_with('[') || !json.ends_with(']') {
        return vec![];
    }

    let inner = &json[1..json.len() - 1].trim();
    if inner.is_empty() {
        return vec![];
    }

    let mut tiles = Vec::new();
    for obj_str in split_json_objects(inner) {
        let obj_str = obj_str.trim();
        if obj_str.is_empty() {
            continue;
        }
        if let Some(tile) = parse_json_object(obj_str) {
            tiles.push(tile);
        }
    }
    tiles
}

/// Split a JSON array body (without outer brackets) into individual object strings.
fn split_json_objects(s: &str) -> Vec<&str> {
    let mut objects = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    let bytes = s.as_bytes();
    let mut in_string = false;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\\' {
                i += 1; // skip escaped char
            } else if b == b'"' {
                in_string = false;
            }
        } else {
            match b {
                b'"' => in_string = true,
                b'{' => {
                    if depth == 0 {
                        start = i;
                    }
                    depth += 1;
                }
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        objects.push(&s[start..=i]);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    objects
}

/// Parse a single JSON object string into a Tile.
fn parse_json_object(obj: &str) -> Option<Tile> {
    let question = extract_json_string_field(obj, "question")?;
    let answer = extract_json_string_field(obj, "answer")?;
    let tags = extract_json_string_array_field(obj, "tags").unwrap_or_default();
    let domain = extract_json_string_field(obj, "domain").unwrap_or_default();

    Some(Tile {
        question,
        answer,
        tags,
        domain,
    })
}

/// Extract a string field value from a JSON object string.
fn extract_json_string_field(obj: &str, field: &str) -> Option<String> {
    let key = format!("\"{}\"", field);
    let pos = obj.find(&key)?;
    let after_key = &obj[pos + key.len()..];
    // Find the colon
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if !after_colon.starts_with('"') {
        return None;
    }
    Some(parse_json_string_value(after_colon))
}

/// Extract a JSON string array field value from a JSON object string.
fn extract_json_string_array_field(obj: &str, field: &str) -> Option<Vec<String>> {
    let key = format!("\"{}\"", field);
    let pos = obj.find(&key)?;
    let after_key = &obj[pos + key.len()..];
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if !after_colon.starts_with('[') {
        return None;
    }
    // Find matching close bracket
    let close = after_colon.find(']')?;
    let array_inner = &after_colon[1..close];
    let mut result = Vec::new();
    // Split by commas and parse each string
    for item in array_inner.split(',') {
        let item = item.trim();
        if item.starts_with('"') {
            result.push(parse_json_string_value(item));
        }
    }
    Some(result)
}

/// Parse a JSON string starting with `"`, handling escape sequences.
fn parse_json_string_value(s: &str) -> String {
    // s starts with '"'
    let s = &s[1..]; // skip opening quote
    let mut result = String::new();
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => break,
            '\\' => {
                if let Some(escaped) = chars.next() {
                    match escaped {
                        'n' => result.push('\n'),
                        't' => result.push('\t'),
                        'r' => result.push('\r'),
                        '"' => result.push('"'),
                        '\\' => result.push('\\'),
                        '/' => result.push('/'),
                        other => {
                            result.push('\\');
                            result.push(other);
                        }
                    }
                }
            }
            other => result.push(other),
        }
    }
    result
}

/// Import tiles from CSV.
/// Format: question,answer (first two columns; extra columns ignored).
/// Skips the header row if first row starts with "question" (case-insensitive).
/// Skips blank lines. Handles double-quoted fields (strip outer quotes).
pub fn import_csv(csv: &str) -> Vec<Tile> {
    let mut tiles = Vec::new();
    let mut lines = csv.lines().peekable();

    // Check and skip header row
    if let Some(&first) = lines.peek() {
        if first.trim().to_lowercase().starts_with("question") {
            lines.next();
        }
    }

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let cols = parse_csv_line(line);
        if cols.len() < 2 {
            // Single-column: question only, answer empty
            if !cols.is_empty() {
                let question = cols[0].clone();
                if !question.is_empty() {
                    tiles.push(Tile::new(question, ""));
                }
            }
            continue;
        }
        let question = cols[0].clone();
        let answer = cols[1].clone();
        if !question.is_empty() {
            tiles.push(Tile::new(question, answer));
        }
    }
    tiles
}

/// Parse a single CSV line into fields, handling double-quoted fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;

    while let Some(c) = chars.next() {
        match c {
            '"' if !in_quotes => {
                in_quotes = true;
            }
            '"' if in_quotes => {
                // Check for escaped quote ""
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            other => current.push(other),
        }
    }
    fields.push(current);
    fields
}

/// Import tiles from plain text.
/// Split by blank lines into paragraphs. Each paragraph: first line = question, rest = answer.
/// Single-line paragraphs: question = line, answer = "".
pub fn import_plaintext(text: &str) -> Vec<Tile> {
    let mut tiles = Vec::new();
    let mut current_para: Vec<&str> = Vec::new();

    let flush_para = |para: &[&str], tiles: &mut Vec<Tile>| {
        if para.is_empty() {
            return;
        }
        let question = para[0].to_string();
        let answer = if para.len() > 1 {
            para[1..].join("\n")
        } else {
            String::new()
        };
        tiles.push(Tile::new(question, answer));
    };

    for line in text.lines() {
        if line.trim().is_empty() {
            flush_para(&current_para, &mut tiles);
            current_para.clear();
        } else {
            current_para.push(line);
        }
    }
    flush_para(&current_para, &mut tiles);

    tiles
}

/// Export tiles to Markdown.
/// Each tile becomes "## {question}\n{answer}\n\n".
pub fn export_markdown(tiles: &[Tile]) -> String {
    if tiles.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for tile in tiles {
        out.push_str("## ");
        out.push_str(&tile.question);
        out.push('\n');
        out.push_str(&tile.answer);
        out.push_str("\n\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- import_markdown ---

    #[test]
    fn test_import_markdown_basic_two_sections() {
        let md = "## What is Rust?\nA systems language.\n\n## What is cargo?\nRust's build tool.\n";
        let tiles = import_markdown(md);
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].question, "What is Rust?");
        assert_eq!(tiles[0].answer, "A systems language.");
        assert_eq!(tiles[1].question, "What is cargo?");
        assert_eq!(tiles[1].answer, "Rust's build tool.");
    }

    #[test]
    fn test_import_markdown_empty_string() {
        let tiles = import_markdown("");
        assert!(tiles.is_empty());
    }

    #[test]
    fn test_import_markdown_no_headers() {
        let md = "No headers here.\nJust plain text.";
        let tiles = import_markdown(md);
        assert!(tiles.is_empty());
    }

    #[test]
    fn test_import_markdown_extracts_tags_from_brackets() {
        let md = "## Concept\nThis belongs to [science] and [physics] domains.\n";
        let tiles = import_markdown(md);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].tags, vec!["science", "physics"]);
        assert_eq!(tiles[0].domain, "science");
    }

    #[test]
    fn test_import_markdown_empty_body_no_tags() {
        let md = "## Just a question\n";
        let tiles = import_markdown(md);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].question, "Just a question");
        assert_eq!(tiles[0].answer, "");
        assert!(tiles[0].tags.is_empty());
        assert_eq!(tiles[0].domain, "");
    }

    // --- import_json ---

    #[test]
    fn test_import_json_parses_valid_array() {
        let json = r#"[{"question":"Q1","answer":"A1","tags":["t1","t2"],"domain":"science"}]"#;
        let tiles = import_json(json);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].question, "Q1");
        assert_eq!(tiles[0].answer, "A1");
        assert_eq!(tiles[0].tags, vec!["t1", "t2"]);
        assert_eq!(tiles[0].domain, "science");
    }

    #[test]
    fn test_import_json_empty_array() {
        let tiles = import_json("[]");
        assert!(tiles.is_empty());
    }

    #[test]
    fn test_import_json_multiple_entries() {
        let json = r#"[{"question":"Q1","answer":"A1"},{"question":"Q2","answer":"A2"}]"#;
        let tiles = import_json(json);
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].question, "Q1");
        assert_eq!(tiles[1].question, "Q2");
    }

    #[test]
    fn test_import_json_missing_optional_fields() {
        let json = r#"[{"question":"Q1","answer":"A1"}]"#;
        let tiles = import_json(json);
        assert_eq!(tiles.len(), 1);
        assert!(tiles[0].tags.is_empty());
        assert_eq!(tiles[0].domain, "");
    }

    #[test]
    fn test_import_json_skips_malformed_missing_answer() {
        let json = r#"[{"question":"Q1"},{"question":"Q2","answer":"A2"}]"#;
        let tiles = import_json(json);
        // Entry without "answer" is skipped; second entry is valid
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].question, "Q2");
    }

    // --- import_csv ---

    #[test]
    fn test_import_csv_basic_two_column() {
        let csv = "What is 2+2?,Four\nWhat is Rust?,A systems language\n";
        let tiles = import_csv(csv);
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].question, "What is 2+2?");
        assert_eq!(tiles[0].answer, "Four");
        assert_eq!(tiles[1].question, "What is Rust?");
        assert_eq!(tiles[1].answer, "A systems language");
    }

    #[test]
    fn test_import_csv_skips_header_row() {
        let csv = "question,answer\nWhat is Rust?,A systems language\n";
        let tiles = import_csv(csv);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].question, "What is Rust?");
    }

    #[test]
    fn test_import_csv_quoted_fields() {
        let csv = "\"What is, Rust?\",\"A systems language\"\n";
        let tiles = import_csv(csv);
        assert_eq!(tiles.len(), 1);
        assert_eq!(tiles[0].question, "What is, Rust?");
        assert_eq!(tiles[0].answer, "A systems language");
    }

    #[test]
    fn test_import_csv_skips_blank_lines() {
        let csv = "Q1,A1\n\nQ2,A2\n";
        let tiles = import_csv(csv);
        assert_eq!(tiles.len(), 2);
    }

    // --- import_plaintext ---

    #[test]
    fn test_import_plaintext_splits_by_blank_lines() {
        let text = "Question one\nAnswer line one\nAnswer line two\n\nQuestion two\nAnswer two\n";
        let tiles = import_plaintext(text);
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].question, "Question one");
        assert_eq!(tiles[0].answer, "Answer line one\nAnswer line two");
        assert_eq!(tiles[1].question, "Question two");
        assert_eq!(tiles[1].answer, "Answer two");
    }

    #[test]
    fn test_import_plaintext_single_line_paragraph() {
        let text = "Just a question\n\nAnother question\n";
        let tiles = import_plaintext(text);
        assert_eq!(tiles.len(), 2);
        assert_eq!(tiles[0].question, "Just a question");
        assert_eq!(tiles[0].answer, "");
        assert_eq!(tiles[1].question, "Another question");
        assert_eq!(tiles[1].answer, "");
    }

    #[test]
    fn test_import_plaintext_empty_string() {
        let tiles = import_plaintext("");
        assert!(tiles.is_empty());
    }

    // --- export_markdown ---

    #[test]
    fn test_export_markdown_formats_tiles_correctly() {
        let tiles = vec![
            Tile {
                question: "Q1".into(),
                answer: "A1".into(),
                tags: vec![],
                domain: String::new(),
            },
            Tile {
                question: "Q2".into(),
                answer: "A2".into(),
                tags: vec![],
                domain: String::new(),
            },
        ];
        let md = export_markdown(&tiles);
        assert_eq!(md, "## Q1\nA1\n\n## Q2\nA2\n\n");
    }

    #[test]
    fn test_export_markdown_empty_slice_returns_empty_string() {
        let md = export_markdown(&[]);
        assert_eq!(md, "");
    }

    // --- roundtrip ---

    #[test]
    fn test_roundtrip_export_then_import_markdown() {
        let original = vec![
            Tile::new("What is Rust?", "A systems language."),
            Tile::new("What is cargo?", "Rust's build tool."),
        ];
        let md = export_markdown(&original);
        let imported = import_markdown(&md);
        assert_eq!(imported.len(), 2);
        assert_eq!(imported[0].question, original[0].question);
        assert_eq!(imported[0].answer, original[0].answer);
        assert_eq!(imported[1].question, original[1].question);
        assert_eq!(imported[1].answer, original[1].answer);
    }
}
