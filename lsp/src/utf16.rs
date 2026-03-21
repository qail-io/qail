//! UTF-16/LSP position mapping helpers.

use tower_lsp::lsp_types::Position;

pub struct Utf16Index<'a> {
    text: &'a str,
    line_starts: Vec<usize>,
}

impl<'a> Utf16Index<'a> {
    pub fn new(text: &'a str) -> Self {
        let mut line_starts = Vec::with_capacity(text.lines().count() + 1);
        line_starts.push(0);
        for (idx, b) in text.bytes().enumerate() {
            if b == b'\n' {
                line_starts.push(idx + 1);
            }
        }

        Self { text, line_starts }
    }

    pub fn line_len_utf16(&self, line: usize) -> Option<usize> {
        let start = *self.line_starts.get(line)?;
        let end = self.line_end_no_newline(line)?;
        Some(self.text.get(start..end)?.encode_utf16().count())
    }

    pub fn one_based_byte_col_to_utf16(&self, line_1: usize, col_1: usize) -> Option<usize> {
        let line = line_1.saturating_sub(1);
        let byte_col = col_1.saturating_sub(1);
        self.byte_col_to_utf16(line, byte_col)
    }

    pub fn byte_col_to_utf16(&self, line: usize, byte_col: usize) -> Option<usize> {
        let line_start = *self.line_starts.get(line)?;
        let line_end = self.line_end_no_newline(line)?;
        let offset = line_start.saturating_add(byte_col).min(line_end);
        let boundary = self.prev_char_boundary(offset);
        Some(self.offset_to_position(boundary).character as usize)
    }

    pub fn offset_to_position(&self, offset: usize) -> Position {
        let capped = offset.min(self.text.len());
        let idx = self.line_starts.partition_point(|&start| start <= capped);
        let line = idx.saturating_sub(1);
        let line_start = self.line_starts.get(line).copied().unwrap_or(0);
        let line_end = self.line_end_no_newline(line).unwrap_or(self.text.len());
        let in_line = capped.min(line_end);
        let char_boundary = self.prev_char_boundary(in_line);
        let utf16_col = self
            .text
            .get(line_start..char_boundary)
            .map(|s| s.encode_utf16().count())
            .unwrap_or(0);

        Position {
            line: line as u32,
            character: utf16_col as u32,
        }
    }

    pub fn position_to_offset(&self, pos: Position) -> Option<usize> {
        let line = pos.line as usize;
        let line_start = *self.line_starts.get(line)?;
        let line_end = self.line_end_no_newline(line)?;
        let line_text = self.text.get(line_start..line_end)?;
        let target = pos.character as usize;

        if target == 0 {
            return Some(line_start);
        }

        let mut utf16 = 0usize;
        for (byte_idx, ch) in line_text.char_indices() {
            if utf16 == target {
                return Some(line_start + byte_idx);
            }
            utf16 += ch.len_utf16();
            if utf16 > target {
                return None;
            }
        }

        (utf16 == target).then_some(line_end)
    }

    fn line_end_no_newline(&self, line: usize) -> Option<usize> {
        let start = *self.line_starts.get(line)?;
        let next_start = self
            .line_starts
            .get(line + 1)
            .copied()
            .unwrap_or(self.text.len());
        if next_start > start
            && self.text.as_bytes().get(next_start.saturating_sub(1)) == Some(&b'\n')
        {
            Some(next_start.saturating_sub(1))
        } else {
            Some(next_start)
        }
    }

    fn prev_char_boundary(&self, mut offset: usize) -> usize {
        offset = offset.min(self.text.len());
        while offset > 0 && !self.text.is_char_boundary(offset) {
            offset -= 1;
        }
        offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_col_maps_to_utf16_for_multibyte_text() {
        let text = "aé🙂b\n";
        let idx = Utf16Index::new(text);

        // Byte columns: a(1), é(2), 🙂(4), b(1)
        assert_eq!(idx.byte_col_to_utf16(0, 0), Some(0));
        assert_eq!(idx.byte_col_to_utf16(0, 1), Some(1));
        assert_eq!(idx.byte_col_to_utf16(0, 3), Some(2));
        assert_eq!(idx.byte_col_to_utf16(0, 7), Some(4));
    }

    #[test]
    fn position_roundtrips_with_surrogate_pairs() {
        let text = "🙂x\n";
        let idx = Utf16Index::new(text);

        let off_emoji_end = idx
            .position_to_offset(Position {
                line: 0,
                character: 2,
            })
            .expect("offset for emoji end");
        assert_eq!(idx.offset_to_position(off_emoji_end).character, 2);

        let off_line_end = idx
            .position_to_offset(Position {
                line: 0,
                character: 3,
            })
            .expect("offset for line end");
        assert_eq!(idx.offset_to_position(off_line_end).character, 3);
    }
}
