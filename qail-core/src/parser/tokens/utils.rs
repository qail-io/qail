use nom::{
    character::complete::char,
    IResult,
};

/// Parse a block delimited by { } handling nested braces.
/// Returns the content *inside* the braces.
pub fn parse_balanced_block(input: &str) -> IResult<&str, &str> {
    let (input, _) = char('{')(input)?;
    
    let mut depth = 1;
    let mut end_idx = 0;
    let mut chars = input.char_indices();
    
    while depth > 0 {
        if let Some((idx, c)) = chars.next() {
            if c == '{' {
                depth += 1;
            } else if c == '}' {
                depth -= 1;
                if depth == 0 {
                    end_idx = idx;
                }
            }
        } else {
            // EOF before closing brace
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
        }
    }
    
    let content = &input[..end_idx];
    let remaining = &input[end_idx + 1..];
    
    Ok((remaining, content))
}

/// Parse content inside balanced delimiters (for !sort(col1, col2))
/// Returns a function that parses until the closing delimiter, handling nesting.
pub fn take_until_balanced(open: char, close: char) -> impl Fn(&str) -> IResult<&str, &str> {
    move |input: &str| {
        let mut depth = 1;  // We're already inside after the opening paren
        let mut end_idx = 0;
        let mut chars = input.char_indices();
        
        while depth > 0 {
            if let Some((idx, c)) = chars.next() {
                if c == open {
                    depth += 1;
                } else if c == close {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = idx;
                    }
                }
            } else {
                // EOF before closing delimiter
                return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
            }
        }
        
        let content = &input[..end_idx];
        let remaining = &input[end_idx..];  // Don't skip the closing paren, let caller consume it
        
        Ok((remaining, content))
    }
}
