//! PostgreSQL array decoding that keeps dimensions, lower bounds and NULL
//! elements, in both text and binary result formats.

use super::{FromPg, TypeError};
use crate::protocol::types::{array_element_oid, oid};

/// PostgreSQL `MAXDIM`.
const MAX_DIMENSIONS: usize = 6;

/// `box[]` is the only built-in array whose text form separates elements with `;`.
const BOX_ARRAY_OID: u32 = 1020;

/// One array dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrayDimension {
    /// Number of elements along this dimension.
    pub len: usize,
    /// Subscript of the first element (1 unless the array was built otherwise).
    pub lower_bound: i32,
}

/// A PostgreSQL array of any shape.
///
/// Elements are stored in row-major order (the last subscript varies
/// fastest), as PostgreSQL sends them. SQL NULL elements are `None`.
/// An empty array has no dimensions.
#[derive(Debug, Clone, PartialEq)]
pub struct PgArray<T> {
    dimensions: Vec<ArrayDimension>,
    elements: Vec<Option<T>>,
}

impl<T> PgArray<T> {
    /// Dimensions, outermost first.
    pub fn dimensions(&self) -> &[ArrayDimension] {
        &self.dimensions
    }

    /// Number of dimensions (0 for an empty array).
    pub fn ndim(&self) -> usize {
        self.dimensions.len()
    }

    /// Elements in row-major order; `None` is SQL NULL.
    pub fn elements(&self) -> &[Option<T>] {
        &self.elements
    }

    /// Consume the array, returning its elements in row-major order.
    pub fn into_elements(self) -> Vec<Option<T>> {
        self.elements
    }

    /// Whether the array has no elements.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl<T: FromPg> FromPg for PgArray<T> {
    /// Binary elements decode with the element OID from the array header.
    /// Text elements decode with the element OID of a built-in array OID, or
    /// OID 0 for other arrays; decoders that check the OID then refuse them.
    fn from_pg(bytes: &[u8], oid_val: u32, format: i16) -> Result<Self, TypeError> {
        match format {
            1 => {
                let raw = parse_binary_array(bytes)?;
                let elements = decode_elements(raw.elements, raw.element_oid, 1)?;
                Ok(PgArray {
                    dimensions: raw.dimensions,
                    elements,
                })
            }
            0 => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| TypeError::InvalidData(format!("Invalid UTF-8: {}", e)))?;
                let delimiter = if oid_val == BOX_ARRAY_OID { ';' } else { ',' };
                let (dimensions, raw) = parse_text_array(text, delimiter)?;
                let element_oid = array_element_oid(oid_val).unwrap_or(0);
                let elements = raw
                    .iter()
                    .map(|element| element.as_deref().map(str::as_bytes))
                    .collect();
                let elements = decode_elements(elements, element_oid, 0)?;
                Ok(PgArray {
                    dimensions,
                    elements,
                })
            }
            other => Err(TypeError::InvalidData(format!(
                "Unsupported array format code: {}",
                other
            ))),
        }
    }
}

fn decode_elements<T: FromPg>(
    raw: Vec<Option<&[u8]>>,
    element_oid: u32,
    format: i16,
) -> Result<Vec<Option<T>>, TypeError> {
    raw.into_iter()
        .enumerate()
        .map(|(idx, element)| {
            element
                .map(|bytes| T::from_pg(bytes, element_oid, format))
                .transpose()
                .map_err(|e| TypeError::InvalidData(format!("array element {}: {}", idx, e)))
        })
        .collect()
}

/// Binary array payload split into its header and element byte slices.
pub(crate) struct RawBinaryArray<'a> {
    pub(crate) element_oid: u32,
    pub(crate) dimensions: Vec<ArrayDimension>,
    pub(crate) elements: Vec<Option<&'a [u8]>>,
}

fn read_i32(bytes: &[u8], pos: &mut usize) -> Result<i32, TypeError> {
    let end = pos
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| TypeError::InvalidData("binary array truncated".to_string()))?;
    let value = i32::from_be_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]);
    *pos = end;
    Ok(value)
}

/// Parse the `array_send` layout: ndim, has-null flag, element OID,
/// (length, lower bound) per dimension, then length-prefixed elements.
pub(crate) fn parse_binary_array(bytes: &[u8]) -> Result<RawBinaryArray<'_>, TypeError> {
    let mut pos = 0;
    let ndim = read_i32(bytes, &mut pos)?;
    let flags = read_i32(bytes, &mut pos)?;
    let element_oid = read_i32(bytes, &mut pos)? as u32;

    let ndim = usize::try_from(ndim)
        .ok()
        .filter(|n| *n <= MAX_DIMENSIONS)
        .ok_or_else(|| {
            TypeError::InvalidData(format!("binary array dimension count out of range: {ndim}"))
        })?;
    if flags != 0 && flags != 1 {
        return Err(TypeError::InvalidData(format!(
            "binary array has invalid flags: {flags}"
        )));
    }

    let mut dimensions = Vec::with_capacity(ndim);
    let mut count: usize = 1;
    for _ in 0..ndim {
        let len = read_i32(bytes, &mut pos)?;
        let lower_bound = read_i32(bytes, &mut pos)?;
        let len = usize::try_from(len).map_err(|_| {
            TypeError::InvalidData(format!("binary array dimension length negative: {len}"))
        })?;
        if len > 0 && lower_bound.checked_add((len - 1) as i32).is_none() {
            return Err(TypeError::InvalidData(
                "binary array upper bound overflows int4".to_string(),
            ));
        }
        count = count
            .checked_mul(len)
            .ok_or_else(|| TypeError::InvalidData("binary array too large".to_string()))?;
        dimensions.push(ArrayDimension { len, lower_bound });
    }
    if ndim == 0 {
        count = 0;
    }
    // Every element carries at least a 4-byte length, which bounds the
    // allocation by the payload actually received.
    if count > (bytes.len() - pos) / 4 {
        return Err(TypeError::InvalidData(format!(
            "binary array declares {count} elements but the payload is shorter"
        )));
    }
    if count == 0 {
        // array_recv stores any zero-length dimension as the empty array.
        dimensions.clear();
    }

    let mut elements = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_i32(bytes, &mut pos)?;
        if len == -1 {
            elements.push(None);
            continue;
        }
        let len = usize::try_from(len).map_err(|_| {
            TypeError::InvalidData(format!("binary array element length invalid: {len}"))
        })?;
        let end = pos
            .checked_add(len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| TypeError::InvalidData("binary array element truncated".to_string()))?;
        elements.push(Some(&bytes[pos..end]));
        pos = end;
    }
    if pos != bytes.len() {
        return Err(TypeError::InvalidData(
            "binary array has trailing bytes".to_string(),
        ));
    }

    Ok(RawBinaryArray {
        element_oid,
        dimensions,
        elements,
    })
}

/// Decode a one-dimensional, NULL-free binary array into `Vec<T>`, refusing
/// every shape a `Vec` cannot hold instead of flattening it.
pub(crate) fn decode_binary_vec<T: FromPg>(
    bytes: &[u8],
    target: &str,
    accepts_element: impl Fn(u32) -> bool,
) -> Result<Vec<T>, TypeError> {
    let raw = parse_binary_array(bytes)?;
    match raw.dimensions.as_slice() {
        [] => return Ok(Vec::new()),
        [dim] if dim.lower_bound == 1 => {}
        [dim] => {
            return Err(TypeError::InvalidData(format!(
                "array lower bound {} cannot be represented as {target}; use PgArray",
                dim.lower_bound
            )));
        }
        _ => {
            return Err(TypeError::InvalidData(format!(
                "multidimensional array cannot be represented as {target}; use PgArray"
            )));
        }
    }
    if !accepts_element(raw.element_oid) {
        return Err(TypeError::InvalidData(format!(
            "binary array element OID {} cannot be decoded as {target}; use PgArray with a matching element type",
            raw.element_oid
        )));
    }
    raw.elements
        .into_iter()
        .enumerate()
        .map(|(idx, element)| match element {
            Some(bytes) => T::from_pg(bytes, raw.element_oid, 1)
                .map_err(|e| TypeError::InvalidData(format!("array element {}: {}", idx, e))),
            None => Err(TypeError::InvalidData(format!(
                "array element {idx} is NULL, which {target} cannot represent; use PgArray"
            ))),
        })
        .collect()
}

/// Text element OIDs whose binary form is the UTF-8 text itself.
pub(crate) fn is_text_like_oid(element_oid: u32) -> bool {
    matches!(
        element_oid,
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME
    )
}

struct TextArrayParser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    delimiter: char,
    /// Length seen at each nesting depth; every sibling must match.
    lens: Vec<Option<usize>>,
    /// Depth at which elements (not sub-arrays) appear.
    leaf_depth: Option<usize>,
    elements: Vec<Option<String>>,
}

fn invalid(msg: impl Into<String>) -> TypeError {
    TypeError::InvalidData(msg.into())
}

/// Parse `array_out` text: optional `[lb:ub]...=` bounds, then nested braces.
fn parse_text_array(
    s: &str,
    delimiter: char,
) -> Result<(Vec<ArrayDimension>, Vec<Option<String>>), TypeError> {
    let (bounds, body) = match s.strip_prefix('[') {
        Some(_) => {
            let eq = s
                .find("={")
                .ok_or_else(|| invalid("array bounds must be followed by '='"))?;
            (Some(parse_text_bounds(&s[..eq])?), &s[eq + 1..])
        }
        None => (None, s),
    };

    let mut parser = TextArrayParser {
        chars: body.chars().peekable(),
        delimiter,
        lens: Vec::new(),
        leaf_depth: None,
        elements: Vec::new(),
    };
    if parser.chars.peek() != Some(&'{') {
        return Err(invalid("Array must be enclosed in braces"));
    }
    parser.parse_level(0)?;
    if parser.chars.next().is_some() {
        return Err(invalid(
            "array has trailing characters after its closing brace",
        ));
    }

    if parser.elements.is_empty() {
        if bounds.is_some() {
            return Err(invalid("empty array cannot carry explicit bounds"));
        }
        return Ok((Vec::new(), Vec::new()));
    }

    let lens: Vec<usize> = parser
        .lens
        .iter()
        .map(|len| len.ok_or_else(|| invalid("array has an empty sub-array")))
        .collect::<Result<_, _>>()?;
    let dimensions = match bounds {
        None => lens
            .iter()
            .map(|&len| ArrayDimension {
                len,
                lower_bound: 1,
            })
            .collect(),
        Some(bounds) => {
            if bounds.len() != lens.len() {
                return Err(invalid(format!(
                    "array bounds declare {} dimensions but the value has {}",
                    bounds.len(),
                    lens.len()
                )));
            }
            for (dim, len) in bounds.iter().zip(&lens) {
                if dim.len != *len {
                    return Err(invalid(format!(
                        "array bounds declare {} elements but the dimension has {}",
                        dim.len, len
                    )));
                }
            }
            bounds
        }
    };
    Ok((dimensions, parser.elements))
}

fn parse_text_bounds(s: &str) -> Result<Vec<ArrayDimension>, TypeError> {
    let mut dimensions = Vec::new();
    let mut rest = s;
    while !rest.is_empty() {
        let inner_end = rest
            .find(']')
            .filter(|_| rest.starts_with('['))
            .ok_or_else(|| invalid(format!("invalid array bounds: {s}")))?;
        let inner = &rest[1..inner_end];
        let (lower, upper) = match inner.split_once(':') {
            Some((lower, upper)) => (lower, upper),
            None => ("1", inner),
        };
        let lower: i32 = lower
            .parse()
            .map_err(|_| invalid(format!("invalid array lower bound: {s}")))?;
        let upper: i32 = upper
            .parse()
            .map_err(|_| invalid(format!("invalid array upper bound: {s}")))?;
        let len = i64::from(upper) - i64::from(lower) + 1;
        let len = usize::try_from(len)
            .ok()
            .filter(|len| *len > 0)
            .ok_or_else(|| invalid(format!("array upper bound below lower bound: {s}")))?;
        dimensions.push(ArrayDimension {
            len,
            lower_bound: lower,
        });
        rest = &rest[inner_end + 1..];
    }
    if dimensions.len() > MAX_DIMENSIONS {
        return Err(invalid("array has more than 6 dimensions"));
    }
    Ok(dimensions)
}

impl TextArrayParser<'_> {
    /// Parse one `{...}` level starting at its opening brace.
    fn parse_level(&mut self, depth: usize) -> Result<(), TypeError> {
        if depth >= MAX_DIMENSIONS {
            return Err(invalid("array has more than 6 dimensions"));
        }
        self.chars.next(); // '{'
        if self.lens.len() <= depth {
            self.lens.push(None);
        }

        if self.chars.peek() == Some(&'}') {
            self.chars.next();
            // `{}` is only valid as the whole (empty) array.
            return if depth == 0 {
                Ok(())
            } else {
                Err(invalid("array has an empty sub-array"))
            };
        }

        let nested = self.chars.peek() == Some(&'{');
        match (nested, self.leaf_depth) {
            (false, None) => self.leaf_depth = Some(depth),
            (false, Some(leaf)) if leaf == depth => {}
            (true, None) => {}
            (true, Some(leaf)) if leaf > depth => {}
            _ => return Err(invalid("array nesting depth differs between elements")),
        }
        let mut count = 0usize;
        loop {
            match (nested, self.chars.peek()) {
                (true, Some('{')) => self.parse_level(depth + 1)?,
                (false, Some(c)) if *c != '{' => {
                    let element = self.parse_element()?;
                    self.elements.push(element);
                }
                _ => return Err(invalid("array mixes sub-arrays and elements")),
            }
            count += 1;
            match self.chars.next() {
                Some(c) if c == self.delimiter => continue,
                Some('}') => break,
                _ => {
                    return Err(invalid(
                        "array element is not followed by a delimiter or '}'",
                    ));
                }
            }
        }

        match self.lens[depth] {
            None => self.lens[depth] = Some(count),
            Some(expected) if expected == count => {}
            Some(_) => {
                return Err(invalid(
                    "multidimensional array has sub-arrays of different lengths",
                ));
            }
        }
        Ok(())
    }

    /// One element, quoted or unquoted. Unquoted `NULL` (any case) is SQL NULL.
    fn parse_element(&mut self) -> Result<Option<String>, TypeError> {
        let mut value = String::new();
        if self.chars.peek() == Some(&'"') {
            self.chars.next();
            loop {
                match self.chars.next() {
                    Some('\\') => value.push(
                        self.chars
                            .next()
                            .ok_or_else(|| invalid("Array element ends with dangling escape"))?,
                    ),
                    Some('"') => return Ok(Some(value)),
                    Some(c) => value.push(c),
                    None => return Err(invalid("Array element has unterminated quote")),
                }
            }
        }

        let mut escaped = false;
        while let Some(&c) = self.chars.peek() {
            if c == self.delimiter || c == '}' {
                break;
            }
            self.chars.next();
            match c {
                '\\' => {
                    escaped = true;
                    value.push(
                        self.chars
                            .next()
                            .ok_or_else(|| invalid("Array element ends with dangling escape"))?,
                    );
                }
                '"' | '{' => return Err(invalid("Unexpected character in unquoted array element")),
                c => value.push(c),
            }
        }
        if value.is_empty() {
            return Err(invalid("Empty unquoted array element"));
        }
        if !escaped && value.eq_ignore_ascii_case("NULL") {
            return Ok(None);
        }
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims(spec: &[(usize, i32)]) -> Vec<ArrayDimension> {
        spec.iter()
            .map(|&(len, lower_bound)| ArrayDimension { len, lower_bound })
            .collect()
    }

    fn text<T: FromPg>(s: &str, array_oid: u32) -> Result<PgArray<T>, TypeError> {
        PgArray::<T>::from_pg(s.as_bytes(), array_oid, 0)
    }

    #[test]
    fn text_keeps_nulls_dimensions_and_bounds() {
        let a = text::<String>(r#"{a,NULL,"NULL","b c"}"#, oid::TEXT_ARRAY).unwrap();
        assert_eq!(a.dimensions(), dims(&[(4, 1)]));
        assert_eq!(
            a.elements(),
            &[
                Some("a".to_string()),
                None,
                Some("NULL".to_string()),
                Some("b c".to_string())
            ]
        );

        let m = text::<i64>("{{1,2,3},{4,NULL,6}}", oid::INT8_ARRAY).unwrap();
        assert_eq!(m.dimensions(), dims(&[(2, 1), (3, 1)]));
        assert_eq!(
            m.into_elements(),
            vec![Some(1), Some(2), Some(3), Some(4), None, Some(6)]
        );

        let b = text::<String>("[0:1][-2:-1]={{a,b},{c,d}}", oid::TEXT_ARRAY).unwrap();
        assert_eq!(b.dimensions(), dims(&[(2, 0), (2, -2)]));
        assert_eq!(b.elements().len(), 4);

        let e = text::<i64>("{}", oid::INT4_ARRAY).unwrap();
        assert_eq!(e.ndim(), 0);
        assert!(e.is_empty());
    }

    #[test]
    fn text_rejects_ragged_or_malformed_shapes() {
        for bad in [
            "{{1,2},{3}}",
            "{{1,2},3}",
            "{1,{2,3}}",
            "{{}}",
            "[0:2]={1,2}",
            "[0:1][0:1]={1,2}",
            "[1:0]={}",
            "{1,2}x",
            "{1,,2}",
            "{{{{{{{1}}}}}}}",
            "{1",
            r#"{"a}"#,
            "{a\"b}",
        ] {
            assert!(text::<String>(bad, oid::TEXT_ARRAY).is_err(), "{bad}");
        }
    }

    #[test]
    fn text_elements_decode_with_the_element_oid() {
        let n = text::<super::super::Numeric>("{1.5,NaN,Infinity}", oid::NUMERIC_ARRAY).unwrap();
        assert_eq!(n.elements()[2].as_ref().unwrap().as_str(), "Infinity");
        // Unknown array OID: OID-checking decoders refuse instead of guessing.
        assert!(text::<super::super::Numeric>("{1.5}", 0).is_err());
        // box[] separates elements with ';'.
        let boxes = text::<String>("{(1,1),(0,0);(2,2),(1,1)}", BOX_ARRAY_OID).unwrap();
        assert_eq!(boxes.elements().len(), 2);
    }

    fn binary(elem_oid: u32, spec: &[(i32, i32)], elements: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(spec.len() as i32).to_be_bytes());
        out.extend_from_slice(&i32::from(elements.iter().any(Option::is_none)).to_be_bytes());
        out.extend_from_slice(&elem_oid.to_be_bytes());
        for (len, lower) in spec {
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&lower.to_be_bytes());
        }
        for element in elements {
            match element {
                Some(bytes) => {
                    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    out.extend_from_slice(bytes);
                }
                None => out.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        out
    }

    #[test]
    fn binary_keeps_nulls_dimensions_and_bounds() {
        let one = 1i32.to_be_bytes();
        let two = 2i32.to_be_bytes();
        let bytes = binary(
            oid::INT4,
            &[(2, 0), (2, 5)],
            &[Some(&one), None, Some(&two), Some(&one)],
        );
        let a = PgArray::<i64>::from_pg(&bytes, oid::INT4_ARRAY, 1).unwrap();
        assert_eq!(a.dimensions(), dims(&[(2, 0), (2, 5)]));
        assert_eq!(a.into_elements(), vec![Some(1), None, Some(2), Some(1)]);

        let empty = binary(oid::INT4, &[], &[]);
        assert!(
            PgArray::<i64>::from_pg(&empty, oid::INT4_ARRAY, 1)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn binary_rejects_malformed_payloads() {
        let one = 1i32.to_be_bytes();
        let good = binary(oid::INT4, &[(1, 1)], &[Some(&one)]);
        // Truncated anywhere.
        for cut in 0..good.len() {
            assert!(PgArray::<i64>::from_pg(&good[..cut], oid::INT4_ARRAY, 1).is_err());
        }
        // Trailing bytes.
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(PgArray::<i64>::from_pg(&trailing, oid::INT4_ARRAY, 1).is_err());
        // Bad flags, too many dimensions, negative length, huge declared count.
        let mut flags = good;
        flags[7] = 2;
        assert!(PgArray::<i64>::from_pg(&flags, oid::INT4_ARRAY, 1).is_err());
        let seven = binary(oid::INT4, &[(1, 1); 7], &[Some(&one)]);
        assert!(PgArray::<i64>::from_pg(&seven, oid::INT4_ARRAY, 1).is_err());
        let negative = binary(oid::INT4, &[(-1, 1)], &[]);
        assert!(PgArray::<i64>::from_pg(&negative, oid::INT4_ARRAY, 1).is_err());
        let huge = binary(oid::INT4, &[(i32::MAX, 1), (i32::MAX, 1)], &[]);
        assert!(PgArray::<i64>::from_pg(&huge, oid::INT4_ARRAY, 1).is_err());
        let overflow = binary(oid::INT4, &[(2, i32::MAX)], &[Some(&one), Some(&one)]);
        assert!(PgArray::<i64>::from_pg(&overflow, oid::INT4_ARRAY, 1).is_err());
    }

    #[test]
    fn binary_vec_refuses_shapes_a_vec_cannot_hold() {
        let one = 1i32.to_be_bytes();
        let ok = binary(oid::INT4, &[(2, 1)], &[Some(&one), Some(&one)]);
        assert_eq!(
            decode_binary_vec::<i64>(&ok, "Vec<i64>", |_| true).unwrap(),
            vec![1, 1]
        );
        for bad in [
            binary(oid::INT4, &[(1, 1), (2, 1)], &[Some(&one), Some(&one)]),
            binary(oid::INT4, &[(1, 0)], &[Some(&one)]),
            binary(oid::INT4, &[(1, 1)], &[None]),
        ] {
            assert!(decode_binary_vec::<i64>(&bad, "Vec<i64>", |_| true).is_err());
        }
        assert!(decode_binary_vec::<i64>(&ok, "Vec<i64>", |_| false).is_err());
    }
}
