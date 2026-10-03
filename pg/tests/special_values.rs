//! Decoder coverage for PostgreSQL special values: array shapes, escape-format
//! bytea, numeric infinity, time 24:00, temporal infinity, alternate
//! DateStyle output and BC dates.
//!
//! Every input here is a byte string PostgreSQL itself emits; the live
//! counterpart (`special_values_live.rs`) fetches the same values from a server.

use qail_pg::protocol::types::oid;
use qail_pg::{Date, FromPg, Numeric, Time, Timestamp};

const USEC_PER_DAY: i64 = 86_400_000_000;

fn binary_array(elem_oid: u32, dims: &[(i32, i32)], elements: &[Option<&[u8]>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(dims.len() as i32).to_be_bytes());
    let has_null = elements.iter().any(Option::is_none);
    out.extend_from_slice(&i32::from(has_null).to_be_bytes());
    out.extend_from_slice(&elem_oid.to_be_bytes());
    for (len, lower) in dims {
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

fn numeric_header(sign: u16) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0u16.to_be_bytes()); // ndigits
    bytes.extend_from_slice(&0i16.to_be_bytes()); // weight
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&0u16.to_be_bytes()); // dscale
    bytes
}

fn date_text(text: &str) -> Result<Date, qail_pg::TypeError> {
    Date::from_pg(text.as_bytes(), oid::DATE, 0)
}

fn timestamp_text(text: &str) -> Result<Timestamp, qail_pg::TypeError> {
    Timestamp::from_pg(text.as_bytes(), oid::TIMESTAMP, 0)
}

// ---- E10: escape-format bytea ----

#[test]
fn bytea_escape_text_decodes_octal_and_backslash() {
    // bytea_output = 'escape' prints '\x015c41ff' as \001\\A\377.
    let bytes = Vec::<u8>::from_pg(br"\001\\A\377", oid::BYTEA, 0).unwrap();
    assert_eq!(bytes, vec![1, 92, b'A', 255]);
}

// ---- E11: binary numeric infinity ----

#[test]
fn numeric_binary_decodes_infinity_signs() {
    let pos = Numeric::from_pg(&numeric_header(0xD000), oid::NUMERIC, 1).unwrap();
    let neg = Numeric::from_pg(&numeric_header(0xF000), oid::NUMERIC, 1).unwrap();
    // Same spelling as the text result format.
    assert_eq!(pos.as_str(), "Infinity");
    assert_eq!(neg.as_str(), "-Infinity");
}

// ---- F4: time 24:00 and temporal infinity ----

#[test]
fn time_accepts_end_of_day_in_text_and_binary() {
    let text = Time::from_pg(b"24:00:00", oid::TIME, 0).unwrap();
    let binary = Time::from_pg(&USEC_PER_DAY.to_be_bytes(), oid::TIME, 1).unwrap();
    assert_eq!(text.usec, USEC_PER_DAY);
    assert_eq!(binary.usec, USEC_PER_DAY);
    assert_eq!(text.hour(), 24);
}

#[test]
fn text_infinity_matches_binary_sentinels() {
    assert_eq!(timestamp_text("infinity").unwrap().usec, i64::MAX);
    assert_eq!(timestamp_text("-infinity").unwrap().usec, i64::MIN);
    assert_eq!(date_text("infinity").unwrap().days, i32::MAX);
    assert_eq!(date_text("-infinity").unwrap().days, i32::MIN);
}

// ---- F5: DateStyle variants, BC, second-resolution offsets ----

#[test]
fn german_datestyle_decodes_unambiguous_dates() {
    let iso = date_text("2026-09-30").unwrap();
    assert_eq!(date_text("30.09.2026").unwrap(), iso);
    let iso_ts = timestamp_text("2026-09-30 14:05:06.5").unwrap();
    assert_eq!(timestamp_text("30.09.2026 14:05:06.5").unwrap(), iso_ts);
}

#[test]
fn bc_suffix_decodes_to_astronomical_year() {
    // 1 BC is astronomical year 0; 2000 years before 2000-01-01 with 485 leap days.
    assert_eq!(date_text("0001-01-01 BC").unwrap().days, -730_485);
    let ts = timestamp_text("0001-01-01 12:00:00 BC").unwrap();
    assert_eq!(ts.usec, -730_485 * USEC_PER_DAY + 12 * 3_600_000_000);
}

#[test]
fn timezone_offset_with_seconds_decodes() {
    // TimeZone 'Asia/Kolkata' prints 1900 instants with the LMT offset.
    let ts = Timestamp::from_pg(b"1900-01-01 05:21:10+05:21:10", oid::TIMESTAMPTZ, 0).unwrap();
    let utc = Timestamp::from_pg(b"1900-01-01 00:00:00+00", oid::TIMESTAMPTZ, 0).unwrap();
    assert_eq!(ts, utc);
}

#[test]
fn ambiguous_datestyles_fail_with_datestyle_error() {
    for text in ["09/30/2026", "30/09/2026", "09-30-2026"] {
        let err = date_text(text).unwrap_err().to_string();
        assert!(err.contains("DateStyle"), "{text}: {err}");
    }
    for text in [
        "09/30/2026 14:05:06.5",
        "Wed Sep 30 14:05:06.5 2026",
        "30.09.2026 17:35:06.5 IST",
    ] {
        let err = Timestamp::from_pg(text.as_bytes(), oid::TIMESTAMPTZ, 0)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("DateStyle") || err.contains("zone"),
            "{text}: {err}"
        );
    }
}

// ---- D10: arrays ----

#[test]
fn vec_decoders_accept_one_dimensional_binary_arrays() {
    let ints = binary_array(
        oid::INT4,
        &[(2, 1)],
        &[Some(&1i32.to_be_bytes()), Some(&2i32.to_be_bytes())],
    );
    assert_eq!(
        Vec::<i64>::from_pg(&ints, oid::INT4_ARRAY, 1).unwrap(),
        vec![1, 2]
    );
    let texts = binary_array(oid::TEXT, &[(2, 1)], &[Some(b"a"), Some(b"b c")]);
    assert_eq!(
        Vec::<String>::from_pg(&texts, oid::TEXT_ARRAY, 1).unwrap(),
        vec!["a", "b c"]
    );
}

#[test]
fn vec_string_rejects_multidimensional_text_array() {
    // Before: split on every comma into ["{a", "b}", "{c", "d}"].
    assert!(Vec::<String>::from_pg(b"{{a,b},{c,d}}", oid::TEXT_ARRAY, 0).is_err());
}

#[test]
fn vec_decoders_refuse_binary_shapes_a_vec_cannot_hold() {
    let one = 1i32.to_be_bytes();
    let two_d = binary_array(oid::INT4, &[(1, 1), (1, 1)], &[Some(&one)]);
    let bounded = binary_array(oid::INT4, &[(1, 0)], &[Some(&one)]);
    let with_null = binary_array(oid::INT4, &[(2, 1)], &[Some(&one), None]);
    for bytes in [&two_d, &bounded, &with_null] {
        assert!(Vec::<i64>::from_pg(bytes, oid::INT4_ARRAY, 1).is_err());
    }
    // Binary int4 elements are not UTF-8 text.
    let ints = binary_array(oid::INT4, &[(1, 1)], &[Some(&one)]);
    assert!(Vec::<String>::from_pg(&ints, oid::INT4_ARRAY, 1).is_err());
}

#[test]
fn pg_array_text_and_binary_agree() {
    use qail_pg::{ArrayDimension, PgArray};

    let text = PgArray::<i64>::from_pg(b"[0:1][3:4]={{1,NULL},{3,4}}", oid::INT4_ARRAY, 0).unwrap();
    let (one, three, four) = (1i32.to_be_bytes(), 3i32.to_be_bytes(), 4i32.to_be_bytes());
    let bytes = binary_array(
        oid::INT4,
        &[(2, 0), (2, 3)],
        &[Some(&one), None, Some(&three), Some(&four)],
    );
    let binary = PgArray::<i64>::from_pg(&bytes, oid::INT4_ARRAY, 1).unwrap();
    assert_eq!(text, binary);
    assert_eq!(
        text.dimensions(),
        &[
            ArrayDimension {
                len: 2,
                lower_bound: 0
            },
            ArrayDimension {
                len: 2,
                lower_bound: 3
            }
        ]
    );
    assert_eq!(text.elements(), &[Some(1), None, Some(3), Some(4)]);
}

#[test]
fn bytea_escape_text_rejects_invalid_sequences() {
    for bad in [&br"\"[..], br"\9", br"\400", br"\00", br"a\b"] {
        assert!(Vec::<u8>::from_pg(bad, oid::BYTEA, 0).is_err());
    }
    // Text with no backslash is the data itself, as before.
    assert_eq!(
        Vec::<u8>::from_pg(b"plain", oid::BYTEA, 0).unwrap(),
        b"plain"
    );
}

#[test]
fn numeric_infinity_converts_to_f64_and_refuses_i64() {
    let inf = Numeric::from_pg(&numeric_header(0xD000), oid::NUMERIC, 1).unwrap();
    let neg = Numeric::from_pg(&numeric_header(0xF000), oid::NUMERIC, 1).unwrap();
    assert_eq!(inf.to_f64().unwrap(), f64::INFINITY);
    assert_eq!(neg.to_f64().unwrap(), f64::NEG_INFINITY);
    assert!(inf.to_i64().is_err());
    assert!(inf.to_i64_exact().is_err());
}

#[test]
fn temporal_infinity_is_explicit() {
    let inf = timestamp_text("infinity").unwrap();
    assert!(inf.is_infinity() && !inf.is_finite());
    assert_eq!(inf, Timestamp::INFINITY);
    assert!(inf.try_to_unix_usec().is_err());
    assert!(Timestamp::NEG_INFINITY.try_to_unix_usec().is_err());
    let epoch = timestamp_text("1970-01-01 00:00:00").unwrap();
    assert_eq!(epoch.try_to_unix_usec().unwrap(), 0);

    assert!(date_text("-infinity").unwrap().is_neg_infinity());
    assert!(date_text("2026-09-30").unwrap().is_finite());
    assert_eq!(Time::END_OF_DAY.hour(), 24);
    assert_eq!(Time::END_OF_DAY.minute(), 0);
    // Constructors keep rejecting hour 24.
    assert!(Time::try_new(24, 0, 0, 0).is_err());
}

#[cfg(feature = "chrono")]
#[test]
fn chrono_text_fallback_requires_the_zone_its_type_carries() {
    use chrono::{DateTime, Utc};

    // timestamptz text without an offset is not server output; no UTC guess.
    assert!(DateTime::<Utc>::from_pg(b"0044-03-15 12:00:00 BC", oid::TIMESTAMPTZ, 0).is_err());
    // timestamp text never has an offset.
    assert!(DateTime::<Utc>::from_pg(b"1900-01-01 05:21:10+05:21:10", oid::TIMESTAMP, 0).is_err());
    let tz =
        DateTime::<Utc>::from_pg(b"1900-01-01 05:21:10+05:21:10", oid::TIMESTAMPTZ, 0).unwrap();
    assert_eq!(tz.to_rfc3339(), "1900-01-01T00:00:00+00:00");
}
