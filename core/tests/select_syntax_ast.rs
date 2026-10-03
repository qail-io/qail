//! Serialized shape of the SELECT syntax AST additions and their text-DSL
//! round trip. Payloads written before these fields existed must decode,
//! and unused fields must not appear in serialized payloads.

use qail_core::ast::{
    CTEDef, CteMaterialization, FrameBound, FrameExclusion, LockMode, Qail, SetOp, WindowFrame,
};

#[test]
fn set_op_serde_keeps_existing_names() {
    assert_eq!(
        serde_json::to_string(&SetOp::UnionAll).unwrap(),
        "\"UnionAll\""
    );
    assert_eq!(
        serde_json::from_str::<SetOp>("\"ExceptAll\"").unwrap(),
        SetOp::ExceptAll
    );
    assert_eq!(
        serde_json::from_str::<SetOp>("\"IntersectAll\"").unwrap(),
        SetOp::IntersectAll
    );
}

#[test]
fn lock_fields_absent_from_serialized_ast_unless_set() {
    let plain = serde_json::to_value(Qail::get("jobs").for_update()).unwrap();
    assert!(plain.get("lock_nowait").is_none());
    assert!(plain.get("lock_of").is_none());
    let decoded: Qail = serde_json::from_value(plain).unwrap();
    assert_eq!(decoded.lock_mode, Some(LockMode::Update));
    assert!(!decoded.lock_nowait && decoded.lock_of.is_empty());

    let set = Qail::get("jobs j").for_update().lock_of(["j"]).nowait();
    let value = serde_json::to_value(&set).unwrap();
    let decoded: Qail = serde_json::from_value(value).unwrap();
    assert!(decoded.lock_nowait);
    assert_eq!(decoded.lock_of, vec!["j".to_string()]);
}

#[test]
fn cte_fields_absent_from_serialized_ast_unless_set() {
    let cmd = Qail::get("recent").with("recent", Qail::get("orders").columns(["id"]));
    let value = serde_json::to_value(&cmd.ctes[0]).unwrap();
    assert!(value.get("materialization").is_none());
    assert!(value.get("search").is_none());
    assert!(value.get("cycle").is_none());
    let decoded: CTEDef = serde_json::from_value(value).unwrap();
    assert_eq!(decoded.materialization, None);

    let cmd = cmd.cte_materialized();
    let value = serde_json::to_value(&cmd.ctes[0]).unwrap();
    let decoded: CTEDef = serde_json::from_value(value).unwrap();
    assert_eq!(
        decoded.materialization,
        Some(CteMaterialization::Materialized)
    );
}

#[test]
fn frame_without_exclusion_serializes_as_before() {
    let frame = WindowFrame::Rows {
        start: FrameBound::UnboundedPreceding,
        end: FrameBound::CurrentRow,
        exclude: FrameExclusion::NoOthers,
    };
    let json = serde_json::to_string(&frame).unwrap();
    assert_eq!(
        json,
        r#"{"Rows":{"start":"UnboundedPreceding","end":"CurrentRow"}}"#
    );
    assert_eq!(serde_json::from_str::<WindowFrame>(&json).unwrap(), frame);

    let excluded = WindowFrame::Groups {
        start: FrameBound::Preceding(1),
        end: FrameBound::CurrentRow,
        exclude: FrameExclusion::Ties,
    };
    let json = serde_json::to_string(&excluded).unwrap();
    assert_eq!(
        serde_json::from_str::<WindowFrame>(&json).unwrap(),
        excluded
    );
}

#[test]
fn row_lock_survives_text_round_trip() {
    for cmd in [
        Qail::get("jobs").for_update(),
        Qail::get("jobs").for_update_skip_locked(),
        Qail::get("jobs")
            .for_no_key_update()
            .lock_of(["jobs"])
            .nowait(),
        Qail::get("jobs").for_key_share(),
        Qail::get("jobs").for_share().skip_locked(),
    ] {
        let text = cmd.to_string();
        let decoded = qail_core::parse(&text).unwrap_or_else(|err| panic!("{text}: {err}"));
        assert_eq!(decoded.lock_mode, cmd.lock_mode, "{text}");
        assert_eq!(decoded.skip_locked, cmd.skip_locked, "{text}");
        assert_eq!(decoded.lock_nowait, cmd.lock_nowait, "{text}");
        assert_eq!(decoded.lock_of, cmd.lock_of, "{text}");
    }
}

#[test]
fn with_alias_reaches_every_alias_slot() {
    use qail_core::ast::builders::ExprExt;
    use qail_core::ast::{Expr, Value};

    let subscript = Expr::Subscript {
        expr: Box::new(Expr::Named("arr".to_string())),
        index: Box::new(Expr::Literal(Value::Int(1))),
        alias: None,
    }
    .with_alias("first");
    assert_eq!(subscript.alias_name(), Some("first"));

    let mut literal = Expr::Literal(Value::Int(1));
    assert!(!literal.set_alias("one"), "a literal has no alias slot");
    assert_eq!(literal, Expr::Literal(Value::Int(1)));
}

#[test]
fn dsl_slice_and_alias_edges() {
    // `[:name` is both a slice to column `name` and an index by `:name`.
    assert!(qail_core::parse("get t fields arr[:n]").is_err());
    let cmd = qail_core::parse("get t fields arr[:2], arr[ : ]").unwrap();
    assert!(matches!(
        &cmd.columns[0],
        qail_core::ast::Expr::ArraySlice {
            lower: None,
            upper: Some(_),
            ..
        }
    ));
    assert!(matches!(
        &cmd.columns[1],
        qail_core::ast::Expr::ArraySlice {
            lower: None,
            upper: None,
            ..
        }
    ));
    assert!(qail_core::parse("get t fields x as a as b").is_err());
    assert!(qail_core::parse("get t fields arr[]").is_err());
}

#[test]
fn dsl_expression_text_round_trip() {
    for text in [
        "get t fields (arr_append(a, 2))[1:2] as head",
        "get t fields not active as inactive",
        "get t fields case status when 'paid' then 1 else 0 end as code",
        "get t fields x::numeric(12,2)::text as amt",
    ] {
        let cmd = qail_core::parse(text).unwrap_or_else(|err| panic!("{text}: {err}"));
        let again =
            qail_core::parse(&cmd.to_string()).unwrap_or_else(|err| panic!("{cmd} : {err}"));
        assert_eq!(again.columns, cmd.columns, "{text}");
    }
}

#[test]
fn text_form_refuses_cte_options_it_cannot_carry() {
    let cmd = Qail::get("recent")
        .with("recent", Qail::get("orders").columns(["id"]))
        .cte_not_materialized();
    assert!(qail_core::fmt::Formatter::new().format(&cmd).is_err());
}
