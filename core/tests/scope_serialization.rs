use qail_core::ast::{Expr, Qail};
use qail_core::rls::{RlsContext, init_scope_registries_from_tables};
use qail_core::wire::*;
use serde_json::{Value, json};
use std::sync::Once;

fn pending() -> Qail {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        init_scope_registries_from_tables(
            &[
                ("scope_transport", "tenant_id"),
                ("scope_owned", "tenant_id"),
            ],
            &[("scope_owned", "owner_id")],
        )
        .unwrap();
    });
    Qail::add("scope_transport")
        .set_value("id", 1)
        .set_value("status", "changed")
        .with_rls(&RlsContext::tenant("a'bound"))
        .unwrap()
}

fn frame(magic: &[u8; 4], payload: &Value) -> Vec<u8> {
    let payload = serde_json::to_vec(payload).unwrap();
    let mut out = magic.to_vec();
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend(payload);
    out
}

#[test]
fn scoped_json_is_an_envelope_and_rejects_unversioned_scope() {
    let cmd = pending();
    let encoded = serde_json::to_value(&cmd).unwrap();
    assert_eq!(encoded["qail_ast_version"], 2);
    assert!(encoded.get("action").is_none()); // baseline Qail requires action
    assert_eq!(
        serde_json::from_value::<Qail>(encoded.clone()).unwrap(),
        cmd
    );
    let mut unversioned = encoded["command"].clone();
    unversioned["conflict_update_scope"] = encoded["conflict_update_scope"].clone();
    assert!(serde_json::from_value::<Qail>(unversioned.clone()).is_err());
    assert!(decode_cmd_binary(&frame(b"QWB2", &unversioned)).is_err());
}

#[test]
fn missing_malformed_and_unsupported_scope_versions_fail_closed() {
    let encoded = serde_json::to_value(pending()).unwrap();
    for version in [
        json!(0),
        json!(1),
        json!(3),
        json!(-1),
        json!("2"),
        Value::Null,
    ] {
        let mut malformed = encoded.clone();
        malformed["qail_ast_version"] = version;
        assert!(
            serde_json::from_value::<Qail>(malformed.clone()).is_err(),
            "{malformed}"
        );
        assert!(decode_cmd_binary(&frame(b"QWB3", &malformed)).is_err());
    }
    for field in ["qail_ast_version", "conflict_update_scope", "command"] {
        let mut malformed = encoded.clone();
        malformed.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<Qail>(malformed).is_err());
    }
    for scope in [json!([]), Value::Null, json!({})] {
        let mut malformed = encoded.clone();
        malformed["conflict_update_scope"] = scope;
        assert!(serde_json::from_value::<Qail>(malformed).is_err());
    }
    let text = serde_json::to_string(&encoded).unwrap();
    let duplicated = format!("{{\"qail_ast_version\":2,{}", &text[1..]);
    assert!(serde_json::from_str::<Qail>(&duplicated).is_err());
    // A malformed envelope must not fall through to a valid raw command.
    let mut mixed = encoded["command"].clone();
    mixed["qail_ast_version"] = json!(99);
    assert!(serde_json::from_value::<Qail>(mixed).is_err());
}

#[test]
fn unscoped_payloads_keep_their_existing_formats() {
    let cmd = Qail::get("scope_transport").limit(2);
    let json = serde_json::to_value(&cmd).unwrap();
    assert!(json.get("action").is_some());
    assert!(json.get("qail_ast_version").is_none());
    assert_eq!(serde_json::from_value::<Qail>(json).unwrap(), cmd);
    let binary = encode_cmd_binary(&cmd).unwrap();
    assert_eq!(&binary[..4], b"QWB2");
    assert_eq!(decode_cmd_binary(&binary).unwrap(), cmd);
    assert!(encode_cmd_text(&cmd).starts_with("QAIL-CMD/1\n"));
    assert!(encode_cmds_text(&[cmd]).starts_with("QAIL-CMDS/1\n"));
}

#[test]
fn scoped_binary_requires_its_version_even_for_nested_commands() {
    let scoped = pending();
    for cmd in [
        scoped.clone(),
        Qail::get("scope_transport").with("pending", scoped),
    ] {
        let mut binary = encode_cmd_binary(&cmd).unwrap();
        assert_eq!(&binary[..4], b"QWB3");
        assert_eq!(decode_cmd_binary(&binary).unwrap(), cmd);
        binary[..4].copy_from_slice(b"QWB2");
        assert!(
            decode_cmd_binary(&binary)
                .unwrap_err()
                .contains("requires QWB3")
        );
        binary[..4].copy_from_slice(b"QWB4");
        assert!(decode_cmd_binary(&binary).is_err());
    }
}

#[test]
fn text_and_batch_transport_preserve_pending_and_active_scope() {
    let pending = pending();
    let global = pending.clone().with_rls(&RlsContext::global()).unwrap();
    let owned = Qail::add("scope_owned")
        .set_value("id", 1)
        .with_rls(&RlsContext::tenant("a").with_user("owner1"))
        .unwrap();
    let active = pending.clone().on_conflict_update(
        &["id"],
        &[("status", Expr::Named("EXCLUDED.status".into()))],
    );
    for cmd in [
        pending.clone(),
        global,
        owned,
        active,
        Qail::get("scope_transport").with("pending", pending),
    ] {
        assert_eq!(
            decode_cmd_binary(&encode_cmd_binary(&cmd).unwrap()).unwrap(),
            cmd
        );
        let encoded = encode_cmd_text(&cmd);
        assert!(encoded.starts_with("QAIL-CMD/2\n"));
        assert_eq!(decode_cmd_text(&encoded).unwrap(), cmd);
        let batch = vec![Qail::get("scope_transport"), cmd];
        let encoded = encode_cmds_text(&batch);
        assert!(encoded.starts_with("QAIL-CMDS/2\n"));
        assert_eq!(decode_cmds_text(&encoded).unwrap(), batch);
    }
    assert!(decode_cmd_text("QAIL-CMD/3\n0\n").is_err());
    assert!(decode_cmd_text("QAIL-CMD/2\n2\n{}").is_err());
    assert!(decode_cmds_text("QAIL-CMDS/3\n0\n").is_err());
    assert!(decode_cmds_text("QAIL-CMDS/2\n1\n2\n{}").is_err());
}

#[test]
fn text_transport_also_preserves_explicit_unscoped_conflict_guards() {
    let mut cmd = Qail::add("rows").set_value("id", 1).on_conflict_update(
        &["id"],
        &[("status", Expr::Named("EXCLUDED.status".into()))],
    );
    cmd.on_conflict
        .as_mut()
        .unwrap()
        .where_conditions
        .push(qail_core::ast::builders::eq("rows.status", "ready"));
    let encoded = encode_cmd_text(&cmd);
    assert!(encoded.starts_with("QAIL-CMD/2\n"));
    assert_eq!(decode_cmd_text(&encoded).unwrap(), cmd);
    assert!(
        decode_cmd_text(&encoded)
            .unwrap()
            .conflict_update_scope
            .is_empty()
    );
    assert_eq!(&encode_cmd_binary(&cmd).unwrap()[..4], b"QWB2");
}
