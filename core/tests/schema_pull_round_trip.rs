//! Security-relevant schema metadata must survive `schema.qail` text and stay
//! fail-closed in checked state diff.

use qail_core::migrate::{diff_schemas_checked, parse_qail, to_qail_string};

/// Text `qail pull` wrote for the PG 18 fixture (F10/F11/F13/E14/B10).
const PULLED: &str = r#"# QAIL Schema

extension "btree_gist" version "1.8"

table bookings {
  id INT primary_key check(id > 0) check_name bookings_id_positive not_valid
  tenant_id INT not_null check(tenant_id < 1000000) check_name bookings_tenant_bounded not_enforced
  room_id INT references rooms(id) not_valid
  author_id INT references rooms(id) not_enforced
  starts_at TIMESTAMPTZ not_null
  ends_at TIMESTAMPTZ not_null
  active BOOLEAN not_null default true
  foreign_key (tenant_id, room_id) references rooms(tenant_id, id) constraint bookings_room_fk match_full on_delete set_null(room_id)
  exclusion bookings_no_overlap EXCLUDE USING gist (room_id WITH =, tstzrange(starts_at, ends_at) WITH &&) WHERE (active) DEFERRABLE INITIALLY DEFERRED
}

table handles {
  id INT primary_key
  org_id INT
  handle TEXT
  enable_rls
}

table rooms {
  id INT primary_key
  tenant_id INT not_null
}

unique index handles_handle_key on handles (handle) nulls_not_distinct
unique index handles_org_handle_key on handles (org_id, handle) nulls_not_distinct
unique index handles_org_partial on handles (org_id) include (handle) nulls_not_distinct where (id > 0)
unique index rooms_tenant_id_id_key on rooms (tenant_id, id)
view v_barrier security_barrier $$
SELECT id,
    tenant_id
   FROM bookings
$$

view v_local check_option local $$
SELECT id,
    org_id
   FROM handles
  WHERE (org_id > 0)
$$

view v_all security_invoker security_barrier check_option cascaded $$
SELECT id,
    org_id
   FROM handles
$$

policy handles_multi on handles for select to app_user, qail_app
  using $$ (org_id IS NOT NULL) $$

policy handles_public on handles for select
  using $$ true $$

policy handles_single on handles for update to app_user
  using $$ true $$
  with_check $$ true $$

"#;

#[test]
fn pulled_security_metadata_round_trips_through_schema_text() {
    let schema = parse_qail(PULLED).expect("pulled text must parse");
    assert_eq!(to_qail_string(&schema), PULLED);
}

#[test]
fn quoted_and_comma_role_names_stay_single_roles() {
    let input = r#"table t {
  id INT primary_key
}

policy p on t for select to "space role", "a,b", plain_role
  using $$ true $$

"#;
    let schema = parse_qail(input).expect("quoted roles must parse");
    let policy = &schema.policies[0];
    assert_eq!(policy.roles(), vec!["space role", "a,b", "plain_role"]);
    assert_eq!(to_qail_string(&schema), format!("# QAIL Schema\n\n{input}"));
}

#[test]
fn policy_role_list_rejects_public_with_other_roles() {
    let input =
        "table t {\n  id INT primary_key\n}\n\npolicy p on t for select to public, app_user\n";
    let err = parse_qail(input).expect_err("PUBLIC plus roles is ignored by PostgreSQL");
    assert!(err.contains("PUBLIC"), "{err}");
}

#[test]
fn checked_diff_refuses_unsupported_markers() {
    let schema = parse_qail(
        "unsupported \"table events is partitioned (RANGE (created_at)); partitioning is not modelled\"\n\ntable events {\n  id BIGINT not_null\n  enable_rls\n  force_rls\n}\n",
    )
    .expect("marker must parse");
    assert_eq!(schema.unsupported.len(), 1);
    let err = diff_schemas_checked(&schema, &schema).expect_err("markers must fail closed");
    assert!(err.contains("objects marked unsupported by pull"), "{err}");
}

#[test]
fn checked_diff_allows_unchanged_constraint_state_and_refuses_new_state() {
    let base = "table rooms {\n  id INT primary_key\n}\n\n";
    let unchanged = parse_qail(&format!(
        "{base}table bookings {{\n  id INT primary_key check(id > 0) check_name bookings_id_positive not_valid\n  room_id INT references rooms(id) not_valid\n}}\n"
    ))
    .expect("parse");
    assert!(
        diff_schemas_checked(&unchanged, &unchanged)
            .expect("identical state needs no operation")
            .is_empty()
    );

    let before = parse_qail(&format!(
        "{base}table bookings {{\n  id INT primary_key\n}}\n"
    ))
    .expect("parse");
    let added = parse_qail(&format!(
        "{base}table bookings {{\n  id INT primary_key\n  room_id INT references rooms(id) not_valid\n}}\n"
    ))
    .expect("parse");
    let err = diff_schemas_checked(&before, &added).expect_err("NOT VALID FK creation");
    assert!(err.contains("NOT VALID"), "{err}");

    let validated = parse_qail(&format!(
        "{base}table bookings {{\n  id INT primary_key check(id > 0) check_name bookings_id_positive\n  room_id INT references rooms(id) not_valid\n}}\n"
    ))
    .expect("parse");
    let err = diff_schemas_checked(&unchanged, &validated).expect_err("CHECK state change");
    assert!(err.contains("CHECK"), "{err}");

    let excluded = parse_qail(&format!(
        "{base}table bookings {{\n  id INT primary_key\n  exclusion bookings_no_dup EXCLUDE USING btree (id WITH =)\n}}\n"
    ))
    .expect("parse");
    let err = diff_schemas_checked(&before, &excluded).expect_err("EXCLUDE creation");
    assert!(err.contains("EXCLUDE"), "{err}");
}

#[test]
fn transpiler_preview_matches_native_fail_closed_rules() {
    use qail_core::ast::{
        Action, Constraint, Expr, ForeignKeyOptions, IndexDef, Qail, TableConstraint,
        ViewCheckOption,
    };
    use qail_core::migrate::policy::RlsPolicy;
    use qail_core::transpiler::ToSql;

    let not_valid_fk = TableConstraint::ForeignKey {
        name: None,
        columns: vec!["room_id".to_string()],
        ref_table: "rooms".to_string(),
        ref_columns: vec!["id".to_string()],
        on_delete: Some("SET NULL".to_string()),
        on_update: None,
        deferrable: None,
        options: ForeignKeyOptions {
            match_full: true,
            on_delete_columns: vec!["room_id".to_string()],
            not_valid: true,
            not_enforced: false,
        },
    };
    let alter = Qail {
        action: Action::Alter,
        table: "bookings".to_string(),
        table_constraints: vec![not_valid_fk.clone()],
        ..Default::default()
    };
    assert_eq!(
        alter.to_sql(),
        "ALTER TABLE bookings ADD FOREIGN KEY (room_id) REFERENCES rooms(id) MATCH FULL ON DELETE SET NULL (room_id) NOT VALID"
    );
    let make = Qail {
        action: Action::Make,
        table: "bookings".to_string(),
        columns: vec![Expr::Def {
            name: "id".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::PrimaryKey],
        }],
        table_constraints: vec![not_valid_fk],
        ..Default::default()
    };
    assert!(make.to_sql().starts_with("/* ERROR"), "{}", make.to_sql());

    let bad_exclude = Qail {
        action: Action::Alter,
        table: "bookings".to_string(),
        table_constraints: vec![TableConstraint::Exclude {
            name: "x".to_string(),
            definition: "EXCLUDE USING gist (a WITH =); DROP TABLE rooms".to_string(),
        }],
        ..Default::default()
    };
    assert!(bad_exclude.to_sql().starts_with("/* ERROR"));

    let view = Qail {
        action: Action::CreateView,
        table: "v".to_string(),
        payload: Some("SELECT id FROM t WHERE id > 0".to_string()),
        view_security_barrier: true,
        view_check_option: Some(ViewCheckOption::Cascaded),
        ..Default::default()
    };
    assert_eq!(
        view.to_sql(),
        "CREATE VIEW v WITH (security_barrier = true) AS SELECT id FROM t WHERE id > 0 WITH CASCADED CHECK OPTION"
    );
    let matview = Qail {
        action: Action::CreateMaterializedView,
        ..view
    };
    assert!(matview.to_sql().starts_with("/* ERROR"));

    let mixed = Qail {
        action: Action::CreatePolicy,
        policy_def: Some(RlsPolicy::create("p", "t").to_roles(["public", "app_user"])),
        ..Default::default()
    };
    assert!(mixed.to_sql().starts_with("/* ERROR"));

    let index = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "i".to_string(),
            table: "t".to_string(),
            columns: vec!["a".to_string()],
            unique: false,
            nulls_not_distinct: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(index.to_sql().starts_with("/* ERROR"));
}

#[test]
fn checked_diff_refuses_nulls_not_distinct_change_on_existing_index() {
    let table = "table handles {\n  id INT primary_key\n  handle TEXT\n}\n\n";
    let distinct = parse_qail(&format!(
        "{table}unique index handles_handle_key on handles (handle)\n"
    ))
    .expect("parse");
    let not_distinct = parse_qail(&format!(
        "{table}unique index handles_handle_key on handles (handle) nulls_not_distinct\n"
    ))
    .expect("parse");
    let err = diff_schemas_checked(&distinct, &not_distinct)
        .expect_err("NULLS NOT DISTINCT change must not be ignored");
    assert!(err.contains("nulls_not_distinct"), "{err}");
}
