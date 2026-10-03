# Applied-scope AST migration

`with_rls()` on an INSERT records its scope in `Qail::conflict_update_scope`.
`on_conflict_update()` copies that scope into the `DO UPDATE ... WHERE` guard,
whichever of the two builders is called first. When the guard is false the
conflicting row is left unchanged and the statement reports zero affected and
zero returned rows.

This is a breaking change for exhaustive Rust struct literals and for readers of
scoped serialized ASTs. Publish it as a breaking release.

## Rust callers

Existing builder signatures are unchanged. Construct intentionally unscoped
commands with builders or update syntax:

```rust
use qail_core::{Action, Qail};

let command = Qail {
    action: Action::Add,
    table: "rows".into(),
    ..Qail::default()
};
```

An exhaustive literal must initialize `conflict_update_scope: Vec::new()`, and
an exhaustive pattern must bind the field or use `..`. An empty field applies no
scope: call `with_rls(&trusted_context)?` where your isolation contract needs AST
scoping. Never derive scope from an INSERT's tenant value.

Behavior of the conflict builders:

- Explicit `where_conditions` survive replacing the conflict clause, including a
  temporary `on_conflict_nothing()`.
- A later `with_rls()` replaces the guard on the same column.
- Native encoding rejects a DO UPDATE that is missing a recorded scope guard or
  that assigns a scoped column.
- Arbitrary later AST mutation is not re-scoped. The field is not an
  authorization credential.

## Serialized forms

| Form | Unscoped command | Command with applied scope |
| --- | --- | --- |
| Serde JSON | flat `Qail` object | `{ "qail_ast_version": 2, "conflict_update_scope": [...], "command": {...} }` |
| Binary wire | `QWB2` | `QWB3` |
| Text wire | `QAIL-CMD/1` / `QAIL-CMDS/1` | `QAIL-CMD/2` / `QAIL-CMDS/2` (AST JSON body) |

The envelope is also used for scoped commands nested inside another command.
Text v2 is also used for any command with an ON CONFLICT clause, because the
canonical text form does not carry conflict clauses.

Decoding rules:

- Unsupported or missing versions, an empty scope array, duplicate fields,
  unknown fields, and a bare top-level `conflict_update_scope` are rejected.
- Binary framing must match the content: a scoped AST in `QWB2` or an unscoped
  AST in `QWB3` is rejected.
- Text v2 bodies go through the same size limits and sanitization as binary.
- Readers built before this change reject every scoped form, because the
  envelope has no top-level `action`. Never strip the envelope or rewrite the
  header to make such a reader accept it.
- Only self-describing Serde formats (JSON) are supported.

`Display`, `to_string()` and `to_sql()` are not lossless transport. Persist with
`encode_cmd_text` / `encode_cmd_binary`.

## Rolling upgrade

Writers switch format automatically; there is no feature flag.

1. Inventory producers, stores, relays and consumers, including persisted
   workflow payloads. Fix exhaustive literals and patterns.
2. Fence producers so an upgraded process cannot send scoped payloads to a
   reader that has not been upgraded.
3. Upgrade every reader, relay and native executor. A reader that can parse a
   QWB2 command with an explicit conflict predicate but runs the earlier native
   encoder still drops that predicate.
4. Upgrade the remaining producers, regenerate fingerprints derived from wire
   text of conflict commands, verify the route end to end, then resume.

Rollback has the same constraint: an earlier reader cannot read persisted v2 or
QWB3 payloads. Stop the route and keep those payloads for a compatible reader.
Scope already lost from a payload cannot be recovered; rebuild such commands
from trusted intent and context.
