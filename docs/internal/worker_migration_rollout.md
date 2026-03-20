# Worker Migration Rollout Runbook (QAIL Native)

This runbook defines a full migration rollout from current worker DB flows to QAIL native migrations, with concrete gates for `engine.qail.io` worker paths (including fleet/transfer related tables used by checkout workflows).

## Scope

- State-based migrations (`qail migrate up/down old:new`)
- File-based migrations (`qail migrate apply`, `--direction down`)
- Modular schema source (`schema/` split per table + `_order.qail`)
- PG18 runtime verification on worker
- Gateway/worker regression safety checks

## Required capabilities and coverage

Covered in this worker validation:

1. Create/rename/drop/type-change with live data (state-based up/down)
2. File-based apply supports:
   - `drop` hints
   - `rename` hints
   - down direction execution
3. Transform hints are intentionally blocked in strict apply mode
4. Modular schema split with strict manifest (`_order.qail`) works
5. Migration version IDs are unique under rapid consecutive applies
6. Migration and gateway test suites are green for migration paths

## Rollout phases

### Phase 0: Freeze and baseline

1. Freeze legacy migration writer in worker deploy pipeline.
2. Export current schema snapshot from target DB.
3. Capture migration history and pending operational windows.

Commands:

```bash
qail pull postgres://... > baseline.qail
qail migrate status --url postgres://...
```

Gate:
- Baseline schema file committed and reviewed.

### Phase 1: Schema source normalization (modular)

1. Move from monolithic schema file to `schema/` modules per table/domain.
2. Add `schema/_order.qail` with strict manifest.

Minimal `_order.qail` pattern:

```qail
-- qail: strict-manifest
common
fleet/operators.qail
fleet/transfer.qail
```

Gate:
- `qail check schema/` passes.
- Strict manifest rejects unlisted modules.

### Phase 2: Migration model split (expand/backfill/contract)

Use file-based groups when rollout needs stepwise deploy safety:

- `*.expand.up.qail`: additive/safe changes only
- `*.backfill.up.qail`: data migration or chunked backfill
- `*.contract.up.qail`: drops/renames after code is switched

Commands:

```bash
qail migrate apply --phase expand
qail migrate apply --phase backfill --backfill-chunk-size 10000
qail migrate apply --phase contract --codebase ./src
```

Gate:
- Contract phase only after code references are clean.

### Phase 3: Staging/shadow validation

1. Run dry plan and impact check.
2. Run shadow migration and verify receipt.

Commands:

```bash
qail migrate plan old_schema:new_schema
qail migrate shadow old_schema:new_schema --url postgres://...
qail migrate promote --url postgres://...
```

Gate:
- Shadow run passes and receipt verification is valid.

### Phase 4: Production cutover

1. Apply with lock and safety policies enabled.
2. For destructive operations, require explicit approval.

Command pattern:

```bash
qail migrate up old_schema:new_schema --url postgres://... \
  --wait-for-lock --lock-timeout-secs 30
```

Only if policy-approved:

```bash
qail migrate up old_schema:new_schema --url postgres://... \
  --allow-destructive
```

Gate:
- Post-apply fingerprint and smoke checks pass.
- Worker critical flow checks pass (fleet transfer + checkout path).

### Phase 5: Rollback readiness

State-based rollback must use `current:target` direction.

```bash
qail migrate down current.qail:target.qail --url postgres://... --force
```

For file-based rollback:

```bash
qail migrate apply --direction down --url postgres://...
```

Gate:
- Rollback drill executed on staging with production-like data shape.

## Operational rules (important)

1. For `migrate down`, always supply `current:target`.
2. Include explicit reverse rename hints in rollback target schemas where needed.
3. Do not use transform hints in strict file-based apply (unsupported by design).
4. Keep `migrations.policy` conservative in production:
   - `destructive = "require-flag"`
   - `lock_risk = "require-flag"`
   - `require_shadow_receipt = true`
5. Prefer modular schema + strict manifest to prevent drift by hidden files.

## Test commands used on this worker

Migration internals:

```bash
cargo test -p qail migrations:: -- --nocapture
```

Modular schema loader:

```bash
cargo test -p qail-core schema_source -- --nocapture
```

Workflow wire persistence path:

```bash
cargo test -p qail-workflow -- --nocapture
```

Gateway regression surface (without building examples):

```bash
cargo test -p qail-gateway --lib --tests -- --nocapture
```

## Known caveats

1. `migrate apply --direction down` executes discovered down files in normal discovery order.
2. If a lower-numbered down file drops objects required by later down files, sequence can fail.
3. Keep down files order-safe, or use explicit `migrate rollback --to ...` planning for versioned reversions.

