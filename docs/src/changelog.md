# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.0)

- Pipeline APIs are now consistently named around `pipeline_execute_*` across driver/connection/pool surfaces.
- Added `AstPipelineMode` (`Auto`/`OneShot`/`Cached`) and auto planner surfaces (`AutoCountPlan`, `AutoCountPath`) for runtime strategy introspection.
- Added prepared AST handle execution (`PreparedAstQuery`) for repeated zero-reencode hot-path calls.
- Strengthened cached pipeline state rollback and protocol desync guards.
- Refreshed benchmark docs with reproducible RTT-aware pattern measurements.
