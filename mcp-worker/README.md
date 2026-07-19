# qail-mcp-worker

Remote MCP server for QAIL, live at `https://dev.qail.io/mcp`.

The server has two halves. The **compute tools** (parse, format, transpile,
explain, schema summary, cookbook) are answered by `handle_rpc`, the WebAssembly
build of the [`qail-mcp`](../mcp) crate, so they run byte-identical logic to the
local stdio server and cannot drift from it. The **knowledge tools** (search,
doc, syntax, examples, map) are TypeScript over an in-bundle corpus and never
enter WASM. `dispatch()` in `src/index.ts` merges the two for `tools/list` and
`resources/list` and routes calls to whichever half owns the name.

## Build and run

```bash
npm install
npm run dev          # builds wasm, then wrangler dev
npm run deploy       # builds wasm, then wrangler deploy
```

`build.sh` needs two tools on `PATH`:

```bash
cargo install wasm-bindgen-cli
brew install binaryen        # provides wasm-opt
```

Output lands in `src/wasm/`, which is gitignored — the binary is built in CI and
never committed.

## Transport

MCP Streamable HTTP, protocol revision `2025-06-18`, **stateless**.

| Request | Response |
|---|---|
| `POST /mcp` with `id` | `200` + `application/json` |
| `POST /mcp` without `id` (notification) | `202`, empty body |
| `POST /mcp` malformed JSON | `400` + JSON-RPC `-32700` |
| `POST /mcp` body > 128 KB | `413` |
| `GET` / `DELETE /mcp` | `405` |
| `OPTIONS /mcp` | `204` |
| `GET /mcp/health` | build metadata |

The server holds no per-session state and advertises no server-initiated
streams, so it never issues `Mcp-Session-Id` and answers with plain JSON. The
spec lets the server choose between `application/json` and `text/event-stream`,
so this is conformant rather than a shortcut. Clients that send
`Accept: text/event-stream` without `application/json` get the single response
as one SSE frame.

Clients speaking only the deprecated 2024-11-05 HTTP+SSE transport
(`GET /sse` → endpoint event → `POST /messages`) are **not** supported; that
would require a genuinely stateful second transport.

## Security

**Origin validation.** The transport spec requires it, to stop a page on an
attacker's domain from driving this server with the victim's network position
(DNS rebinding). A present-but-unrecognised `Origin` gets `403`; an absent one
is allowed, because native clients (Claude Desktop, Code, curl) never send it.
The allowlist is `ALLOWED_ORIGINS` in `src/index.ts`, plus any localhost origin
so the MCP Inspector works locally. CORS echoes the validated origin rather than
`*` — `*` would contradict the check it sits next to.

**Rate limiting.** A Durable Object (`src/ratelimit.ts`) caps 120 requests per
minute per IP. This is separate from `limits.cpu_ms`, which bounds a single
pathological request and does nothing about a flood of cheap ones.

Cloudflare's native `ratelimits` binding was evaluated first and rejected. It is
documented as intentionally permissive and eventually consistent, which makes it
unsuitable for a hard per-IP cap — measured here, a 200-request burst against a
configured limit of 120 was not throttled at all. That is consistent with its
documented behaviour, not evidence of a defect.

Failure handling is classified rather than swallowed:

| Condition | Response |
|---|---|
| over budget | `429` + `Retry-After` |
| DO `.overloaded` | `429` — object overload from one IP *is* the abuse signal, so failing open here would pass exactly the traffic being limited |
| DO `.retryable` | one bounded retry, then fail open |
| anything else | fail open |

Every fail-open is logged as `ratelimit_bypass` and must be counted. An
uncounted bypass is indistinguishable from working protection: an earlier
version of this file swallowed exceptions in a catch, reported 160 allowed
against a limit of 120, and was described as verified.

Measured on a 200-request same-IP burst: **exactly 120 × 2xx / 80 × 429**, with
zero bypasses logged. The DO is single-threaded and the counter increment has no
`await` between read and write, so successful calls stop at exactly the limit.

**Input validation.** `qail-mcp` validates JSON-RPC envelopes (`jsonrpc` must be
`"2.0"`, `id` must be a string or number), requires complete `initialize`
params, and enforces each tool's advertised `inputSchema` — unknown arguments,
wrong types and out-of-enum values are all `-32602` rather than silently
defaulted.

## Two things that will bite you

**Use `--target web`, not `--target bundler`.** Cloudflare resolves a `.wasm`
import to a `WebAssembly.Module`, not to instantiated exports. The bundler
target assumes webpack-style WASM ESM integration and fails at runtime with
`wasm.__wbindgen_add_to_stack_pointer is not a function`. The web target plus an
explicit `initSync({ module })` at module scope is correct — and instantiating
once per isolate at startup is cheaper than per request.

**`wasm-opt` needs bulk memory enabled.** Current rustc emits `memory.copy`,
which binaryen's default feature set rejects:

```
[wasm-validator error] memory.copy operations require bulk memory operations
```

`build.sh` passes `--enable-bulk-memory-opt --enable-nontrapping-float-to-int
--enable-sign-ext`.

## Routes

`routes` in `wrangler.jsonc` are **active**. Worker routes do take precedence
over the `qail-web` Pages custom domain on the same hostname, verified against
the live site. Nothing was shadowed: Pages serves its homepage as a soft-404 for
every unknown path, so `/mcp` held no real content. To re-check:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://dev.qail.io/mcp
```

A Pages 404 page means the route is not taking effect. Note both patterns are
needed — a pattern without a wildcard matches only the exact path, so `/mcp`
alone would not serve `/mcp/health`.

## Size

676 KB raw, 260 KB gzipped after `wasm-opt -Oz`, against a 10 MiB compressed
Workers limit on paid plans.
