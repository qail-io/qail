# qail-mcp-worker

Remote MCP server for QAIL, intended for `https://dev.qail.io/mcp`.

This Worker is HTTP framing only. Every JSON-RPC message is passed to
`handle_rpc`, the WebAssembly build of the [`qail-mcp`](../mcp) crate, so the
remote server and the local stdio server run identical dispatch logic and cannot
drift apart.

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

`routes` in `wrangler.jsonc` are commented out pending verification that a
Worker route on `dev.qail.io/mcp` takes precedence over the `qail-web` Pages
custom domain on the same hostname. Verify with a stub before enabling:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://dev.qail.io/mcp
```

A Pages 404 page means the route is not taking effect. Note both patterns are
needed — a pattern without a wildcard matches only the exact path, so `/mcp`
alone would not serve `/mcp/health`.

## Size

676 KB raw, 260 KB gzipped after `wasm-opt -Oz`, against a 10 MiB compressed
Workers limit on paid plans.
