# Deploying the qail MCP Worker

Clean clone to live service at <https://dev.qail.io/mcp>.

Worker name `qail-mcp`. Account id `9cb94b30d11ba6cabb9f6dec788ce0af`.

---

## 1. Prerequisites

| Tool | Why | Install |
| --- | --- | --- |
| Rust (stable) | builds `qail-wasm` | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| `wasm32-unknown-unknown` | wasm target | `rustup target add wasm32-unknown-unknown` |
| `wasm-bindgen-cli` | JS glue | `cargo install wasm-bindgen-cli` |
| binaryen >= 121 (`wasm-opt`) | `-Oz` size pass | `brew install binaryen` (Linux: see below) |
| Node.js >= 20 | wrangler + knowledge builder | `brew install node` |

`wasm-bindgen-cli` **must match** the `wasm-bindgen` version in `Cargo.lock`. A mismatch fails
at bindgen time with a version-mismatch error, not at runtime.

**Do not `apt-get install binaryen` on Linux.** Ubuntu 24.04 packages binaryen 108, but
`build.sh` passes `--enable-bulk-memory-opt`, which binaryen only grew in **v121**. `wasm-opt`
hard-errors on an unrecognised option, and `build.sh` runs under `set -euo pipefail`, so the
build aborts. Install the official release tarball instead — CI pins the same version:

```bash
V=131
curl -fsSL -O https://github.com/WebAssembly/binaryen/releases/download/version_$V/binaryen-version_$V-x86_64-linux.tar.gz
tar -xzf binaryen-version_$V-x86_64-linux.tar.gz
export PATH="$PWD/binaryen-version_$V/bin:$PATH"
wasm-opt --version   # expect: wasm-opt version 131
```

Verified on: rustc 1.95.0, node v24.15.0, wrangler ^4.63.0, binaryen 131.

## 2. Build and deploy

```bash
git clone https://github.com/qail-io/qail.git && cd qail
cd mcp-worker && npm ci && cd ..

# 1. wasm artifact  -> mcp-worker/src/wasm/   (gitignored, MUST be built)
./mcp-worker/build.sh

# 2. knowledge base -> mcp-worker/src/knowledge/*.json  (committed; only if repo docs changed)
node mcp-worker/scripts/build-knowledge.mjs

# 3. deploy
cd mcp-worker
npx wrangler deploy
```

`npm run deploy` from `mcp-worker/` runs steps 1 and 3 together. It does **not** run step 2.

Dry run (no credentials needed, checks bundle assembles):

```bash
cd mcp-worker && npx wrangler deploy --dry-run --outdir /tmp/x
```

Expected bundle: **~1.62 MiB raw / ~500 KiB gzip**. A materially smaller number means
`src/wasm/` was empty and the build silently shipped without the parser.

## 3. Generated vs committed

| Path | State | Produced by |
| --- | --- | --- |
| `mcp-worker/src/wasm/{qail_wasm.js,qail_wasm_bg.wasm}` | **gitignored — build it every clone** | `mcp-worker/build.sh` |
| `mcp-worker/src/knowledge/{corpus.json,index.json,VERSION.json}` | **committed** | `scripts/build-knowledge.mjs` |
| `docs/generated/*.json` | **committed** | `cargo run -p qail-core --example knowledge_export -- --out docs/generated` |

A clean clone will **not** build without step 1. `.gitignore:115` excludes `mcp-worker/src/wasm/`.

`build-knowledge.mjs` is deterministic — identical inputs give byte-identical output
(verified by md5 across runs). It reads the repo from `QAIL_RS_PATH` (default `../..`).
Current corpus: **403 chunks**, stamped in `VERSION.json` with `qailCommit` and `qailVersion`.
Regenerate and commit whenever repo docs/examples change; CI can re-run it and diff to
detect staleness.

## 4. Two non-obvious build constraints

Both of these broke this build once. `build.sh` encodes them; do not "simplify" them away.

**`wasm-bindgen --target web`, not `--target bundler`.** Cloudflare Workers resolves a `.wasm`
import to a `WebAssembly.Module`, not to instantiated exports, so the module must be
instantiated explicitly via `initSync`. The `bundler` target assumes webpack-style WASM ESM
integration and fails at *runtime* with:

```
wasm.__wbindgen_add_to_stack_pointer is not a function
```

**`wasm-opt` needs bulk-memory enabled explicitly.** Current rustc emits `memory.copy`, which
binaryen's default feature set rejects as invalid. All three flags are required:

```
wasm-opt -Oz --enable-bulk-memory-opt --enable-nontrapping-float-to-int --enable-sign-ext
```

`--enable-bulk-memory-opt` is also what forces binaryen >= 121; the other two flags are old
and accepted everywhere. See §1 — this is the one prerequisite where the distro package is
too stale to work at all.

`wasm-opt` also writes to a temp file and then moves it — reading and writing the same path
in place is not guaranteed safe.

## 5. Credentials

The Worker itself needs **no wrangler secrets**. Its only binding is the `RATE_LIMITER`
Durable Object, declared in `wrangler.jsonc` and created by migration tag `v1`
(`new_sqlite_classes: ["RateLimiter"]`). There is no `wrangler secret put` step.

Deploy *authentication* only:

```bash
export CLOUDFLARE_API_TOKEN=...      # scope: Workers Scripts:Edit (+ Workers Routes:Edit)
export CLOUDFLARE_ACCOUNT_ID=9cb94b30d11ba6cabb9f6dec788ce0af
```

Interactive alternative: `npx wrangler login`.

Routes are declared in `wrangler.jsonc` and need zone `qail.io` on the same account:
`dev.qail.io/mcp` and `dev.qail.io/mcp/*`. Both patterns are required — a wildcard-less
route matches only the exact path, so `/mcp` alone would not serve `/mcp/health`.

## 6. Verify the deploy

```bash
curl -s https://dev.qail.io/mcp/health
```

Expected exactly:

```json
{"status":"ok","qail_version":"1.3.6","protocol_version":"2025-06-18",
 "transport":"streamable-http","stateless":true,"rate_limited":true}
```

`"rate_limited": false` means the Durable Object binding did not attach — the deploy is live
but unprotected. Re-check `wrangler.jsonc` bindings and redeploy.

Surface counts and a real parse:

```bash
M='{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
curl -s https://dev.qail.io/mcp -H 'Content-Type: application/json' -d "$M" |
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).result.tools.length))'
# 11   (resources/list -> 7, prompts/list -> 3)

curl -s https://dev.qail.io/mcp -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","id":2,
  "method":"tools/call","params":{"name":"qail_parse_query",
  "arguments":{"query":"get users","dialect":"postgres"}}}'
# result.isError == false  -> the wasm parser is live, not a stub
```

That last call is the load-bearing check: a deploy missing `src/wasm/` still returns
healthy on `/mcp/health` but fails every parse.

Logs: `cd mcp-worker && npx wrangler tail`.

## 7. Pre-deploy checks

```bash
cargo test -p qail-mcp                          # 14 tests
cargo run -p qail-core --example knowledge_export -- --out docs/generated
```

`knowledge_export` also lints every ` ```qail ` fence under `docs/` and **exits non-zero** if
one fails to parse. Escape hatch: `--allow-doc-failures`.

## 8. Conformance suite

`scripts/conformance.mjs` asserts the live protocol surface (JSON-RPC validation, Origin/CORS,
protocol-version negotiation, tool input schemas, surface counts) and the rate limiter. CI runs
it automatically after every deploy; run it by hand when you want the strict limiter check.

| Invocation | Rate-limit coverage |
| --- | --- |
| `node scripts/conformance.mjs --url https://dev.qail.io` | **full** — asserts exactly 120 allowed / 80 denied |
| `... --ratelimit-probe` | **partial** — 130 requests, asserts ≥ 1 `429` with `Retry-After` (what CI runs) |
| `... --skip-ratelimit` | **none** |

The strict split requires a **clean, unshared egress IP**: the limiter is a fixed 120-req/60s
window keyed on client IP, so anything else leaving your address spends the same budget. That
is why CI uses `--ratelimit-probe` — GitHub runners share NAT addresses. There are **no unit
tests** for `src/ratelimit.ts`; the strict split is verified only by running this script
manually. Back-to-back runs need ~60 s between them, which the script handles by default.

Pointing `--url` at a bare host root always resolves to that host's real `/mcp` endpoint. To
mutation-test the suite, give a base with a non-empty path (`https://dev.qail.io/docs`) or a
different host — the resolved endpoint is echoed before the first check.
