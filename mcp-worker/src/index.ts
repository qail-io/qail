/**
 * Remote MCP server for QAIL, served at https://dev.qail.io/mcp
 *
 * This Worker is HTTP framing and nothing else. Every JSON-RPC message is
 * handed to `handle_rpc`, the WebAssembly build of the `qail-mcp` crate, so the
 * remote server and the local stdio server run identical dispatch logic.
 *
 * Transport: MCP Streamable HTTP, protocol revision 2025-06-18, stateless.
 * The server holds no per-session state and advertises no server-initiated
 * streams, so it answers with plain JSON and never issues an Mcp-Session-Id.
 * That is a conformant choice, not a shortcut: the spec lets the server pick
 * between `application/json` and `text/event-stream`.
 */

// Workers resolves a `.wasm` import to a WebAssembly.Module, not to
// instantiated exports, so the module is instantiated explicitly here.
// Instantiation is synchronous and happens once per isolate at startup rather
// than per request; Workers forbids async I/O in global scope, but
// `new WebAssembly.Instance` on an already-loaded module is allowed.
import wasmModule from "./wasm/qail_wasm_bg.wasm";
import { initSync, handle_rpc, version } from "./wasm/qail_wasm.js";
import { RateLimiter, checkRateLimit } from "./ratelimit.js";
import {
    KNOWLEDGE_TOOLS,
    KNOWLEDGE_RESOURCES,
    isKnowledgeTool,
    callKnowledgeTool,
    readKnowledgeResource,
} from "./knowledge.js";

initSync({ module: wasmModule });

/** Protocol revision implemented by the qail-mcp crate. */
const PROTOCOL_VERSION = "2025-06-18";

/**
 * Only 2025-06-18 is implemented, so only 2025-06-18 is accepted.
 *
 * An earlier version of this file also accepted 2025-03-26 on the theory that
 * the spec treats it as the default when the header is absent. That was wrong
 * to advertise: 2025-03-26 permits JSON-RPC batching, which the dispatcher does
 * not implement, so a client relying on the older revision would send a batch
 * and get a single-message response. Rejecting the header is honest; silently
 * accepting it is not.
 *
 * An absent header is still allowed — many clients omit it — and is treated as
 * this revision.
 */
const SUPPORTED_PROTOCOL_VERSIONS = new Set(["2025-06-18"]);

/**
 * Origins permitted to drive this server from a browser.
 *
 * The transport spec requires Origin validation to prevent DNS-rebinding
 * attacks, where a page on an attacker's domain resolves a hostname to a local
 * or internal address and then drives the MCP server with the victim's network
 * position. Non-browser clients (Claude Desktop, Code, curl) send no Origin at
 * all and are unaffected by this check.
 */
const ALLOWED_ORIGINS = new Set([
    "https://dev.qail.io",
    "https://qail.io",
    "https://www.qail.io",
    "https://qail-mcp.derylabergin.workers.dev",
]);

/** Localhost origins are allowed so the MCP Inspector can be run locally. */
function isAllowedOrigin(origin: string): boolean {
    if (ALLOWED_ORIGINS.has(origin)) return true;
    try {
        const { hostname, protocol } = new URL(origin);
        return (
            (protocol === "http:" || protocol === "https:") &&
            (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]")
        );
    } catch {
        return false;
    }
}

/**
 * Reject oversized bodies before touching WASM. The parser caps input at 64 KB
 * (MAX_INPUT_LENGTH); this leaves room for JSON-RPC envelope overhead while
 * keeping a hostile payload away from the parser entirely.
 */
const MAX_BODY_BYTES = 128 * 1024;

/**
 * CORS headers, echoing the caller's origin rather than `*`.
 *
 * `*` would contradict the Origin check below — it tells every browser page on
 * every domain that cross-origin reads are permitted. Only origins that pass
 * validation are echoed; requests with no Origin get no CORS headers, which is
 * correct because they are not browser requests.
 */
function corsHeaders(origin: string | null): Record<string, string> {
    if (!origin || !isAllowedOrigin(origin)) return {};
    return {
        "Access-Control-Allow-Origin": origin,
        // The response varies by request Origin, so caches must key on it.
        Vary: "Origin",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers":
            "Content-Type, Accept, Mcp-Session-Id, MCP-Protocol-Version, Authorization",
        // Exposed even though this server never sets Mcp-Session-Id: some
        // clients probe for it, and a missing expose-header reads as a CORS
        // failure rather than an absent header.
        "Access-Control-Expose-Headers": "Mcp-Session-Id, MCP-Protocol-Version",
        "Access-Control-Max-Age": "86400",
    };
}

function baseHeaders(
    origin: string | null = null,
    extra: Record<string, string> = {},
): Record<string, string> {
    return {
        ...corsHeaders(origin),
        "MCP-Protocol-Version": PROTOCOL_VERSION,
        // Responses embed the request id, so nothing here is cacheable whole.
        "Cache-Control": "no-store",
        ...extra,
    };
}

/** A JSON-RPC error envelope for failures that occur outside the WASM dispatch. */
function rpcError(
    code: number,
    message: string,
    status: number,
    origin: string | null = null,
): Response {
    return new Response(
        JSON.stringify({ jsonrpc: "2.0", id: null, error: { code, message } }),
        { status, headers: baseHeaders(origin, { "Content-Type": "application/json" }) },
    );
}

/**
 * Some clients speak Streamable HTTP but only parse `text/event-stream`.
 * Emitting the single response as one SSE frame satisfies them without making
 * the server stateful or introducing a real stream.
 */
function sseResponse(body: string, origin: string | null): Response {
    return new Response(`event: message\ndata: ${body}\n\n`, {
        status: 200,
        headers: baseHeaders(origin, {
            "Content-Type": "text/event-stream",
            Connection: "keep-alive",
        }),
    });
}

function wantsSse(request: Request): boolean {
    const accept = request.headers.get("Accept") ?? "";
    return accept.includes("text/event-stream") && !accept.includes("application/json");
}

/**
 * Route one JSON-RPC message between the two halves of this server.
 *
 * The compute tools (parse, transpile, explain, schema summary, cookbook) live
 * in WebAssembly and are answered by `handle_rpc`. The knowledge tools (search,
 * doc, syntax, examples, map) are pure TypeScript over an in-bundle corpus.
 * Neither half knows about the other, so this function merges their `tools/list`
 * and `resources/list` output and routes calls to whichever half owns the name.
 *
 * Anything not explicitly handled here — initialize, ping, prompts, the compute
 * tools — falls through to WASM unchanged, so the stdio server and this one stay
 * behaviourally identical for everything they share.
 */
function dispatch(body: string): string | undefined {
    let message: { id?: unknown; method?: unknown; params?: any };
    try {
        message = JSON.parse(body);
    } catch {
        // Let the WASM side produce the canonical -32700 envelope rather than
        // duplicating its error shape here.
        return handle_rpc(body);
    }

    const { id, method, params } = message;
    // Notifications carry no id and get no response, whichever half would own them.
    if (id === undefined || id === null) {
        return handle_rpc(body);
    }

    const ok = (result: unknown) => JSON.stringify({ jsonrpc: "2.0", id, result });

    switch (method) {
        case "tools/list": {
            const base = wasmResult(body);
            const tools = [...(base?.tools ?? []), ...KNOWLEDGE_TOOLS];
            return ok({ tools });
        }

        case "resources/list": {
            const base = wasmResult(body);
            const resources = [...(base?.resources ?? []), ...KNOWLEDGE_RESOURCES];
            return ok({ resources });
        }

        case "tools/call": {
            const name = params?.name;
            if (typeof name === "string" && isKnowledgeTool(name)) {
                try {
                    return ok(callKnowledgeTool(name, params?.arguments ?? {}));
                } catch (err) {
                    // Tool-level failures are results with isError, not JSON-RPC
                    // errors — matching how the WASM tools report problems.
                    return ok({
                        content: [{ type: "text", text: `${err}` }],
                        isError: true,
                    });
                }
            }
            return handle_rpc(body);
        }

        case "resources/read": {
            const uri = params?.uri;
            if (typeof uri === "string") {
                const text = readKnowledgeResource(uri);
                if (text !== null) {
                    return ok({
                        contents: [{ uri, mimeType: "text/markdown", text }],
                    });
                }
            }
            return handle_rpc(body);
        }

        default:
            return handle_rpc(body);
    }
}

/** Run a message through WASM and return just its `result`, if it produced one. */
function wasmResult(body: string): any {
    const raw = handle_rpc(body);
    if (raw === undefined) return undefined;
    try {
        return JSON.parse(raw)?.result;
    } catch {
        return undefined;
    }
}

async function handleMcpPost(request: Request, env: Env, origin: string | null): Promise<Response> {
    // Rate limiting, not the CPU cap. `limits.cpu_ms` bounds one pathological
    // request; it does nothing about a flood of cheap ones. Parsing is
    // CPU-bound and this endpoint is public and unauthenticated.
    const rate = await checkRateLimit(env.RATE_LIMITER, request);
    if (!rate.allowed) {
        return new Response(
            JSON.stringify({
                jsonrpc: "2.0",
                id: null,
                error: { code: -32000, message: "Rate limit exceeded. Try again shortly." },
            }),
            {
                status: 429,
                headers: baseHeaders(origin, {
                    "Content-Type": "application/json",
                    "Retry-After": String(rate.retryAfter),
                }),
            },
        );
    }

    const declared = request.headers.get("MCP-Protocol-Version");
    if (declared && !SUPPORTED_PROTOCOL_VERSIONS.has(declared)) {
        return rpcError(
            -32600,
            `Unsupported MCP-Protocol-Version: ${declared}. This server implements ${PROTOCOL_VERSION} only.`,
            400,
            origin,
        );
    }

    const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
    if (declaredLength > MAX_BODY_BYTES) {
        return rpcError(-32600, "Request body too large", 413, origin);
    }

    const body = await request.text();
    if (body.length > MAX_BODY_BYTES) {
        return rpcError(-32600, "Request body too large", 413, origin);
    }
    if (body.trim() === "") {
        return rpcError(-32700, "Parse error: empty request body", 400, origin);
    }

    // The crate returns undefined for notifications, which by JSON-RPC
    // definition have no response. Over HTTP that is 202 with an empty body.
    let response: string | undefined;
    try {
        response = dispatch(body);
    } catch (err) {
        // handle_rpc is written not to throw; reaching here means the module
        // itself is unhealthy rather than the request being malformed.
        return rpcError(-32603, `Internal error: ${err}`, 500, origin);
    }

    if (response === undefined) {
        return new Response(null, { status: 202, headers: baseHeaders(origin) });
    }

    // Malformed JSON produces a -32700 envelope rather than a throw, so the
    // status is corrected here to keep HTTP semantics honest.
    const status = response.includes('"code":-32700') ? 400 : 200;

    if (wantsSse(request)) {
        return sseResponse(response, origin);
    }

    return new Response(response, {
        status,
        headers: baseHeaders(origin, { "Content-Type": "application/json" }),
    });
}

/**
 * Cloudflare rate-limiting binding. Optional so `wrangler dev` and any
 * deployment without the binding still run — the limiter is then skipped rather
 * than crashing, and the absence is visible at /mcp/health.
 */
interface Env {
    RATE_LIMITER?: DurableObjectNamespace;
}

/** Build metadata, for humans and for the docs-site drift check. */
function handleHealth(env: Env, origin: string | null): Response {
    return new Response(
        JSON.stringify(
            {
                status: "ok",
                qail_version: version(),
                protocol_version: PROTOCOL_VERSION,
                transport: "streamable-http",
                stateless: true,
                rate_limited: Boolean(env.RATE_LIMITER),
            },
            null,
            2,
        ),
        { status: 200, headers: baseHeaders(origin, { "Content-Type": "application/json" }) },
    );
}

export { RateLimiter };

export default {
    async fetch(request: Request, env: Env): Promise<Response> {
        const url = new URL(request.url);
        const path = url.pathname.replace(/\/+$/, "") || "/";
        const origin = request.headers.get("Origin");

        // The transport spec requires servers to validate Origin, to stop a
        // page on an attacker's domain from driving this server with the
        // victim's network position (DNS rebinding). A browser always sends
        // Origin; native clients never do, so an absent Origin is allowed and
        // a present-but-unrecognised one is refused outright.
        if (origin !== null && !isAllowedOrigin(origin)) {
            return new Response(
                JSON.stringify({
                    jsonrpc: "2.0",
                    id: null,
                    error: { code: -32600, message: `Origin not allowed: ${origin}` },
                }),
                {
                    status: 403,
                    headers: {
                        "Content-Type": "application/json",
                        "Cache-Control": "no-store",
                        Vary: "Origin",
                    },
                },
            );
        }

        if (request.method === "OPTIONS") {
            return new Response(null, { status: 204, headers: baseHeaders(origin) });
        }

        if (path === "/mcp/health" || path === "/health") {
            return handleHealth(env, origin);
        }

        if (path !== "/mcp" && path !== "/") {
            return rpcError(-32600, `Not found: ${url.pathname}`, 404, origin);
        }

        switch (request.method) {
            case "POST":
                return handleMcpPost(request, env, origin);

            // This server offers no server-initiated stream and has no session
            // to tear down, so both are 405 rather than unimplemented stubs.
            case "GET":
            case "DELETE":
                return new Response(
                    JSON.stringify({
                        jsonrpc: "2.0",
                        id: null,
                        error: {
                            code: -32600,
                            message:
                                "This MCP server is stateless: it supports POST only. " +
                                "There is no server-initiated stream (GET) and no session to delete (DELETE).",
                        },
                    }),
                    {
                        status: 405,
                        headers: baseHeaders(origin, {
                            "Content-Type": "application/json",
                            Allow: "POST, OPTIONS",
                        }),
                    },
                );

            default:
                return rpcError(-32600, `Method not allowed: ${request.method}`, 405, origin);
        }
    },
};
