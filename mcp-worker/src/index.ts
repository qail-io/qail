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

initSync({ module: wasmModule });

/** Protocol revision implemented by the qail-mcp crate. */
const PROTOCOL_VERSION = "2025-06-18";

/**
 * Revisions we accept in the MCP-Protocol-Version header. 2025-03-26 is
 * included because the spec says a server should assume it when the header is
 * absent, so rejecting it outright would be stricter than the spec allows.
 */
const SUPPORTED_PROTOCOL_VERSIONS = new Set(["2025-06-18", "2025-03-26"]);

/**
 * Reject oversized bodies before touching WASM. The parser caps input at 64 KB
 * (MAX_INPUT_LENGTH); this leaves room for JSON-RPC envelope overhead while
 * keeping a hostile payload away from the parser entirely.
 */
const MAX_BODY_BYTES = 128 * 1024;

const CORS_HEADERS: Record<string, string> = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, GET, DELETE, OPTIONS",
    "Access-Control-Allow-Headers":
        "Content-Type, Accept, Mcp-Session-Id, MCP-Protocol-Version, Authorization",
    // Exposed even though this server never sets Mcp-Session-Id: some clients
    // probe for it, and a missing expose-header reads as a CORS failure rather
    // than an absent header.
    "Access-Control-Expose-Headers": "Mcp-Session-Id, MCP-Protocol-Version",
    "Access-Control-Max-Age": "86400",
};

function baseHeaders(extra: Record<string, string> = {}): Record<string, string> {
    return {
        ...CORS_HEADERS,
        "MCP-Protocol-Version": PROTOCOL_VERSION,
        // Responses embed the request id, so nothing here is cacheable whole.
        "Cache-Control": "no-store",
        ...extra,
    };
}

/** A JSON-RPC error envelope for failures that occur outside the WASM dispatch. */
function rpcError(code: number, message: string, status: number): Response {
    return new Response(
        JSON.stringify({ jsonrpc: "2.0", id: null, error: { code, message } }),
        { status, headers: baseHeaders({ "Content-Type": "application/json" }) },
    );
}

/**
 * Some clients speak Streamable HTTP but only parse `text/event-stream`.
 * Emitting the single response as one SSE frame satisfies them without making
 * the server stateful or introducing a real stream.
 */
function sseResponse(body: string): Response {
    return new Response(`event: message\ndata: ${body}\n\n`, {
        status: 200,
        headers: baseHeaders({
            "Content-Type": "text/event-stream",
            Connection: "keep-alive",
        }),
    });
}

function wantsSse(request: Request): boolean {
    const accept = request.headers.get("Accept") ?? "";
    return accept.includes("text/event-stream") && !accept.includes("application/json");
}

async function handleMcpPost(request: Request): Promise<Response> {
    const declared = request.headers.get("MCP-Protocol-Version");
    if (declared && !SUPPORTED_PROTOCOL_VERSIONS.has(declared)) {
        return rpcError(
            -32600,
            `Unsupported MCP-Protocol-Version: ${declared}. This server implements ${PROTOCOL_VERSION}.`,
            400,
        );
    }

    const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
    if (declaredLength > MAX_BODY_BYTES) {
        return rpcError(-32600, "Request body too large", 413);
    }

    const body = await request.text();
    if (body.length > MAX_BODY_BYTES) {
        return rpcError(-32600, "Request body too large", 413);
    }
    if (body.trim() === "") {
        return rpcError(-32700, "Parse error: empty request body", 400);
    }

    // The crate returns undefined for notifications, which by JSON-RPC
    // definition have no response. Over HTTP that is 202 with an empty body.
    let response: string | undefined;
    try {
        response = handle_rpc(body);
    } catch (err) {
        // handle_rpc is written not to throw; reaching here means the module
        // itself is unhealthy rather than the request being malformed.
        return rpcError(-32603, `Internal error: ${err}`, 500);
    }

    if (response === undefined) {
        return new Response(null, { status: 202, headers: baseHeaders() });
    }

    // Malformed JSON produces a -32700 envelope rather than a throw, so the
    // status is corrected here to keep HTTP semantics honest.
    const status = response.includes('"code":-32700') ? 400 : 200;

    if (wantsSse(request)) {
        return sseResponse(response);
    }

    return new Response(response, {
        status,
        headers: baseHeaders({ "Content-Type": "application/json" }),
    });
}

/** Build metadata, for humans and for the docs-site drift check. */
function handleHealth(): Response {
    return new Response(
        JSON.stringify(
            {
                status: "ok",
                qail_version: version(),
                protocol_version: PROTOCOL_VERSION,
                transport: "streamable-http",
                stateless: true,
            },
            null,
            2,
        ),
        { status: 200, headers: baseHeaders({ "Content-Type": "application/json" }) },
    );
}

export default {
    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url);
        const path = url.pathname.replace(/\/+$/, "") || "/";

        if (request.method === "OPTIONS") {
            return new Response(null, { status: 204, headers: baseHeaders() });
        }

        if (path === "/mcp/health" || path === "/health") {
            return handleHealth();
        }

        if (path !== "/mcp" && path !== "/") {
            return rpcError(-32600, `Not found: ${url.pathname}`, 404);
        }

        switch (request.method) {
            case "POST":
                return handleMcpPost(request);

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
                        headers: baseHeaders({
                            "Content-Type": "application/json",
                            Allow: "POST, OPTIONS",
                        }),
                    },
                );

            default:
                return rpcError(-32600, `Method not allowed: ${request.method}`, 405);
        }
    },
};
