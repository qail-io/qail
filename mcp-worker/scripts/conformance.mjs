#!/usr/bin/env node
/**
 * Protocol + rate-limit conformance suite for the QAIL MCP Worker.
 *
 *   node scripts/conformance.mjs [--url https://dev.qail.io/mcp] [--skip-ratelimit]
 *                                [--ratelimit-probe] [--fresh] [--expect-version 1.3.5]
 *
 * Node builtins only (global `fetch`, `Promise.all`). No dependencies.
 *
 * Exit code is 0 only if every check passes.
 *
 * --------------------------------------------------------------------------
 * `--url` NORMALISATION — MATTERS WHEN MUTATION-TESTING THIS SUITE
 * --------------------------------------------------------------------------
 * `--url` names the BASE, and `/mcp` + `/mcp/health` are derived from it. Both
 * `https://host` and `https://host/mcp` therefore resolve to the same pair, by
 * design — and so does a bare `https://host/`, which is the sharp edge: pointing
 * this suite at a docs root to prove it can fail will instead silently re-test
 * the real endpoint and report PASS.
 *
 * A bare host root prints a loud stderr warning for exactly that reason. To
 * mutation-test the suite, give a base with a non-empty path (`https://host/docs`,
 * which resolves to `https://host/docs/mcp`) or a different host entirely. The
 * resolved endpoint is always echoed on stdout before the first check — read it.
 *
 * --------------------------------------------------------------------------
 * TIMING REQUIREMENT FOR THE RATE-LIMIT TEST — READ BEFORE RE-RUNNING
 * --------------------------------------------------------------------------
 * The limiter is a FIXED window: 120 requests per 60 s per client IP, counted
 * by a Durable Object keyed on the IP. It is *not* a sliding window and it is
 * *not* per-connection — every request this script makes from this machine
 * shares one budget with every other request from this machine.
 *
 * Two consequences:
 *
 *   1. The ~25 protocol checks above the burst consume budget from the very
 *      same window. Firing the 200-request burst immediately after them would
 *      see fewer than 120 allowed and the test would fail for the wrong
 *      reason. So by default this script SLEEPS just over one full window
 *      (WINDOW_WAIT_MS) before the burst, guaranteeing a clean window.
 *
 *   2. A naive back-to-back re-run of this script fails the same way: the
 *      previous run's 200 requests are still inside the window. Wait ~60 s
 *      between runs, or let the default sleep handle it.
 *
 * `--fresh` skips that sleep. Note this is a debugging escape hatch, not a
 * speed-up: the ~30 protocol checks that run first have already spent part of
 * the window, so a `--fresh` burst reports roughly 98 allowed rather than 120
 * and fails. It is only correct when the burst is genuinely the first traffic
 * in the window — e.g. when you have edited main() to run it alone.
 *
 * `--skip-ratelimit` drops the burst entirely — use it in fast/inner loops and
 * in any CI job that runs concurrently with another copy of itself, since two
 * runners behind one NAT egress IP share a budget and would both fail.
 *
 * `--ratelimit-probe` is the NAT-safe middle ground, and is what CI runs. It
 * fires PROBE_SIZE (> the 120 budget) requests and asserts only that AT LEAST
 * ONE came back 429 with a Retry-After. That is strictly weaker than the exact
 * 120/80 split, but it is the direction that survives a shared egress IP:
 * competing traffic only makes throttling MORE likely, never less. It catches
 * the failure that actually matters unattended — the limiter silently not
 * limiting at all (binding dropped, fail-open path taken, DO not attached).
 *
 * A preflight probe (`awaitCleanWindow`) refuses to start while this IP is
 * actively throttled, so a too-soon re-run exits 2 with an explanation rather
 * than reporting two dozen bogus protocol failures.
 */

const WINDOW_MS = 60_000;
const WINDOW_WAIT_MS = 63_000; // one window + slack for clock skew / flight time
const BURST_SIZE = 200;
const EXPECTED_ALLOWED = 120;
const EXPECTED_DENIED = BURST_SIZE - EXPECTED_ALLOWED;
// Comfortably above the 120 budget, so a working limiter must deny some even
// from a completely fresh window.
const PROBE_SIZE = 130;

const EXPECTED_PROTOCOL_VERSION = "2025-06-18";
const EXPECTED_TOOLS = 11;
const EXPECTED_RESOURCES = 7;
const EXPECTED_PROMPTS = 3;

// ---------------------------------------------------------------------------
// args
// ---------------------------------------------------------------------------

function parseArgs(argv) {
    const opts = {
        url: "http://localhost:8787",
        skipRateLimit: false,
        rateLimitProbe: false,
        fresh: false,
        expectVersion: null,
    };
    const takeValue = (arg, i) => {
        const next = argv[i + 1];
        if (!next) fatal(`${arg} requires a value`);
        return next;
    };
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (arg === "--skip-ratelimit") opts.skipRateLimit = true;
        else if (arg === "--ratelimit-probe") opts.rateLimitProbe = true;
        else if (arg === "--fresh") opts.fresh = true;
        else if (arg === "--url") {
            opts.url = takeValue(arg, i);
            i += 1;
        } else if (arg.startsWith("--url=")) {
            opts.url = arg.slice("--url=".length);
        } else if (arg === "--expect-version") {
            opts.expectVersion = takeValue(arg, i);
            i += 1;
        } else if (arg.startsWith("--expect-version=")) {
            opts.expectVersion = arg.slice("--expect-version=".length);
        } else if (arg === "--help" || arg === "-h") {
            console.log(
                "usage: node scripts/conformance.mjs [--url <base>] [--expect-version <semver>]\n" +
                    "                                   [--skip-ratelimit | --ratelimit-probe] [--fresh]",
            );
            process.exit(0);
        } else {
            fatal(`unknown argument: ${arg}`);
        }
    }
    if (opts.skipRateLimit && opts.rateLimitProbe) {
        fatal("--skip-ratelimit and --ratelimit-probe are mutually exclusive");
    }
    return opts;
}

function fatal(msg) {
    console.error(`conformance: ${msg}`);
    process.exit(2);
}

const opts = parseArgs(process.argv.slice(2));

// Accept either "https://host" or "https://host/mcp"; derive both endpoints.
// A bare host root is accepted too, but warns — see the header: it is the shape
// that silently turns a deliberate mutation test back into a real-endpoint run.
let ORIGIN_BASE;
try {
    const parsed = new URL(opts.url);
    const supplied = parsed.pathname.replace(/\/+$/, ""); // "", "/mcp", "/docs", ...
    if (supplied === "") {
        console.error(
            `conformance: WARNING — --url ${opts.url} has no path; testing ${parsed.origin}/mcp.\n` +
                `  A bare host root always resolves to the real MCP endpoint. If you meant to point\n` +
                `  this suite at a NON-MCP target to prove it can fail, use a non-empty path\n` +
                `  (e.g. ${parsed.origin}/docs) or a different host.`,
        );
    }
    parsed.pathname = supplied === "/mcp" ? "" : supplied;
    parsed.search = "";
    parsed.hash = "";
    ORIGIN_BASE = parsed.toString().replace(/\/$/, "");
} catch {
    fatal(`--url is not a valid URL: ${opts.url}`);
}

const MCP_URL = `${ORIGIN_BASE}/mcp`;
const HEALTH_URL = `${ORIGIN_BASE}/mcp/health`;
// The worker allows its own origin (and any localhost), so this works against
// both the deployed service and `wrangler dev`.
const ALLOWED_ORIGIN = new URL(ORIGIN_BASE).origin;

// ---------------------------------------------------------------------------
// tiny assertion harness
// ---------------------------------------------------------------------------

let passed = 0;
const failures = [];
let currentGroup = "";

function group(name) {
    currentGroup = name;
    console.log(`\n── ${name} ─────────────────────────────────────────`);
}

/**
 * Record one check. `expected` and `actual` are rendered on failure, so pass
 * whatever makes the failure diagnosable rather than a bare boolean.
 */
function assert(name, condition, expected, actual, hint) {
    if (condition) {
        passed += 1;
        console.log(`ok   ${name}`);
        return true;
    }
    const line = `FAIL ${name} — expected ${render(expected)} got ${render(actual)}`;
    console.log(hint ? `${line}\n     hint: ${hint}` : line);
    failures.push({ group: currentGroup, name, expected, actual, hint });
    return false;
}

function assertEqual(name, actual, expected, hint) {
    return assert(name, Object.is(actual, expected), expected, actual, hint);
}

function render(v) {
    if (typeof v === "string") return v;
    try {
        return JSON.stringify(v);
    } catch {
        return String(v);
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/** POST a raw body string. `payload` is sent verbatim — malformed JSON is a case. */
async function rawPost(payload, headers = {}) {
    const res = await fetch(MCP_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...headers },
        body: payload,
    });
    const text = await res.text();
    let json;
    try {
        json = text.length ? JSON.parse(text) : undefined;
    } catch {
        json = undefined;
    }
    return { res, status: res.status, text, json };
}

/** POST a JSON-RPC message object. */
function post(message, headers = {}) {
    return rawPost(JSON.stringify(message), headers);
}

function rpc(method, params, id = 1) {
    const msg = { jsonrpc: "2.0", id, method };
    if (params !== undefined) msg.params = params;
    return post(msg);
}

function callTool(name, args) {
    return rpc("tools/call", { name, arguments: args });
}

/** Error code from a JSON-RPC error envelope, or a describable stand-in. */
function errCode(r) {
    if (r.json?.error?.code !== undefined) return r.json.error.code;
    if (r.json?.result !== undefined) return "a result (no error)";
    return `no JSON-RPC error; body: ${r.text.slice(0, 160)}`;
}

/**
 * True when a `ping` actually succeeded.
 *
 * The allow-path checks below use this instead of `status !== 403` / `!== 400`.
 * Asserting the absence of one specific rejection cannot tell "allowed" apart
 * from "broken": a 500, a 404, or an HTML error page all satisfy `!== 403`, so
 * such a check passes against a server that has no MCP endpoint at all.
 */
function isOkPing(r) {
    return r.status === 200 && r.json?.result !== undefined;
}

/** Renders what actually came back, for the failure message of an allow-path check. */
function describeResponse(r) {
    if (r.json?.error !== undefined) return `HTTP ${r.status} with error ${render(r.json.error)}`;
    if (r.json?.result !== undefined) return `HTTP ${r.status} with a result`;
    return `HTTP ${r.status}, body: ${r.text.slice(0, 160)}`;
}

/** All text content blocks of a tools/call result, concatenated. */
function toolText(r) {
    const content = r.json?.result?.content;
    if (!Array.isArray(content)) return "";
    return content
        .filter((c) => typeof c?.text === "string")
        .map((c) => c.text)
        .join("\n");
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Refuse to start while this IP is still being throttled.
 *
 * Every protocol check below is a POST, so it spends the same 120/60s budget
 * as the burst. Running them while throttled turns all ~30 of them into
 * `-32000 Rate limit exceeded`, which renders as two dozen unrelated protocol
 * failures — the exact false alarm this suite exists to prevent. So probe
 * first, and treat "throttled" as a harness precondition (exit 2), never as a
 * conformance failure (exit 1).
 */
async function awaitCleanWindow() {
    const MAX_ATTEMPTS = 3;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
        const probe = await post({ jsonrpc: "2.0", id: "preflight", method: "ping" });
        if (probe.status !== 429) return;

        const retryAfter = Number(probe.res.headers.get("retry-after"));
        const waitMs = (Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : 60) * 1000 + 3_000;

        if (attempt === MAX_ATTEMPTS) {
            fatal(
                `endpoint is still rate-limiting this IP after ${MAX_ATTEMPTS} attempts.\n` +
                    `  The limiter is ${EXPECTED_ALLOWED} requests / ${WINDOW_MS / 1000}s per IP and the ` +
                    `protocol checks spend that same budget.\n` +
                    `  Something else is driving traffic from this IP, or a previous run is still in flight. ` +
                    `Wait a minute and retry.\n` +
                    `  This is a harness precondition, not a server conformance failure.`,
            );
        }
        console.log(
            `     preflight: throttled (429). Waiting ${Math.round(waitMs / 1000)}s for the window to clear ` +
                `[attempt ${attempt}/${MAX_ATTEMPTS - 1}]`,
        );
        await sleep(waitMs);
    }
}

// ---------------------------------------------------------------------------
// checks
// ---------------------------------------------------------------------------

async function transportSemantics() {
    group("transport semantics");

    const get = await fetch(MCP_URL, { method: "GET" });
    await get.text();
    assertEqual("GET /mcp is 405", get.status, 405);

    const del = await fetch(MCP_URL, { method: "DELETE" });
    await del.text();
    assertEqual("DELETE /mcp is 405", del.status, 405);

    const options = await fetch(MCP_URL, { method: "OPTIONS" });
    const optionsBody = await options.text();
    assertEqual("OPTIONS /mcp is 204", options.status, 204);
    assertEqual("OPTIONS /mcp has empty body", optionsBody.length, 0);

    // A JSON-RPC notification has no id and must get no response body.
    const notif = await rawPost(JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" }));
    assertEqual("notification is 202", notif.status, 202);
    assertEqual("notification body is empty", notif.text.length, 0);

    const health = await fetch(HEALTH_URL);
    const healthJson = await health.json().catch(() => ({}));
    assertEqual("GET /mcp/health is 200", health.status, 200);
    assertEqual("health.status", healthJson.status, "ok");
    assertEqual("health.protocol_version", healthJson.protocol_version, EXPECTED_PROTOCOL_VERSION);
    assertEqual("health.transport", healthJson.transport, "streamable-http");
    assertEqual("health.stateless", healthJson.stateless, true);
    assertEqual("health.rate_limited", healthJson.rate_limited, true);
    // Shape, not just truthiness: "unknown" or "" would sail through a
    // non-empty-string check while telling us the build stamped nothing.
    assert(
        "health.qail_version is a semver",
        typeof healthJson.qail_version === "string" && /^\d+\.\d+\.\d+/.test(healthJson.qail_version),
        "a semver like 1.3.5",
        healthJson.qail_version,
    );
    // Pinned only when CI passes the workspace version in, so the deployed wasm
    // is proven to be the commit that deployed it. Unpinned locally, since
    // hardcoding a literal here would break on every version bump.
    if (opts.expectVersion !== null) {
        assertEqual(
            "health.qail_version matches --expect-version",
            healthJson.qail_version,
            opts.expectVersion,
            "the live worker is not the build that was just deployed",
        );
    }
}

async function jsonRpcValidation() {
    group("JSON-RPC validation");

    const badVersion = await post({ jsonrpc: "1.0", id: 1, method: "ping" });
    assertEqual('jsonrpc "1.0" rejected', errCode(badVersion), -32600);

    const noVersion = await post({ id: 1, method: "ping" });
    assertEqual("missing jsonrpc field rejected", errCode(noVersion), -32600);

    for (const [label, id] of [
        ["object", { a: 1 }],
        ["array", [1, 2]],
        ["boolean", true],
    ]) {
        const r = await post({ jsonrpc: "2.0", id, method: "ping" });
        assertEqual(`id as ${label} rejected`, errCode(r), -32600);
    }

    const malformed = await rawPost("{not json");
    assertEqual("malformed JSON is HTTP 400", malformed.status, 400);
    assertEqual("malformed JSON is -32700", errCode(malformed), -32700);
}

async function initializeLifecycle() {
    group("initialize lifecycle");

    const noParams = await post({ jsonrpc: "2.0", id: 1, method: "initialize" });
    assertEqual("initialize without params is -32602", errCode(noParams), -32602);

    const good = await rpc("initialize", {
        protocolVersion: EXPECTED_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: "qail-conformance", version: "1.0.0" },
    });
    assertEqual(
        "initialize returns protocolVersion",
        good.json?.result?.protocolVersion,
        EXPECTED_PROTOCOL_VERSION,
        good.json?.error ? `server returned error: ${render(good.json.error)}` : undefined,
    );
}

async function originAndCors() {
    group("Origin / CORS");

    const evil = await post({ jsonrpc: "2.0", id: 1, method: "ping" }, { Origin: "https://evil.example" });
    assertEqual("disallowed Origin is 403", evil.status, 403);

    // Native MCP clients send no Origin at all; rejecting them would break them.
    const none = await post({ jsonrpc: "2.0", id: 1, method: "ping" });
    assert(
        "absent Origin is served",
        isOkPing(none),
        "HTTP 200 with a JSON-RPC result",
        describeResponse(none),
        "native stdio-style clients omit Origin entirely",
    );

    const allowed = await post(
        { jsonrpc: "2.0", id: 1, method: "ping" },
        { Origin: ALLOWED_ORIGIN },
    );
    assert(
        "allowed Origin is served",
        isOkPing(allowed),
        "HTTP 200 with a JSON-RPC result",
        describeResponse(allowed),
        `origin under test: ${ALLOWED_ORIGIN}`,
    );

    const acao = allowed.res.headers.get("access-control-allow-origin");
    assert(
        "CORS is not a wildcard",
        acao !== "*",
        'anything but "*"',
        acao,
        'a wildcard ACAO on a stateful endpoint lets any page call it; the worker must echo the origin',
    );
    assertEqual("CORS echoes the request Origin", acao, ALLOWED_ORIGIN);

    // The preflight must carry the same non-wildcard echo, or browsers block.
    const preflight = await fetch(MCP_URL, {
        method: "OPTIONS",
        headers: {
            Origin: ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    });
    await preflight.text();
    const preflightAcao = preflight.headers.get("access-control-allow-origin");
    assert("preflight CORS is not a wildcard", preflightAcao !== "*", 'anything but "*"', preflightAcao);
    assertEqual("preflight echoes the request Origin", preflightAcao, ALLOWED_ORIGIN);
}

async function protocolVersionNegotiation() {
    group("protocol version negotiation");

    const stale = await post(
        { jsonrpc: "2.0", id: 1, method: "ping" },
        { "MCP-Protocol-Version": "2025-03-26" },
    );
    assertEqual("unsupported MCP-Protocol-Version is 400", stale.status, 400);

    const absent = await post({ jsonrpc: "2.0", id: 1, method: "ping" });
    assert(
        "absent MCP-Protocol-Version is served",
        isOkPing(absent),
        "HTTP 200 with a JSON-RPC result",
        describeResponse(absent),
        "the header is optional; absence must not be treated as a mismatch",
    );

    const current = await post(
        { jsonrpc: "2.0", id: 1, method: "ping" },
        { "MCP-Protocol-Version": EXPECTED_PROTOCOL_VERSION },
    );
    assert(
        "current MCP-Protocol-Version is served",
        isOkPing(current),
        "HTTP 200 with a JSON-RPC result",
        describeResponse(current),
    );
}

async function toolInputSchemaEnforcement() {
    group("tool input schema enforcement");

    const unknownArg = await callTool("qail_parse_query", { query: "get users", bogus: true });
    assertEqual(
        "unknown argument rejected",
        errCode(unknownArg),
        -32602,
        "additionalProperties must be false, or typo'd args silently no-op",
    );

    const wrongType = await callTool("qail_parse_query", {
        query: "get users",
        parameterized: "false",
    });
    assertEqual(
        "wrong argument type rejected",
        errCode(wrongType),
        -32602,
        'the string "false" is truthy in JS — coercing it would invert the caller\'s intent',
    );

    const badEnum = await callTool("qail_parse_query", { query: "get users", dialect: "oracle" });
    assertEqual("out-of-enum value rejected", errCode(badEnum), -32602);

    const valid = await callTool("qail_parse_query", { query: "get users", dialect: "postgres" });
    assertEqual(
        "valid arguments succeed",
        valid.json?.result?.isError,
        false,
        valid.json?.error ? `server returned error: ${render(valid.json.error)}` : undefined,
    );
}

async function contentCorrectness() {
    group("content correctness");

    const cookbook = await callTool("qail_builder_cookbook", { topic: "schema" });
    const text = toolText(cookbook);
    assert(
        "cookbook returns text content",
        text.length > 0,
        "non-empty text content",
        cookbook.json?.error ? render(cookbook.json.error) : `${text.length} chars`,
    );
    assert(
        "cookbook uses QAIL brace syntax",
        text.includes("table users {"),
        'text containing "table users {"',
        text.includes("table users") ? "a table users declaration in some other syntax" : "no table users declaration",
    );
    // Anchored to a non-empty subject on purpose. A bare `!text.includes(...)`
    // is vacuously true when `text` is "", so it would pass against a server
    // that has no cookbook tool at all — the negative half of this repro would
    // be decorative. Every `!includes` assertion in this file must be guarded
    // the same way.
    assert(
        "cookbook does not emit SQL paren syntax",
        text.length > 0 && !text.includes("table users ("),
        'non-empty text with no occurrence of "table users ("',
        text.length === 0
            ? "no text content at all — nothing was inspected"
            : 'found "table users (" — SQL DDL leaking into QAIL examples teaches clients invalid syntax',
    );
}

async function surfaceCounts() {
    group("surface counts");

    const tools = await rpc("tools/list", {});
    assertEqual("tools/list count", tools.json?.result?.tools?.length, EXPECTED_TOOLS);

    const resources = await rpc("resources/list", {});
    assertEqual("resources/list count", resources.json?.result?.resources?.length, EXPECTED_RESOURCES);

    const prompts = await rpc("prompts/list", {});
    assertEqual("prompts/list count", prompts.json?.result?.prompts?.length, EXPECTED_PROMPTS);
}

/** Fire `n` concurrent pings; return statuses + Retry-After + elapsed ms. */
async function burst(n) {
    const body = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "ping" });
    const started = Date.now();
    const responses = await Promise.all(
        Array.from({ length: n }, () =>
            fetch(MCP_URL, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body,
            }).then(
                async (res) => {
                    await res.text();
                    return { status: res.status, retryAfter: res.headers.get("retry-after") };
                },
                (err) => ({ status: `network error: ${err?.message ?? err}`, retryAfter: null }),
            ),
        ),
    );
    return { responses, elapsed: Date.now() - started };
}

function distributionOf(responses) {
    const distribution = new Map();
    for (const r of responses) distribution.set(r.status, (distribution.get(r.status) ?? 0) + 1);
    return [...distribution.entries()]
        .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
        .map(([status, n]) => `${status}: ${n}`)
        .join(", ");
}

/**
 * NAT-safe rate-limit check — the one CI runs unattended.
 *
 * Asserts only that the limiter limits *something*. It deliberately does not
 * pin the 120/80 split, because a GitHub runner shares its egress IP and would
 * see fewer than 120 allowed through no fault of the deploy. The inequality it
 * does assert points the safe way: extra competing traffic can only produce
 * MORE 429s, never fewer, so this cannot flake toward a false pass.
 *
 * What it still catches, and what --skip-ratelimit did not: a limiter that is
 * not limiting at all.
 */
async function rateLimitProbe() {
    group("rate limit (probe)");
    console.log(
        `     firing ${PROBE_SIZE} requests (budget is ${EXPECTED_ALLOWED}/${WINDOW_MS / 1000}s); ` +
            `asserting only that the limiter engages`,
    );

    const { responses, elapsed } = await burst(PROBE_SIZE);
    const distText = distributionOf(responses);
    const denied = responses.filter((r) => r.status === 429).length;
    const allowed = responses.filter(
        (r) => typeof r.status === "number" && r.status >= 200 && r.status < 300,
    ).length;

    assert(
        "limiter engages under a burst above the budget",
        denied >= 1,
        `at least one 429 among ${PROBE_SIZE} requests`,
        `${denied} — distribution: ${distText}`,
        `${PROBE_SIZE} requests exceed the ${EXPECTED_ALLOWED}-request budget, so zero 429s means the ` +
            `limiter is not limiting: check the RATE_LIMITER Durable Object binding and the worker ` +
            `logs for {"event":"ratelimit_bypass"}.`,
    );

    assert(
        "at least one 429 carries Retry-After",
        responses.some((r) => r.status === 429 && r.retryAfter !== null),
        "a Retry-After header on some 429",
        denied === 0 ? "no 429 responses at all" : "429s present but none carried Retry-After",
        "without Retry-After a client cannot back off correctly",
    );

    console.log(
        `     distribution: ${distText} (burst took ${elapsed} ms; ${allowed} allowed / ${denied} denied)`,
    );
}

async function rateLimit() {
    group("rate limit");

    if (opts.fresh) {
        console.log(
            `     WARNING: --fresh skips the window wait, but the protocol checks above already spent\n` +
                `     part of this window. Expect the allowed count to come in under ${EXPECTED_ALLOWED} and fail.`,
        );
    } else {
        console.log(
            `     waiting ${Math.round(WINDOW_WAIT_MS / 1000)}s for the ${WINDOW_MS / 1000}s window to clear ` +
                `(the checks above consumed budget from this IP; pass --fresh to skip)`,
        );
        await sleep(WINDOW_WAIT_MS);
    }

    const { responses, elapsed } = await burst(BURST_SIZE);
    const distText = distributionOf(responses);

    const allowed = responses.filter((r) => typeof r.status === "number" && r.status >= 200 && r.status < 300).length;
    const denied = responses.filter((r) => r.status === 429).length;

    // The burst must complete inside one window or the fixed window rolls over
    // mid-test and the counts are meaningless.
    assert(
        "burst completed within one window",
        elapsed < WINDOW_MS,
        `< ${WINDOW_MS} ms`,
        `${elapsed} ms`,
        "the window rolled over mid-burst; counts below are not trustworthy",
    );

    assert(
        `exactly ${EXPECTED_ALLOWED} of ${BURST_SIZE} allowed`,
        allowed === EXPECTED_ALLOWED,
        EXPECTED_ALLOWED,
        allowed,
        `distribution: ${distText}. More than ${EXPECTED_ALLOWED} allowed means the limiter was bypassed ` +
            `(check worker logs for {"event":"ratelimit_bypass"}); fewer means the window was already ` +
            `partly consumed — re-run without --fresh, or wait 60s.`,
    );

    assert(
        `exactly ${EXPECTED_DENIED} of ${BURST_SIZE} rejected with 429`,
        denied === EXPECTED_DENIED,
        EXPECTED_DENIED,
        denied,
        `distribution: ${distText}`,
    );

    assert(
        "at least one 429 carries Retry-After",
        responses.some((r) => r.status === 429 && r.retryAfter !== null),
        "a Retry-After header on some 429",
        denied === 0 ? "no 429 responses at all" : "429s present but none carried Retry-After",
        "without Retry-After a client cannot back off correctly",
    );

    console.log(`     distribution: ${distText} (burst took ${elapsed} ms)`);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

async function main() {
    console.log(`QAIL MCP conformance suite`);
    console.log(`  endpoint: ${MCP_URL}`);
    console.log(`  health:   ${HEALTH_URL}`);
    console.log(`  origin under test: ${ALLOWED_ORIGIN}`);

    await awaitCleanWindow();

    await transportSemantics();
    await jsonRpcValidation();
    await initializeLifecycle();
    await originAndCors();
    await protocolVersionNegotiation();
    await toolInputSchemaEnforcement();
    await contentCorrectness();
    await surfaceCounts();

    if (opts.skipRateLimit) {
        group("rate limit");
        console.log("     skipped (--skip-ratelimit) — the limiter is NOT covered by this run");
    } else if (opts.rateLimitProbe) {
        await rateLimitProbe();
    } else {
        await rateLimit();
    }

    const total = passed + failures.length;
    console.log(`\n${"=".repeat(58)}`);
    if (failures.length === 0) {
        console.log(`PASS — ${passed}/${total} checks passed`);
        process.exit(0);
    }
    console.log(`FAIL — ${failures.length} of ${total} checks failed:`);
    for (const f of failures) {
        console.log(`  [${f.group}] ${f.name}`);
        console.log(`      expected ${render(f.expected)}`);
        console.log(`      got      ${render(f.actual)}`);
        if (f.hint) console.log(`      ${f.hint}`);
    }
    process.exit(1);
}

main().catch((err) => {
    console.error(`\nconformance: aborted — ${err?.stack ?? err}`);
    process.exit(2);
});
