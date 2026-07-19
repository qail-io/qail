/**
 * Fixed-window rate limiter backed by a Durable Object.
 *
 * Cloudflare's native `ratelimits` binding was tried first and proved inert on
 * this account: `limit()` returned `{success: true}` indefinitely, past the
 * configured threshold, across two namespace ids. A limiter that never limits
 * is worse than none — it reports protection that does not exist — so this
 * replaces it with a mechanism whose behaviour can be demonstrated.
 *
 * One Durable Object per client IP. A DO is cheap while idle, and routing by IP
 * means each client's counter is strongly consistent rather than per-colo
 * approximate. Counters live in memory only: a DO eviction resets the window,
 * which is an acceptable trade for a public read-only endpoint and avoids a
 * storage write on every request.
 */

/** Requests permitted per window, per IP. */
const LIMIT = 120;

/** Window length in milliseconds. */
const WINDOW_MS = 60_000;

export class RateLimiter {
    private count = 0;
    private windowStart = 0;

    async fetch(request: Request): Promise<Response> {
        // Date.now() inside a DO is fine; the "no clock in global scope"
        // restriction applies to Workers' top-level, not to request handlers.
        const now = Date.now();

        if (now - this.windowStart >= WINDOW_MS) {
            this.windowStart = now;
            this.count = 0;
        }

        this.count += 1;
        const allowed = this.count <= LIMIT;
        const retryAfter = Math.max(
            1,
            Math.ceil((this.windowStart + WINDOW_MS - now) / 1000),
        );

        return Response.json({ allowed, count: this.count, limit: LIMIT, retryAfter });
    }
}

export interface RateLimitResult {
    allowed: boolean;
    retryAfter: number;
}

/**
 * Check one request against its IP's budget.
 *
 * Fails **open**: if the Durable Object namespace is missing or the call throws,
 * the request is allowed. A rate limiter that takes the service down when its
 * own backend is unavailable trades a bounded problem for an unbounded one.
 */
export async function checkRateLimit(
    namespace: DurableObjectNamespace | undefined,
    request: Request,
): Promise<RateLimitResult> {
    if (!namespace) return { allowed: true, retryAfter: 0 };

    const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
    try {
        const stub = namespace.get(namespace.idFromName(ip));
        const response = await stub.fetch("https://ratelimit.internal/check");
        const body = (await response.json()) as { allowed: boolean; retryAfter: number };
        return { allowed: body.allowed, retryAfter: body.retryAfter };
    } catch {
        return { allowed: true, retryAfter: 0 };
    }
}
