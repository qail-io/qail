/**
 * Fixed-window rate limiter backed by a Durable Object.
 *
 * Why not Cloudflare's native `ratelimits` binding: it is documented as
 * intentionally permissive and eventually consistent, which makes it a poor fit
 * for a hard per-IP cap on a CPU-bound public endpoint. Measured here, a
 * 200-request burst against a configured limit of 120 was not throttled at all.
 * That is consistent with its documented behaviour rather than proof of a
 * defect — it is the wrong tool for this job, not a broken one.
 *
 * One Durable Object per client IP. A DO is single-threaded, so the
 * read-modify-write below is atomic without a lock: there is no `await` between
 * reading `count` and writing it back, so a successful call can never race
 * another. Successful calls therefore stop at exactly LIMIT.
 *
 * Counters live in memory only. A DO eviction resets the window, which is an
 * acceptable trade for a public read-only endpoint and avoids a storage write
 * on every request.
 */

/** Requests permitted per window, per IP. */
const LIMIT = 120;

/** Window length in milliseconds. */
const WINDOW_MS = 60_000;

export class RateLimiter {
    private count = 0;
    private windowStart = 0;

    async fetch(_request: Request): Promise<Response> {
        const now = Date.now();

        if (now - this.windowStart >= WINDOW_MS) {
            this.windowStart = now;
            this.count = 0;
        }

        // Atomic by construction: no await between read and write.
        this.count += 1;
        const allowed = this.count <= LIMIT;
        const retryAfter = Math.max(
            1,
            Math.ceil((this.windowStart + WINDOW_MS - now) / 1000),
        );

        return Response.json({ allowed, count: this.count, limit: LIMIT, retryAfter });
    }
}

/**
 * What the limiter decided, and why.
 *
 * `bypass` is distinguished from `allow` deliberately: a bypass means the
 * limiter did not run, and must be counted rather than folded into the success
 * path. Conflating the two is what made an earlier version of this file report
 * 160 allowed against a limit of 120 and call it working.
 */
export type LimitDecision =
    | { outcome: "allow"; count: number }
    | { outcome: "deny"; retryAfter: number }
    | { outcome: "overloaded"; retryAfter: number }
    | { outcome: "bypass"; reason: string };

/** Cloudflare tags Durable Object errors with these hints. */
interface DurableObjectError extends Error {
    overloaded?: boolean;
    retryable?: boolean;
}

function classify(err: unknown): DurableObjectError {
    return (err ?? {}) as DurableObjectError;
}

/**
 * Check one request against its IP's budget.
 *
 * Failure handling, in order of how much we can infer:
 *
 * - **`.overloaded`** — too many concurrent requests to this object. Cloudflare
 *   documents these as not-to-be-retried. Semantically this *is* the condition
 *   the limiter exists to catch (one IP saturating one object), so it returns
 *   429 rather than failing open. Failing open here would let precisely the
 *   traffic pattern we are limiting slip through.
 * - **`.retryable`** — a transient hiccup. One bounded retry, then fail open.
 * - **anything else** — fail open, with the reason recorded.
 *
 * Every bypass is returned as such so the caller can log and count it. A
 * limiter that silently allows on error reports protection it is not providing.
 */
export async function checkRateLimit(
    namespace: DurableObjectNamespace | undefined,
    request: Request,
): Promise<LimitDecision> {
    if (!namespace) return { outcome: "bypass", reason: "no-binding" };

    const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
    const stub = namespace.get(namespace.idFromName(ip));

    for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
            const response = await stub.fetch("https://ratelimit.internal/check");
            const body = (await response.json()) as {
                allowed: boolean;
                count: number;
                retryAfter: number;
            };
            return body.allowed
                ? { outcome: "allow", count: body.count }
                : { outcome: "deny", retryAfter: body.retryAfter };
        } catch (err) {
            const e = classify(err);

            if (e.overloaded === true) {
                // Not retryable per Cloudflare, and not a reason to let the
                // request through: object overload from a single IP is the
                // abuse signal itself.
                return { outcome: "overloaded", retryAfter: 60 };
            }

            if (e.retryable === true && attempt === 0) {
                continue;
            }

            return {
                outcome: "bypass",
                reason: `${e.name ?? "Error"}: ${e.message ?? String(err)}`.slice(0, 120),
            };
        }
    }

    return { outcome: "bypass", reason: "retry-exhausted" };
}
