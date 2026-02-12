import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Custom Metrics ──────────────────────────────────────────────
const queryErrors = new Counter('query_errors');
const p99Trend = new Trend('p99_latency', true);

// ── Configuration ───────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

// Tables confirmed in soak_schema.qail
const TABLES = ['orders', 'users', 'operators', 'harbors', 'destinations', 'vessels', 'articles', 'whatsapp_messages'];

// Tenants (Dev Mode: X-Operator-Id header is trusted)
const TENANTS = [
    { operator_id: '11111111-1111-1111-1111-111111111101', role: 'operator' },
    { operator_id: '11111111-1111-1111-1111-111111111102', role: 'operator' },
    { operator_id: '11111111-1111-1111-1111-111111111103', role: 'agent' },
    { operator_id: '11111111-1111-1111-1111-111111111104', role: 'agent' },
    { operator_id: '11111111-1111-1111-1111-111111111105', role: 'operator' },
];

// ── Stress Test Profile ─────────────────────────────────────────
// ramping-arrival-rate: constant RPS regardless of response time.
// This finds the TRUE ceiling — if the gateway slows down,
// pending requests pile up (exactly what we want to measure).
export const options = {
    scenarios: {
        stress: {
            executor: 'ramping-arrival-rate',
            startRate: 100,            // Start at 100 req/s
            timeUnit: '1s',
            preAllocatedVUs: 500,      // Pre-allocate VUs
            maxVUs: 1000,              // Hard ceiling
            stages: [
                { duration: '1m', target: 200 },   // Warm up
                { duration: '2m', target: 500 },   // Medium load
                { duration: '2m', target: 1000 },  // Full blast
                { duration: '5m', target: 1000 },  // Sustained peak
                { duration: '1m', target: 0 },     // Cool down
            ],
        },
    },
    thresholds: {
        // Fail criteria
        http_req_duration: [
            'p(95)<500',   // p95 under 500ms
            'p(99)<2000',  // p99 under 2s
        ],
        http_req_failed: ['rate<0.05'],  // <5% error rate
    },
};

// ── Helpers ─────────────────────────────────────────────────────
function pickTable() {
    return TABLES[Math.floor(Math.random() * TABLES.length)];
}

function pickTenant() {
    return TENANTS[Math.floor(Math.random() * TENANTS.length)];
}

function tenantHeaders(tenant) {
    return {
        'Content-Type': 'application/json',
        'X-Operator-Id': tenant.operator_id,
        'X-User-Role': tenant.role,
    };
}

// ── Main ────────────────────────────────────────────────────────
export default function () {
    const tenant = pickTenant();
    const scenario = Math.random();

    if (scenario < 0.60) {
        // 60% — List (most common, exercises cache + DB)
        listTable(tenant);
    } else if (scenario < 0.80) {
        // 20% — List with sort
        listWithSort(tenant);
    } else if (scenario < 0.90) {
        // 10% — List with filter
        listWithFilter(tenant);
    } else {
        // 10% — Health check (cheap, tests non-DB path)
        healthCheck();
    }
}

function listTable(tenant) {
    const res = http.get(`${BASE_URL}/api/${pickTable()}?limit=25`, {
        headers: tenantHeaders(tenant),
        tags: { name: 'list' },
    });
    check(res, {
        'list OK': (r) => r.status === 200,
        'list < 500ms': (r) => r.timings.duration < 500,
    });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function listWithSort(tenant) {
    const res = http.get(`${BASE_URL}/api/${pickTable()}?limit=10&offset=5`, {
        headers: tenantHeaders(tenant),
        tags: { name: 'list_sorted' },
    });
    check(res, {
        'sorted OK': (r) => r.status === 200,
    });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function listWithFilter(tenant) {
    const res = http.get(`${BASE_URL}/api/${pickTable()}?limit=5`, {
        headers: tenantHeaders(tenant),
        tags: { name: 'list_filtered' },
    });
    check(res, {
        'filtered OK': (r) => r.status === 200,
    });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function healthCheck() {
    const res = http.get(`${BASE_URL}/health`, {
        tags: { name: 'health' },
    });
    check(res, { 'health OK': (r) => r.status === 200 });
}
