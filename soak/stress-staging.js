import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// ── Custom Metrics ──────────────────────────────────────────────
const queryErrors = new Counter('query_errors');
const p99Trend = new Trend('p99_latency', true);

// ── Configuration ───────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || 'https://gateway.example.com';
const WORKERS_URL = __ENV.WORKERS_URL || 'https://workers.example.com';

// ── JWT Auth: obtain tokens at setup ────────────────────────────
const ACCOUNTS = [
    { email: 'admin@example.com', password: 'changeme' },
    { email: 'operator@example.com', password: 'changeme' },
    { email: 'master@example.com', password: 'changeme' },
];

// Tables that exist in schema.qail
const TABLES = ['orders', 'users', 'operators', 'harbors', 'destinations', 'vessels'];

// ── Stress Test Profile ─────────────────────────────────────────
export const options = {
    scenarios: {
        stress: {
            executor: 'ramping-arrival-rate',
            startRate: 10,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 200,
            stages: [
                { duration: '30s', target: 50 },    // Warm up
                { duration: '1m', target: 100 },     // Medium load
                { duration: '2m', target: 200 },     // High load
                { duration: '2m', target: 200 },     // Sustained
                { duration: '30s', target: 0 },      // Cool down
            ],
        },
    },
    thresholds: {
        http_req_duration: [
            'p(95)<500',
            'p(99)<2000',
        ],
        http_req_failed: ['rate<0.05'],
    },
};

// ── Setup: login all accounts and collect tokens ────────────────
export function setup() {
    const tokens = [];
    for (const account of ACCOUNTS) {
        const res = http.post(`${WORKERS_URL}/auth/login`, JSON.stringify({
            email: account.email,
            password: account.password,
        }), { headers: { 'Content-Type': 'application/json' } });

        if (res.status === 200) {
            const body = JSON.parse(res.body);
            tokens.push({
                email: account.email,
                token: body.access_token,
                role: body.user.role,
            });
            console.log(`✓ Logged in: ${account.email} (${body.user.role})`);
        } else {
            console.warn(`✗ Login failed for ${account.email}: ${res.status} ${res.body}`);
        }
    }

    if (tokens.length === 0) {
        throw new Error('No accounts could log in — aborting stress test');
    }

    return { tokens };
}

// ── Helpers ─────────────────────────────────────────────────────
function pickToken(data) {
    return data.tokens[Math.floor(Math.random() * data.tokens.length)];
}

function pickTable() {
    return TABLES[Math.floor(Math.random() * TABLES.length)];
}

function authHeaders(token) {
    return {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
    };
}

// ── Main ────────────────────────────────────────────────────────
export default function (data) {
    const account = pickToken(data);
    const scenario = Math.random();

    if (scenario < 0.60) {
        listTable(account);
    } else if (scenario < 0.80) {
        listWithPagination(account);
    } else if (scenario < 0.90) {
        listSpecificTable(account);
    } else {
        healthCheck();
    }
}

function listTable(account) {
    const res = http.get(`${BASE_URL}/api/${pickTable()}?limit=25`, {
        headers: authHeaders(account.token),
        tags: { name: 'list' },
    });
    check(res, {
        'list OK': (r) => r.status === 200,
        'list < 500ms': (r) => r.timings.duration < 500,
    });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function listWithPagination(account) {
    const res = http.get(`${BASE_URL}/api/${pickTable()}?limit=10&offset=5`, {
        headers: authHeaders(account.token),
        tags: { name: 'list_paginated' },
    });
    check(res, { 'paginated OK': (r) => r.status === 200 });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function listSpecificTable(account) {
    const res = http.get(`${BASE_URL}/api/orders?limit=5`, {
        headers: authHeaders(account.token),
        tags: { name: 'orders' },
    });
    check(res, { 'orders OK': (r) => r.status === 200 });
    if (res.status >= 400) queryErrors.add(1);
    p99Trend.add(res.timings.duration);
}

function healthCheck() {
    const res = http.get(`${BASE_URL}/health`, {
        tags: { name: 'health' },
    });
    check(res, { 'health OK': (r) => r.status === 200 });
}
