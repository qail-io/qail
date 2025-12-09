import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const p99Trend = new Trend('p99_latency', true);

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8082';

const TENANTS = [
    { operator_id: '11111111-1111-1111-1111-111111111101', role: 'operator' },
    { operator_id: '11111111-1111-1111-1111-111111111102', role: 'operator' },
];

// Spike test: baseline → spike → recovery
// Senior's test: push 1500 req/s for 5 min, drop back to 600, watch recovery curve
export const options = {
    scenarios: {
        spike: {
            executor: 'ramping-arrival-rate',
            startRate: 100,
            timeUnit: '1s',
            preAllocatedVUs: 200,
            maxVUs: 500,
            stages: [
                { duration: '30s', target: 600 },    // Ramp to baseline
                { duration: '1m', target: 600 },    // Hold baseline
                { duration: '15s', target: 1500 },   // Spike!
                { duration: '1m', target: 1500 },   // Sustained spike
                { duration: '10s', target: 600 },    // Drop back
                { duration: '1m', target: 600 },    // Recovery window
                { duration: '15s', target: 0 },      // Cool down
            ],
        },
    },
    thresholds: {
        http_req_duration: ['p(99)<2000'],
        http_req_failed: ['rate<0.10'],
    },
};

function pickTenant() {
    return TENANTS[Math.floor(Math.random() * TENANTS.length)];
}

export default function () {
    const tenant = pickTenant();
    const tables = ['orders', 'harbors', 'destinations', 'vessels', 'articles'];
    const table = tables[Math.floor(Math.random() * tables.length)];

    const res = http.get(`${BASE_URL}/api/${table}?limit=10`, {
        headers: {
            'Content-Type': 'application/json',
            'X-Operator-Id': tenant.operator_id,
            'X-User-Role': tenant.role,
        },
    });

    check(res, { 'OK': (r) => r.status === 200 });
    p99Trend.add(res.timings.duration);
}
