import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// ── Custom Metrics ──────────────────────────────────────────────
const explainRejections = new Counter('explain_rejections');
const rateLimited = new Counter('rate_limited');
const tenantLatency = new Trend('tenant_latency', true);

// ── Configuration ───────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || 'https://gateway.qail.io';

// Simulated tenants with different roles
const TENANTS = [
  { operator_id: '11111111-1111-1111-1111-111111111101', role: 'operator' },
  { operator_id: '11111111-1111-1111-1111-111111111102', role: 'operator' },
  { operator_id: '11111111-1111-1111-1111-111111111103', role: 'agent' },
  { operator_id: '11111111-1111-1111-1111-111111111104', role: 'agent' },
  { operator_id: '11111111-1111-1111-1111-111111111105', role: 'operator' },
  { operator_id: '11111111-1111-1111-1111-111111111106', role: 'viewer' },
  { operator_id: '11111111-1111-1111-1111-111111111107', role: 'operator' },
  { operator_id: '11111111-1111-1111-1111-111111111108', role: 'agent' },
  { operator_id: '11111111-1111-1111-1111-111111111109', role: 'operator' },
  { operator_id: '11111111-1111-1111-1111-111111111110', role: 'viewer' },
];

// Actual tables from qail_battle DB
const TABLES = ['booking_orders', 'agents', 'articles', 'users', 'routes', 'schedules', 'analytics_events'];

// ── Soak Test Profile ───────────────────────────────────────────
export const options = {
  scenarios: {
    soak: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5m', target: 50 },   // Ramp up
        { duration: '71h', target: 50 },   // Steady state
        { duration: '5m', target: 0 },    // Ramp down
      ],
      gracefulRampDown: '30s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<2000'],
    http_req_failed: ['rate<0.05'],  // <5% error rate (some tables may 404)
  },
};

// ── Helpers ─────────────────────────────────────────────────────
function tenantHeaders(tenant) {
  return {
    'Content-Type': 'application/json',
    'X-Operator-Id': tenant.operator_id,
    'X-User-Role': tenant.role,
  };
}

function pickTenant() {
  return TENANTS[Math.floor(Math.random() * TENANTS.length)];
}

function pickTable() {
  return TABLES[Math.floor(Math.random() * TABLES.length)];
}

// ── Default Function ────────────────────────────────────────────
export default function () {
  const tenant = pickTenant();
  const scenario = SCENARIOS[Math.floor(Math.random() * SCENARIOS.length)];

  const start = Date.now();
  scenario(tenant);
  tenantLatency.add(Date.now() - start, { operator: tenant.operator_id.slice(-3) });

  sleep(0.1 + Math.random() * 0.9);
}

// ── Scenarios ───────────────────────────────────────────────────
const SCENARIOS = [
  listTable,
  listWithSort,
  listWithFilter,
  healthCheck,
  healthCheck,  // weighted: health is cheap, test it often
  schemaEndpoint,
];

function listTable(tenant) {
  const table = pickTable();
  const res = http.get(`${BASE_URL}/api/${table}?limit=25`, {
    headers: tenantHeaders(tenant),
  });
  check(res, {
    'list: status 2xx or auth error': (r) => r.status < 500,
  });
  trackErrors(res);
}

function listWithSort(tenant) {
  const table = pickTable();
  const res = http.get(`${BASE_URL}/api/${table}?sort=-created_at&limit=10`, {
    headers: tenantHeaders(tenant),
  });
  check(res, {
    'sorted: status < 500': (r) => r.status < 500,
  });
  trackErrors(res);
}

function listWithFilter(tenant) {
  const table = pickTable();
  const res = http.get(`${BASE_URL}/api/${table}?limit=10&select=id`, {
    headers: tenantHeaders(tenant),
  });
  check(res, {
    'filtered: status < 500': (r) => r.status < 500,
  });
  trackErrors(res);
}

function healthCheck() {
  const res = http.get(`${BASE_URL}/health`);
  check(res, { 'health: status 200': (r) => r.status === 200 });
}

function schemaEndpoint(tenant) {
  const res = http.get(`${BASE_URL}/api/_schema`, {
    headers: tenantHeaders(tenant),
  });
  check(res, { 'schema: status < 500': (r) => r.status < 500 });
}

// ── Error Tracking ──────────────────────────────────────────────
function trackErrors(res) {
  if (res.status === 429) rateLimited.add(1);
  if (res.status === 422) {
    try {
      const body = JSON.parse(res.body);
      if (body.code === 'QUERY_TOO_EXPENSIVE') explainRejections.add(1);
    } catch (e) { }
  }
}
