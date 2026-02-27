# QAIL Gateway — SLO Definitions & Failure Runbooks

## Service Level Objectives

| SLO | Target | Metric | Window |
|---|---|---|---|
| **Availability** | 99.9% | `1 - (5xx / total requests)` | Rolling 30 days |
| **Latency (p95)** | < 200ms | `histogram_quantile(0.95, qail_http_request_duration_seconds)` | Rolling 7 days |
| **Latency (p99)** | < 500ms | `histogram_quantile(0.99, qail_http_request_duration_seconds)` | Rolling 7 days |
| **Query p95** | < 100ms | `histogram_quantile(0.95, qail_query_duration_ms)` | Rolling 7 days |
| **Error Budget** | < 0.1% 5xx | `rate(qail_http_requests_total{status=~"5.."}[30d]) / rate(qail_http_requests_total[30d])` | Rolling 30 days |
| **Pool Saturation** | < 80% | `qail_pool_active_connections / qail_pool_max_connections` | Instant |

### Alert Thresholds

```yaml
# Prometheus alerting rules
groups:
  - name: qail_gateway
    rules:
      - alert: HighErrorRate
        expr: sum(rate(qail_http_requests_total{status=~"5.."}[5m])) / sum(rate(qail_http_requests_total[5m])) > 0.01
        for: 5m
        labels: { severity: critical }
        annotations:
          summary: "Gateway 5xx error rate > 1%"

      - alert: HighLatency
        expr: histogram_quantile(0.95, sum(rate(qail_http_request_duration_seconds_bucket[5m])) by (le)) > 0.5
        for: 10m
        labels: { severity: warning }
        annotations:
          summary: "Gateway p95 latency > 500ms"

      - alert: PoolExhaustion
        expr: qail_pool_active_connections / qail_pool_max_connections > 0.9
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "Connection pool > 90% utilized"

      - alert: RateLimitSpike
        expr: rate(qail_rate_limited_total[5m]) > 10
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Excessive rate limiting (> 10/s)"

      - alert: CacheHitRateDrop
        expr: qail_cache_hits_total / (qail_cache_hits_total + qail_cache_misses_total) < 0.5
        for: 15m
        labels: { severity: warning }
        annotations:
          summary: "Cache hit rate dropped below 50%"
```

---

## Failure Runbooks

### 1. High 5xx Error Rate

**Symptoms**: `HighErrorRate` alert fires, users see 500 errors.

**Diagnosis**:
1. Check `qail_db_errors_total` by `sqlstate` — is it a DB issue?
2. Check pod logs: `journalctl -u qail-gateway --since '10 minutes ago'`
3. Check connection pool: is `qail_pool_active_connections` at max?
4. Check if a deploy just happened

**Common SQLSTATEs**:
| SQLSTATE | Meaning | Action |
|---|---|---|
| `40001` | Serialization failure | Automatic retry (handled by retry policy) |
| `57014` | Statement timeout | Increase `statement_timeout_ms` or optimize query |
| `53300` | Too many connections | Scale pool or reduce concurrency |
| `08006` | Connection failure | Check Postgres health, network |

**Mitigation**:
- If pool exhaustion: increase `max_connections` in config
- If statement timeout: check slow queries in `pg_stat_activity`
- If cascading: restart gateway pods one at a time

---

### 2. High Latency

**Symptoms**: `HighLatency` alert fires, p95 > 500ms.

**Diagnosis**:
1. Check `qail_query_duration_ms` by `action` — is it reads or writes?
2. Check `qail_cache_hits_total` — did cache effectiveness drop?
3. Check Postgres: `SELECT * FROM pg_stat_activity WHERE state = 'active' ORDER BY query_start;`
4. Check for lock contention: `SELECT * FROM pg_locks WHERE NOT granted;`

**Mitigation**:
- If cache miss spike: warm cache, check for schema changes that invalidated it
- If slow queries: add indexes, check `EXPLAIN ANALYZE` output
- If lock contention: check for long-running transactions, increase `lock_timeout_ms`

---

### 3. Connection Pool Exhaustion

**Symptoms**: `PoolExhaustion` alert, new requests fail with 503.

**Diagnosis**:
1. `qail_pool_active_connections` vs `qail_pool_max_connections`
2. Check for leaked connections: long-running queries in `pg_stat_activity`
3. Check for connection storms from new deployments

**Mitigation**:
- Kill idle-in-transaction connections: `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND query_start < now() - interval '5 minutes';`
- Increase pool size (requires restart)
- Enable connection queuing with tenant semaphore

---

### 4. Rate Limiting Spike

**Symptoms**: `RateLimitSpike` alert, clients getting 429s.

**Diagnosis**:
1. Is it a single tenant or global? Check `x-operator-id` in access logs
2. Is it legitimate traffic or an attack?
3. Check `qail_idempotency_hits_total` — are clients retrying without keys?

**Mitigation**:
- If legitimate: increase rate limit in config
- If attack: add IP-level blocking at Caddy/Cloudflare
- If retry storm: ensure clients send `Idempotency-Key` headers

---

### 5. Complexity/Explain Rejections

**Symptoms**: `qail_complexity_rejections_total` or `qail_explain_rejections_total` rising.

**Diagnosis**:
1. Check rejected queries in logs (search for `QUERY_TOO_COMPLEX` or `QUERY_TOO_EXPENSIVE`)
2. Compare `qail_last_rejected_cost` against `qail_explain_cost_limit`

**Mitigation**:
- If legitimate queries: relax `max_depth`, `max_filters`, `max_joins` in complexity guard
- If cost too high: add database indexes, simplify query patterns
- If malicious: keep limits, add to query allow-list if needed
