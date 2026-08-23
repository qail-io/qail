# Real-Time & Events

The gateway supports real-time data through WebSocket subscriptions, live queries, and event triggers with webhook delivery.

---

## WebSocket Subscriptions

Subscribe to table changes via WebSocket (backed by PostgreSQL `LISTEN/NOTIFY`):

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
ws.send(JSON.stringify({
  type: 'subscribe',
  channel: 'order_updates'
}));

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  // type: "subscribed" | "notification" | "error"
  console.log(msg);
};
```

The client sends a channel *fragment*. The gateway derives the PostgreSQL
channel from the authenticated tenant (`t_...`) or, for a tenant-less consumer
account, the authenticated user (`u_...`). Clients must not send the reserved
`qail_table_` or `qail_lq_` live-query namespaces.

Producers must use the same derivation:

```rust
let ctx = RlsContext::tenant("acme");
let notify = Qail::notify_scoped(&ctx, "order_updates", payload)?;
driver.execute(&notify).await?;
```

Manual subscriptions are identity-scoped, not row-filtered. Restrict which
fragments an identity may request with `channel_policies` in `policies.yaml`:

```yaml
policies: []
channel_policies:
  - name: own_chat
    pattern: "chat_$user_id_*"
  - name: seller_orders
    pattern: "orders_$tenant_id_*"
    role: seller
```

When `channel_policies` is absent or empty, current fragment behavior is
preserved. Once any channel policy exists, unmatched fragments are denied
before `LISTEN`. Claim substitutions are literal; a claim containing `*`
cannot widen a pattern.

---

## Live Queries

Auto-refresh query results when underlying data changes:

```javascript
ws.send(JSON.stringify({
  type: 'live_query',
  qail: "get orders where status = 'paid' order by created_at desc limit 10",
  table: 'orders',
  interval_ms: 2000
}));
```

The gateway re-executes the query at the specified interval and pushes updated results only when data has changed.

Live-query NOTIFY payloads are never forwarded to the client. They are only
wake-ups; the gateway re-fetches under the subscriber's authorization context.

---

## Event Triggers

Fire webhooks on database mutations. Define triggers in YAML:

```yaml
- name: order_created
  table: orders
  operations: [create]
  webhook_url: "https://api.example.com/hooks/order-created"
  headers:
    X-Secret: webhook-secret-key
  retry_count: 3
- name: order_updated
  table: orders
  operations: [update]
  webhook_url: "https://api.example.com/hooks/order-updated"
```

### Webhook Payload

```json
{
  "trigger": "order_created",
  "table": "orders",
  "operation": "INSERT",
  "data": {
    "new": { "id": "uuid-...", "total": 150.00, "status": "pending" },
    "old": null
  },
  "timestamp": "2025-01-15T10:30:00Z"
}
```

### Delivery Guarantees

- **Non-blocking:** Webhooks fire asynchronously via `tokio::spawn`. The REST response is never delayed.
- **Retry with backoff:** Failed deliveries retry with exponential backoff up to the configured count.
- **Custom headers:** Attach secret keys or auth tokens to webhook requests.

### Operations

Each trigger can fire on one or more operations:

| Operation | Fires on | Payload |
|-----------|----------|---------|
| `create` | `POST /api/{table}` | `new` data |
| `update` | `PATCH /api/{table}/:id` | `new` + `old` data |
| `delete` | `DELETE /api/{table}/:id` | `old` data |

---

## NDJSON Streaming

For large datasets, stream results as newline-delimited JSON:

```
GET /api/events?stream=true
```

Each row is sent as a separate JSON line with chunked transfer encoding — no buffering:

```
{"id":"uuid-1","type":"click","timestamp":"2025-01-01T00:00:00Z"}
{"id":"uuid-2","type":"purchase","timestamp":"2025-01-01T00:01:00Z"}
...
```

Ideal for data exports, ETL pipelines, and processing large tables without memory pressure.
