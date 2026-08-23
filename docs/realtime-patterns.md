# Realtime Patterns — LISTEN/NOTIFY + WebSocket

Qail provides AST-native realtime via PostgreSQL LISTEN/NOTIFY, exposed over WebSocket.

---

## Architecture

```
Client (WebSocket)
    │  ws://gateway:8080/ws
    │  → { "type": "subscribe", "channel": "order_updates" }
    ▼
┌───────────────────────────────────────────┐
│  Gateway WebSocket Handler (ws.rs)        │
│  → derive t_... / u_... scoped channel   │
│  → authorize fragment, then LISTEN        │
│  → PG notifies → relay to WS client      │
└───────────────────────────────────────────┘
    ▲
    │  NOTIFY from PG trigger / app code
    │
┌───────────────────────────────────────────┐
│  PostgreSQL                               │
│  → Trigger: PERFORM pg_notify(...)        │
│  → App:     NOTIFY order_updates, payload │
└───────────────────────────────────────────┘
```

---

## 1. Produce On The Scoped Channel

The WebSocket client sends `order_updates`, but PostgreSQL listens on a derived
identity-scoped name. Application producers should use the shared core builder
instead of reconstructing that name:

```rust
use qail_core::{Qail, rls::RlsContext};

let ctx = RlsContext::tenant("acme");
let payload = r#"{"action":"created","id":"order-42"}"#;
let notify = Qail::notify_scoped(&ctx, "order_updates", payload)?;
driver.execute(&notify).await?;
```

Tenant contexts produce `t_...` channels. User-only contexts produce `u_...`
channels, which lets a non-multitenant consumer API isolate notifications per
recipient. If a database trigger must produce notifications, centralize the
same derivation in one database function and treat changes to the channel
format as a migration boundary.

## 2. TypeScript SDK (Recommended)

```typescript
import { QailClient } from '@qail/client';

const qail = new QailClient({
  url: 'http://localhost:8080',
  token: 'your-jwt-token',
});

// Subscribe to order events
const sub = qail.subscribe('order_updates', (payload) => {
  const order = JSON.parse(payload);
  console.log(`New order: ${order.id} — $${order.total}`);
  
  // Update your UI
  updateOrderList(order);
});

// Unsubscribe when done
sub.unsubscribe();
```

## 3. Raw WebSocket

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');

ws.onopen = () => {
  // Subscribe to a channel
  ws.send(JSON.stringify({
    type: 'subscribe',
    channel: 'order_updates'
  }));
};

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.channel === 'order_updates') {
    console.log('Order event:', msg.payload);
  }
};

// Unsubscribe
ws.send(JSON.stringify({
  type: 'unsubscribe',
  channel: 'order_updates'
}));
```

## 4. Channel Authorization

Identity scoping prevents one tenant or user from listening in another
identity's namespace. `channel_policies` controls which fragments the identity
may request:

```yaml
policies: []
channel_policies:
  - name: own_chat
    pattern: "chat_$user_id_*"
  - name: tenant_orders
    pattern: "orders_$tenant_id_*"
    role: operator
```

With no channel policies, existing valid fragments remain allowed. Once one
exists, unmatched fragments are denied before PostgreSQL receives `LISTEN`.
Placeholders include `$user_id`, `$tenant_id`, `$role`, and JWT claims. Claim
values are literal even when they contain `*`.

---

## Patterns

### Live Dashboard Feed

```typescript
// Subscribe to multiple channels for a monitoring dashboard
const orderSub = qail.subscribe('order_updates', handleOrder);
const paymentSub = qail.subscribe('payment_updates', handlePayment);
const inventorySub = qail.subscribe('inventory_changes', handleInventory);

// Cleanup on unmount
function cleanup() {
  orderSub.unsubscribe();
  paymentSub.unsubscribe();
  inventorySub.unsubscribe();
}
```

### Tenant-Scoped Events

Use the authenticated tenant context when producing. The gateway and producer
derive the same PostgreSQL channel, so another tenant cannot receive the event:

```rust
let ctx = RlsContext::tenant(tenant_id);
let notify = Qail::notify_scoped(&ctx, "tenant_events", payload)?;
driver.execute(&notify).await?;
```

```typescript
qail.subscribe('tenant_events', (raw) => {
  const event = JSON.parse(raw);
  handleTenantEvent(event);
});
```

### Collaborative Real-Time (Cursor/Presence)

```rust
let recipient = RlsContext::user("abc");
let notify = Qail::notify_scoped(
    &recipient,
    "presence",
    r#"{"user_id":"abc","cursor":{"x":100,"y":200}}"#,
)?;
driver.execute(&notify).await?;
```

```typescript
qail.subscribe('presence', (raw) => {
  const { user_id, cursor } = JSON.parse(raw);
  updateCursorPosition(user_id, cursor);
});
```

---

## Security Notes

- **Identity isolation is in the channel name** — tenant and user namespaces are distinct and derived by the gateway
- **Channel policies authorize fragments** — once configured, unmatched fragments fail closed before `LISTEN`
- **Manual payloads are forwarded only to manual subscribers** — live-query wake-up payloads never reach clients
- **Reserved namespaces are rejected** — clients cannot manually subscribe to `qail_table_` or `qail_lq_`
- **JWT required** — WebSocket connections use the same auth as HTTP (Bearer token in initial handshake)
- **Connection limits** — gateway tracks active WS connections via Prometheus metric `qail_ws_connections`
