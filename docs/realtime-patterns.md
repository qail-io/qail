# Realtime Patterns — LISTEN/NOTIFY + WebSocket

Qail provides AST-native realtime via PostgreSQL LISTEN/NOTIFY, exposed over WebSocket.

---

## Architecture

```
Client (WebSocket)
    │  ws://gateway:8080/ws
    │  → { "action": "listen", "channel": "order_updates" }
    ▼
┌───────────────────────────────────────────┐
│  Gateway WebSocket Handler (ws.rs)        │
│  → LISTEN order_updates (via Qail AST)    │
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

## 1. PostgreSQL Trigger Setup

```sql
-- Notify on new orders
CREATE OR REPLACE FUNCTION notify_order_created()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'order_updates',
    json_build_object(
      'action', 'created',
      'id', NEW.id,
      'operator_id', NEW.operator_id,
      'total', NEW.total
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER order_created_trigger
  AFTER INSERT ON orders
  FOR EACH ROW EXECUTE FUNCTION notify_order_created();
```

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
    action: 'listen',
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
  action: 'unlisten',
  channel: 'order_updates'
}));
```

## 4. Via Qail DSL

The gateway also exposes LISTEN/NOTIFY as first-class AST commands:

```bash
# Listen (typically via WebSocket, not HTTP)
curl -X POST http://localhost:8080/qail \
  -d 'listen order_updates'

# Notify (from another client or app)
curl -X POST http://localhost:8080/qail \
  -d "notify order_updates 'payload here'"

# Unlisten
curl -X POST http://localhost:8080/qail \
  -d 'unlisten order_updates'
```

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

Include `operator_id` in the notification payload so clients can filter:

```sql
CREATE OR REPLACE FUNCTION notify_tenant_event()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'tenant_events',
    json_build_object(
      'operator_id', NEW.operator_id,
      'table', TG_TABLE_NAME,
      'action', TG_OP,
      'id', NEW.id
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
```

```typescript
qail.subscribe('tenant_events', (raw) => {
  const event = JSON.parse(raw);
  // Only process events for this tenant
  if (event.operator_id === currentTenantId) {
    handleTenantEvent(event);
  }
});
```

### Collaborative Real-Time (Cursor/Presence)

```sql
-- Lightweight presence channel
NOTIFY presence, '{"user_id":"abc","cursor":{"x":100,"y":200}}';
```

```typescript
qail.subscribe('presence', (raw) => {
  const { user_id, cursor } = JSON.parse(raw);
  updateCursorPosition(user_id, cursor);
});
```

---

## Security Notes

- **Channel names are not RLS-filtered** — any authenticated WebSocket client can LISTEN on any channel
- **Filter in the payload** — include `operator_id` in NOTIFY payloads and filter client-side
- **JWT required** — WebSocket connections use the same auth as HTTP (Bearer token in initial handshake)
- **Connection limits** — gateway tracks active WS connections via Prometheus metric `qail_ws_connections`
