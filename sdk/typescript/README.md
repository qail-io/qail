# @qail/client

TypeScript SDK for the [Qail Gateway](https://github.com/qail-io/qail) — a zero-overhead database gateway with compile-time safety.

## Install

```bash
npm install @qail/client
```

## Quick Start

```typescript
import { QailClient } from '@qail/client';

const qail = new QailClient({
  url: 'http://localhost:8080',
  token: 'your-jwt-token',
});

// List users (GET /api/users)
const users = await qail.from('users')
  .select(['id', 'name', 'email'])
  .where('active', 'eq', true)
  .desc('created_at')
  .limit(10)
  .all();

// Get by ID
const user = await qail.from('users').get('uuid-123');

// Insert
await qail.into('users')
  .values({ name: 'Alice', email: 'alice@example.com' })
  .returning('*')
  .exec();

// Update
await qail.update('users')
  .set({ name: 'Alice Updated' })
  .returning('*')
  .exec('uuid-123');

// Delete
await qail.delete('users').exec('uuid-123');
```

## String vs Structured Modes

- Default builder calls (`from`, `where`, `insert`, `update`) send structured REST parameters.
- `query()` and `batch()` are explicit text-DSL modes.
- For strict AST-only execution, pair this SDK with gateway binary/allow-list policies.

## Query Builder API

### Select (from)

```typescript
const orders = await qail.from<Order>('orders')
  .select(['id', 'total', 'status'])
  .where('status', 'in', ['pending', 'confirmed'])
  .where('total', 'gte', 100)
  .asc('created_at')
  .limit(20)
  .offset(40)
  .exec();

// → GET /api/orders?select=id,total,status&status.in=pending,confirmed&total.gte=100&sort=created_at:asc&limit=20&offset=40
```

### FK Expansion (joins)

```typescript
// Flat join (LEFT JOIN)
const ordersWithUsers = await qail.from('orders')
  .expand('users')
  .all();

// Nested JSON objects
const usersWithOrders = await qail.from('users')
  .nested('orders')
  .all();
```

### Aggregation

```typescript
const stats = await qail.from('orders')
  .where('status', 'eq', 'completed')
  .aggregate('sum', 'total', ['operator_id']);

// → GET /api/orders/aggregate?func=sum&column=total&group_by=operator_id&status.eq=completed
```

### Upsert

```typescript
await qail.into('settings')
  .values({ key: 'theme', value: 'dark' })
  .onConflict('key', 'update')
  .returning('*')
  .exec();
```

### Raw DSL

```typescript
// Direct QAIL text protocol
const result = await qail.query('get users fields id, name where active = true limit 10');

// Batch
const results = await qail.batch([
  'get users fields id, name limit 5',
  'get orders fields id, total limit 5',
]);
```

## Realtime (WebSocket)

```typescript
const sub = qail.subscribe('order_updates', (payload) => {
  console.log('New order:', JSON.parse(payload));
});

// Later...
sub.unsubscribe();
```

## Configuration

```typescript
const qail = new QailClient({
  url: 'http://localhost:8080',

  // Static token
  token: 'eyJhbG...',

  // Or: dynamic token (refresh support)
  token: async () => {
    const res = await fetch('/auth/refresh');
    return (await res.json()).token;
  },

  // Custom headers
  headers: { 'X-Tenant-ID': 'operator-123' },

  // Timeout (default: 30s)
  timeout: 10_000,
});
```

## Filter Operators

| Operator | SQL | Example |
|----------|-----|---------|
| `eq` | `=` | `.where('status', 'eq', 'active')` |
| `ne` | `!=` | `.where('role', 'ne', 'admin')` |
| `gt` / `gte` | `>` / `>=` | `.where('price', 'gte', 100)` |
| `lt` / `lte` | `<` / `<=` | `.where('age', 'lt', 18)` |
| `like` | `LIKE` | `.where('name', 'like', '%alice%')` |
| `ilike` | `ILIKE` | `.where('email', 'ilike', '%@gmail%')` |
| `in` | `IN` | `.where('id', 'in', [1, 2, 3])` |
| `not_in` | `NOT IN` | `.where('status', 'not_in', ['deleted'])` |
| `is_null` | `IS NULL` | `.where('deleted_at', 'is_null', true)` |
| `contains` | `@>` | `.where('tags', 'contains', 'urgent')` |

## Error Handling

```typescript
import { QailError } from '@qail/client';

try {
  await qail.from('nonexistent').all();
} catch (e) {
  if (e instanceof QailError) {
    console.error(e.status);  // 404
    console.error(e.code);    // "NOT_FOUND"
    console.error(e.message); // "Table not found"
  }
}
```

## License

Apache-2.0
