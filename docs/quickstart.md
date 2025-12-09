# Quickstart — First Query in 5 Minutes

Get the Qail Gateway running locally with Docker and execute your first query.

---

## 1. Start the Gateway

```bash
git clone https://github.com/qail-io/qail && cd qail
docker compose up -d
```

This starts:
- **PostgreSQL 17** on port 5432
- **Qail Gateway** on port 8080

Wait for healthy:
```bash
curl http://localhost:8080/health
# → {"status":"ok","version":"0.20.1"}
```

## 2. Create a Table

```bash
curl -X POST http://localhost:8080/qail \
  -H "Content-Type: text/plain" \
  -d 'create table tasks (
    id uuid primary key default gen_random_uuid(),
    title text not null,
    done boolean default false,
    created_at timestamptz default now()
  )'
```

## 3. Insert Data

**Via DSL:**
```bash
curl -X POST http://localhost:8080/qail \
  -H "Content-Type: text/plain" \
  -d "add tasks set title = 'Ship the SDK', done = false"
```

**Via REST API:**
```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Write docs", "done": false}'
```

## 4. Query Data

**List all (REST):**
```bash
curl http://localhost:8080/api/tasks
```
```json
{
  "data": [
    {"id": "a1b2c3...", "title": "Ship the SDK", "done": false, "created_at": "2026-02-13T..."}
  ],
  "count": 1,
  "limit": 50,
  "offset": 0
}
```

**With filters:**
```bash
# GET /api/tasks?done.eq=false&sort=created_at:desc&limit=10
curl "http://localhost:8080/api/tasks?done.eq=false&sort=created_at:desc&limit=10"
```

**Via DSL:**
```bash
curl -X POST http://localhost:8080/qail \
  -H "Content-Type: text/plain" \
  -d 'get tasks fields id, title where done = false order created_at desc limit 10'
```

## 5. Update and Delete

```bash
# Update
curl -X PATCH http://localhost:8080/api/tasks/a1b2c3... \
  -H "Content-Type: application/json" \
  -d '{"done": true}'

# Delete
curl -X DELETE http://localhost:8080/api/tasks/a1b2c3...
```

## 6. Use the TypeScript SDK

```bash
npm install @qail/client
```

```typescript
import { QailClient } from '@qail/client';

const qail = new QailClient({ url: 'http://localhost:8080' });

// List incomplete tasks
const tasks = await qail.from('tasks')
  .where('done', 'eq', false)
  .desc('created_at')
  .limit(10)
  .all();

// Create a task
await qail.into('tasks')
  .values({ title: 'Try Qail', done: false })
  .exec();
```

## 7. Enable JWT Auth (Production)

```bash
# In docker-compose.yml, uncomment:
JWT_SECRET=your-256-bit-secret
```

```bash
curl -X POST http://localhost:8080/qail \
  -H "Authorization: Bearer eyJhbGciOi..." \
  -d 'get tasks fields id, title limit 10'
```

See [Auth Cookbook](auth-cookbook.md) for full JWT → RLS documentation.

---

## Next Steps

| What | Where |
|------|-------|
| Auth + RLS guide | [docs/auth-cookbook.md](auth-cookbook.md) |
| Filter operators | [SDK README](../sdk/typescript/README.md) |
| FK expansion | `?expand=orders` or `?expand=nested:orders` |
| Aggregation | `GET /api/tasks/aggregate?func=count` |
| OpenAPI spec | `GET /api/_openapi` |
| Health + metrics | `GET /health`, `GET /metrics` |
| Realtime events | [docs/realtime-patterns.md](realtime-patterns.md) |
