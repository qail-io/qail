import { describe, it, expect, vi, beforeEach } from 'vitest';
import { QailClient, QailError, SelectBuilder } from './index';

// ─── Mock fetch ─────────────────────────────────────────────────────

function mockFetch(response: unknown, status = 200) {
    return vi.fn().mockResolvedValue({
        ok: status >= 200 && status < 300,
        status,
        json: async () => response,
        text: async () => JSON.stringify(response),
    });
}

function createClient(fetchFn: ReturnType<typeof vi.fn>) {
    return new QailClient({
        url: 'http://localhost:8080',
        token: 'test-jwt-token',
        fetch: fetchFn as unknown as typeof fetch,
    });
}

class MockWebSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    static readonly CLOSING = 2;
    static readonly CLOSED = 3;

    url: string;
    readyState = MockWebSocket.CONNECTING;
    sent: string[] = [];
    closeCalled = false;
    onopen: ((event: Event) => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onclose: ((event: CloseEvent) => void) | null = null;
    onerror: ((event: Event) => void) | null = null;

    constructor(url: string) {
        this.url = url;
    }

    send(data: string) {
        this.sent.push(data);
    }

    close() {
        this.closeCalled = true;
        this.readyState = MockWebSocket.CLOSED;
        this.onclose?.({} as CloseEvent);
    }

    open() {
        this.readyState = MockWebSocket.OPEN;
        this.onopen?.({} as Event);
    }

    emitMessage(data: string) {
        this.onmessage?.({ data } as MessageEvent);
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

describe('QailClient', () => {
    describe('health', () => {
        it('returns health response', async () => {
            const fetch = mockFetch({ status: 'ok', version: '0.20.1' });
            const client = createClient(fetch);
            const res = await client.health();
            expect(res.status).toBe('ok');
            expect(fetch).toHaveBeenCalledWith(
                'http://localhost:8080/health',
                expect.objectContaining({ method: 'GET' }),
            );
        });
    });

    describe('raw query', () => {
        it('sends DSL to /qail', async () => {
            const fetch = mockFetch({ data: [{ id: 1, name: 'Alice' }], rows_affected: 1, columns: ['id', 'name'] });
            const client = createClient(fetch);
            const res = await client.query('get users fields id, name limit 10');
            expect(res.data).toHaveLength(1);
            expect(fetch).toHaveBeenCalledWith(
                'http://localhost:8080/qail',
                expect.objectContaining({
                    method: 'POST',
                    body: 'get users fields id, name limit 10',
                    headers: expect.objectContaining({
                        'Authorization': 'Bearer test-jwt-token',
                        'Content-Type': 'text/plain',
                    }),
                }),
            );
        });
    });

    describe('SelectBuilder', () => {
        it('builds GET /api/users with filters', async () => {
            const fetch = mockFetch({ data: [], count: 0, limit: 10, offset: 0 });
            const client = createClient(fetch);

            await client.from('users')
                .select(['id', 'name', 'email'])
                .where('active', 'eq', true)
                .where('age', 'gte', 18)
                .desc('created_at')
                .limit(10)
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('/api/users?');
            expect(url).toContain('select=id%2Cname%2Cemail');
            expect(url).toContain('limit=10');
            expect(url).toContain('sort=created_at%3Adesc');
            expect(url).toContain('active.eq=true');
            expect(url).toContain('age.gte=18');
        });

        it('.all() returns just data', async () => {
            const fetch = mockFetch({ data: [{ id: 1 }, { id: 2 }], count: 2, limit: 50, offset: 0 });
            const client = createClient(fetch);

            const users = await client.from('users').all();
            expect(users).toEqual([{ id: 1 }, { id: 2 }]);
        });

        it('.get(id) fetches single row', async () => {
            const fetch = mockFetch({ data: { id: 42, name: 'Bob' } });
            const client = createClient(fetch);

            const user = await client.from('users').get(42);
            expect(user).toEqual({ id: 42, name: 'Bob' });
            expect(fetch.mock.calls[0][0]).toBe('http://localhost:8080/api/users/42');
        });

        it('supports expand (FK join)', async () => {
            const fetch = mockFetch({ data: [], count: 0, limit: 50, offset: 0 });
            const client = createClient(fetch);

            await client.from('orders')
                .expand('users')
                .expand('products')
                .limit(5)
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('expand=users%2Cproducts');
        });

        it('supports nested expansion', async () => {
            const fetch = mockFetch({ data: [], count: 0, limit: 50, offset: 0 });
            const client = createClient(fetch);

            await client.from('users')
                .nested('orders')
                .limit(5)
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('expand=nested%3Aorders');
        });

        it('encodes reserved characters in filter values', async () => {
            const fetch = mockFetch({ data: [], count: 0, limit: 50, offset: 0 });
            const client = createClient(fetch);

            await client.from('users')
                .where('name', 'eq', 'A&B=1 C')
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('name.eq=A%26B%3D1%20C');
            expect(url).not.toContain('name.eq=A&B=1 C');
        });
    });

    describe('InsertBuilder', () => {
        it('POST /api/users with body', async () => {
            const fetch = mockFetch({ data: { id: 1, name: 'New' }, rows_affected: 1 });
            const client = createClient(fetch);

            await client.into('users')
                .values({ name: 'New', email: 'new@test.com' })
                .returning('*')
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('/api/users?returning=*');
            expect(fetch.mock.calls[0][1].method).toBe('POST');
            expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ name: 'New', email: 'new@test.com' });
        });

        it('supports upsert via onConflict', async () => {
            const fetch = mockFetch({ data: {}, rows_affected: 1 });
            const client = createClient(fetch);

            await client.into('users')
                .values({ id: 1, name: 'Updated' })
                .onConflict('id', 'update')
                .exec();

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toContain('on_conflict=id');
            expect(url).toContain('on_conflict_action=update');
        });
    });

    describe('UpdateBuilder', () => {
        it('PATCH /api/users/:id', async () => {
            const fetch = mockFetch({ data: { id: 1, name: 'Updated' }, rows_affected: 1 });
            const client = createClient(fetch);

            await client.update('users')
                .set({ name: 'Updated' })
                .returning('*')
                .exec(1);

            const url = fetch.mock.calls[0][0] as string;
            expect(url).toBe('http://localhost:8080/api/users/1?returning=*');
            expect(fetch.mock.calls[0][1].method).toBe('PATCH');
        });
    });

    describe('DeleteBuilder', () => {
        it('DELETE /api/users/:id', async () => {
            const fetch = mockFetch({ deleted: true });
            const client = createClient(fetch);

            const res = await client.delete('users').exec(42);
            expect(res.deleted).toBe(true);
            expect(fetch.mock.calls[0][0]).toBe('http://localhost:8080/api/users/42');
            expect(fetch.mock.calls[0][1].method).toBe('DELETE');
        });
    });

    describe('error handling', () => {
        it('throws QailError on 4xx', async () => {
            const fetch = mockFetch({ error: 'Not found', code: 'NOT_FOUND' }, 404);
            const client = createClient(fetch);

            await expect(client.from('nonexistent').all()).rejects.toThrow(QailError);
            await expect(client.from('nonexistent').all()).rejects.toMatchObject({
                status: 404,
                code: 'NOT_FOUND',
            });
        });

        it('parses enriched error fields (hint, table, column)', async () => {
            const errorBody = {
                code: 'VALIDATION_ERROR',
                message: 'Unique constraint violated',
                details: 'duplicate key value violates unique constraint "users_email_key"',
                hint: 'A row with this email already exists',
                table: 'users',
                column: 'email',
            };
            const fetch = mockFetch(errorBody, 409);
            const client = createClient(fetch);

            try {
                await client.into('users').values({ email: 'dup@test.com' }).exec();
                expect.unreachable('Should have thrown');
            } catch (e) {
                const err = e as QailError;
                expect(err.code).toBe('VALIDATION_ERROR');
                expect(err.message).toBe('Unique constraint violated');
                expect(err.hint).toBe('A row with this email already exists');
                expect(err.table).toBe('users');
                expect(err.column).toBe('email');
            }
        });
    });

    describe('generateTypes', () => {
        it('fetches TypeScript interfaces as text', async () => {
            const tsSource = `// Auto-generated\nexport interface Users {\n  id: string;\n}\n`;
            const fetch = vi.fn().mockResolvedValue({
                ok: true,
                status: 200,
                text: async () => tsSource,
            });
            const client = createClient(fetch);

            const result = await client.generateTypes();
            expect(result).toBe(tsSource);
            expect(result).toContain('export interface Users');
            const url = fetch.mock.calls[0][0] as string;
            expect(url).toBe('http://localhost:8080/api/_schema/typescript');
        });
    });

    describe('auth', () => {
        it('includes Bearer token', async () => {
            const fetch = mockFetch({ status: 'ok', version: '0.20.1' });
            const client = createClient(fetch);
            await client.health();

            const headers = fetch.mock.calls[0][1].headers;
            expect(headers['Authorization']).toBe('Bearer test-jwt-token');
        });

        it('supports token function (refresh)', async () => {
            const fetch = mockFetch({ status: 'ok', version: '0.20.1' });
            const client = new QailClient({
                url: 'http://localhost:8080',
                token: () => 'dynamic-token',
                fetch: fetch as unknown as typeof fetch,
            });

            await client.health();
            const headers = fetch.mock.calls[0][1].headers;
            expect(headers['Authorization']).toBe('Bearer dynamic-token');
        });
    });

    describe('realtime websocket', () => {
        it('passes token via query param in subscribe()', async () => {
            const fetch = mockFetch({ status: 'ok', version: '0.20.1' });
            let created: MockWebSocket | undefined;

            const client = new QailClient({
                url: 'http://localhost:8080',
                token: 'ws-jwt',
                fetch: fetch as unknown as typeof fetch,
                wsAuthMode: 'query',
                webSocketFactory: (url) => {
                    created = new MockWebSocket(url);
                    return created as unknown as WebSocket;
                },
            });

            const sub = client.subscribe('orders', () => { });
            await vi.waitFor(() => expect(created).toBeDefined());
            expect(created!.url).toContain('/ws?access_token=ws-jwt');

            created!.open();
            expect(created!.sent[0]).toBe(JSON.stringify({ action: 'listen', channel: 'orders' }));
            sub.unsubscribe();
        });

        it('unsubscribe() before open prevents listen and callbacks', async () => {
            const fetch = mockFetch({ status: 'ok', version: '0.20.1' });
            let created: MockWebSocket | undefined;
            const onMessage = vi.fn();

            const client = new QailClient({
                url: 'http://localhost:8080',
                token: 'ws-jwt',
                fetch: fetch as unknown as typeof fetch,
                webSocketFactory: (url) => {
                    created = new MockWebSocket(url);
                    return created as unknown as WebSocket;
                },
            });

            const sub = client.subscribe('orders', onMessage);
            await vi.waitFor(() => expect(created).toBeDefined());

            sub.unsubscribe();
            expect(created!.closeCalled).toBe(true);

            created!.open();
            created!.emitMessage(JSON.stringify({ channel: 'orders', payload: '{"ok":true}' }));

            expect(created!.sent).toEqual([]);
            expect(onMessage).not.toHaveBeenCalled();
            expect(sub.active).toBe(false);
        });
    });
});
