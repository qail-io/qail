// ─── Types ──────────────────────────────────────────────────────────

/** Filter operators matching PostgREST-style query params */
export type FilterOp =
    | 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte'
    | 'like' | 'ilike' | 'in' | 'not_in'
    | 'is_null' | 'is_not_null' | 'contains';

export type SortDirection = 'asc' | 'desc';

export interface QailConfig {
    /** Gateway URL, e.g. "http://localhost:8080" */
    url: string;
    /** JWT token or function that returns one (for refresh) */
    token?: string | (() => string | Promise<string>);
    /** Default headers to include on every request */
    headers?: Record<string, string>;
    /** Request timeout in ms (default: 30000) */
    timeout?: number;
    /** Custom fetch implementation (default: globalThis.fetch) */
    fetch?: typeof fetch;
}

export interface ListResponse<T = Record<string, unknown>> {
    data: T[];
    count: number;
    total?: number;
    limit: number;
    offset: number;
}

export interface SingleResponse<T = Record<string, unknown>> {
    data: T;
}

export interface MutationResponse<T = Record<string, unknown>> {
    data: T;
    rows_affected: number;
}

export interface BatchResult<T = Record<string, unknown>> {
    index: number;
    success: boolean;
    rows?: T[];
    count?: number;
    error?: string;
}

export interface QueryResponse<T = Record<string, unknown>> {
    data: T[];
    rows_affected: number;
    columns: string[];
}

export interface HealthResponse {
    status: string;
    version: string;
}

export interface AggregateResponse {
    data: Record<string, unknown>[];
    count: number;
}

// ─── Errors ─────────────────────────────────────────────────────────

export class QailError extends Error {
    constructor(
        message: string,
        public status: number,
        public code: string,
        public detail?: string,
        public hint?: string,
        public table?: string,
        public column?: string,
    ) {
        super(message);
        this.name = 'QailError';
    }
}

// ─── Client ─────────────────────────────────────────────────────────

export class QailClient {
    private baseUrl: string;
    private defaultHeaders: Record<string, string>;
    private timeout: number;
    private tokenSource?: string | (() => string | Promise<string>);
    private _fetch: typeof fetch;

    constructor(config: QailConfig) {
        this.baseUrl = config.url.replace(/\/+$/, '');
        this.defaultHeaders = config.headers ?? {};
        this.timeout = config.timeout ?? 30_000;
        this.tokenSource = config.token;
        this._fetch = config.fetch ?? globalThis.fetch;
    }

    // ── Query builder entry points ──────────────────────────────────

    /** Start a SELECT query on a table */
    from<T = Record<string, unknown>>(table: string): SelectBuilder<T> {
        return new SelectBuilder<T>(this, table);
    }

    /** Start an INSERT on a table */
    into<T = Record<string, unknown>>(table: string): InsertBuilder<T> {
        return new InsertBuilder<T>(this, table);
    }

    /** Start an UPDATE on a table */
    update<T = Record<string, unknown>>(table: string): UpdateBuilder<T> {
        return new UpdateBuilder<T>(this, table);
    }

    /** Start a DELETE on a table */
    delete(table: string): DeleteBuilder {
        return new DeleteBuilder(this, table);
    }

    // ── Raw QAIL text protocol ──────────────────────────────────────

    /** Execute raw Qail DSL text (e.g. "get users fields id, name limit 10") */
    async query<T = Record<string, unknown>>(dsl: string): Promise<QueryResponse<T>> {
        return this.request('POST', '/qail', dsl, 'text/plain');
    }

    /** Execute a batch of Qail DSL queries */
    async batch(queries: string[]): Promise<BatchResult[]> {
        return this.request('POST', '/qail/batch', JSON.stringify(queries));
    }

    // ── Utilities ───────────────────────────────────────────────────

    /** Health check */
    async health(): Promise<HealthResponse> {
        return this.request('GET', '/health');
    }

    /** Get OpenAPI spec */
    async openapi(): Promise<unknown> {
        return this.request('GET', '/api/_openapi');
    }

    /** Get schema introspection */
    async schema(): Promise<unknown> {
        return this.request('GET', '/api/_schema');
    }

    /**
     * Generate TypeScript interfaces from the gateway schema.
     *
     * Returns a string containing valid TypeScript interface declarations
     * that can be written to a `.d.ts` file for type-safe queries.
     *
     * @example
     * ```ts
     * const types = await qail.generateTypes();
     * // Write to file: fs.writeFileSync('db.d.ts', types);
     * ```
     */
    async generateTypes(): Promise<string> {
        return this.requestText('GET', '/api/_schema/typescript');
    }

    // ── Realtime (WebSocket) ────────────────────────────────────────

    /** Subscribe to a Postgres LISTEN channel via WebSocket */
    subscribe(channel: string, onMessage: (payload: string) => void): QailSubscription {
        const wsUrl = this.baseUrl.replace(/^http/, 'ws') + '/ws';
        const ws = new WebSocket(wsUrl);
        let alive = true;

        ws.onopen = () => {
            ws.send(JSON.stringify({ action: 'listen', channel }));
        };

        ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data as string);
                if (msg.channel === channel && msg.payload) {
                    onMessage(msg.payload);
                }
            } catch {
                // Non-JSON message, ignore
            }
        };

        return {
            unsubscribe: () => {
                alive = false;
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify({ action: 'unlisten', channel }));
                    ws.close();
                }
            },
            get active() { return alive && ws.readyState === WebSocket.OPEN; },
        };
    }

    // ── Internal HTTP ───────────────────────────────────────────────

    /** @internal — JSON response */
    async request<R>(
        method: string,
        path: string,
        body?: string,
        contentType?: string,
    ): Promise<R> {
        const headers: Record<string, string> = {
            ...this.defaultHeaders,
            'Content-Type': contentType ?? 'application/json',
        };

        // Resolve token
        if (this.tokenSource) {
            const token = typeof this.tokenSource === 'function'
                ? await this.tokenSource()
                : this.tokenSource;
            headers['Authorization'] = `Bearer ${token}`;
        }

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);

        try {
            const res = await this._fetch(`${this.baseUrl}${path}`, {
                method,
                headers,
                body: method === 'GET' ? undefined : body,
                signal: controller.signal,
            });

            if (!res.ok) {
                const text = await res.text();
                let parsed: Record<string, string> = {};
                try { parsed = JSON.parse(text); } catch { /* raw text */ }
                throw new QailError(
                    parsed.message ?? parsed.error ?? text,
                    res.status,
                    parsed.code ?? `HTTP_${res.status}`,
                    parsed.details ?? parsed.detail,
                    parsed.hint,
                    parsed.table,
                    parsed.column,
                );
            }

            return await res.json() as R;
        } finally {
            clearTimeout(timer);
        }
    }

    /** @internal — text/plain response */
    async requestText(
        method: string,
        path: string,
    ): Promise<string> {
        const headers: Record<string, string> = { ...this.defaultHeaders };

        if (this.tokenSource) {
            const token = typeof this.tokenSource === 'function'
                ? await this.tokenSource()
                : this.tokenSource;
            headers['Authorization'] = `Bearer ${token}`;
        }

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeout);

        try {
            const res = await this._fetch(`${this.baseUrl}${path}`, {
                method,
                headers,
                signal: controller.signal,
            });

            if (!res.ok) {
                const text = await res.text();
                let parsed: Record<string, string> = {};
                try { parsed = JSON.parse(text); } catch { /* raw text */ }
                throw new QailError(
                    parsed.message ?? parsed.error ?? text,
                    res.status,
                    parsed.code ?? `HTTP_${res.status}`,
                );
            }

            return await res.text();
        } finally {
            clearTimeout(timer);
        }
    }
}

// ─── Subscription ───────────────────────────────────────────────────

export interface QailSubscription {
    unsubscribe(): void;
    readonly active: boolean;
}

// ─── Select Builder ─────────────────────────────────────────────────

export class SelectBuilder<T = Record<string, unknown>> {
    private _columns?: string;
    private _filters: string[] = [];
    private _sort: string[] = [];
    private _limit?: number;
    private _offset?: number;
    private _expand: string[] = [];
    private _distinct?: string;
    private _search?: string;
    private _searchColumns?: string;
    private _stream = false;

    constructor(
        private client: QailClient,
        private table: string,
    ) { }

    /** Select specific columns */
    select(columns: (keyof T & string)[] | string[]): this {
        this._columns = columns.join(',');
        return this;
    }

    /** Add a filter condition */
    where(column: keyof T & string, op: FilterOp, value: unknown): this {
        const encoded = Array.isArray(value) ? value.join(',') : String(value);
        this._filters.push(`${column}.${op}=${encodeURIComponent(encoded)}`);
        return this;
    }

    /** Shorthand: where(column, 'eq', value) */
    eq(column: keyof T & string, value: unknown): this {
        return this.where(column, 'eq', value);
    }

    /** Sort ascending */
    asc(column: keyof T & string): this {
        this._sort.push(`${column}:asc`);
        return this;
    }

    /** Sort descending */
    desc(column: keyof T & string): this {
        this._sort.push(`${column}:desc`);
        return this;
    }

    /** Limit results */
    limit(n: number): this {
        this._limit = n;
        return this;
    }

    /** Offset results */
    offset(n: number): this {
        this._offset = n;
        return this;
    }

    /** Expand a FK relation via LEFT JOIN */
    expand(relation: string): this {
        this._expand.push(relation);
        return this;
    }

    /** Expand as nested JSON objects */
    nested(relation: string): this {
        this._expand.push(`nested:${relation}`);
        return this;
    }

    /** Distinct on columns */
    distinct(columns: string[]): this {
        this._distinct = columns.join(',');
        return this;
    }

    /** Full-text search */
    search(term: string, columns?: string[]): this {
        this._search = term;
        if (columns) this._searchColumns = columns.join(',');
        return this;
    }

    /** Enable NDJSON streaming */
    stream(): this {
        this._stream = true;
        return this;
    }

    /** Execute and return the list response */
    async exec(): Promise<ListResponse<T>> {
        const params = new URLSearchParams();
        if (this._columns) params.set('select', this._columns);
        if (this._sort.length) params.set('sort', this._sort.join(','));
        if (this._limit != null) params.set('limit', String(this._limit));
        if (this._offset != null) params.set('offset', String(this._offset));
        if (this._expand.length) params.set('expand', this._expand.join(','));
        if (this._distinct) params.set('distinct', this._distinct);
        if (this._search) params.set('search', this._search);
        if (this._searchColumns) params.set('search_columns', this._searchColumns);
        if (this._stream) params.set('stream', 'true');

        // Append raw filters
        const qs = params.toString();
        const filterQs = this._filters.join('&');
        const fullQs = [qs, filterQs].filter(Boolean).join('&');
        const path = `/api/${this.table}${fullQs ? '?' + fullQs : ''}`;

        return this.client.request('GET', path);
    }

    /** Execute and return just the data array */
    async all(): Promise<T[]> {
        const res = await this.exec();
        return res.data;
    }

    /** Get a single row by primary key */
    async get(id: string | number): Promise<T> {
        const res = await this.client.request<SingleResponse<T>>(
            'GET', `/api/${this.table}/${id}`,
        );
        return res.data;
    }

    /** Aggregate query */
    async aggregate(
        func: 'count' | 'sum' | 'avg' | 'min' | 'max',
        column?: string,
        groupBy?: string[],
    ): Promise<AggregateResponse> {
        const params = new URLSearchParams();
        params.set('func', func);
        if (column) params.set('column', column);
        if (groupBy) params.set('group_by', groupBy.join(','));

        // Append raw filters
        const qs = params.toString();
        const filterQs = this._filters.join('&');
        const fullQs = [qs, filterQs].filter(Boolean).join('&');

        return this.client.request('GET', `/api/${this.table}/aggregate?${fullQs}`);
    }
}

// ─── Insert Builder ─────────────────────────────────────────────────

export class InsertBuilder<T = Record<string, unknown>> {
    private _data: Record<string, unknown> | Record<string, unknown>[] = {};
    private _returning?: string;
    private _onConflict?: string;
    private _onConflictAction?: string;

    constructor(
        private client: QailClient,
        private table: string,
    ) { }

    /** Set the data to insert (single row or batch) */
    values(data: Partial<T> | Partial<T>[]): this {
        this._data = data as Record<string, unknown> | Record<string, unknown>[];
        return this;
    }

    /** Return specific columns after insert */
    returning(columns: '*' | (keyof T & string)[]): this {
        this._returning = columns === '*' ? '*' : columns.join(',');
        return this;
    }

    /** Upsert: on conflict column */
    onConflict(column: string, action: 'update' | 'nothing' = 'update'): this {
        this._onConflict = column;
        this._onConflictAction = action;
        return this;
    }

    /** Execute the insert */
    async exec(): Promise<MutationResponse<T>> {
        const params = new URLSearchParams();
        if (this._returning) params.set('returning', this._returning);
        if (this._onConflict) params.set('on_conflict', this._onConflict);
        if (this._onConflictAction) params.set('on_conflict_action', this._onConflictAction);

        const qs = params.toString();
        const path = `/api/${this.table}${qs ? '?' + qs : ''}`;

        return this.client.request('POST', path, JSON.stringify(this._data));
    }
}

// ─── Update Builder ─────────────────────────────────────────────────

export class UpdateBuilder<T = Record<string, unknown>> {
    private _data: Record<string, unknown> = {};
    private _returning?: string;

    constructor(
        private client: QailClient,
        private table: string,
    ) { }

    /** Set the fields to update */
    set(data: Partial<T>): this {
        this._data = data as Record<string, unknown>;
        return this;
    }

    /** Return columns after update */
    returning(columns: '*' | (keyof T & string)[]): this {
        this._returning = columns === '*' ? '*' : columns.join(',');
        return this;
    }

    /** Execute the update on a specific row */
    async exec(id: string | number): Promise<MutationResponse<T>> {
        const params = new URLSearchParams();
        if (this._returning) params.set('returning', this._returning);

        const qs = params.toString();
        const path = `/api/${this.table}/${id}${qs ? '?' + qs : ''}`;

        return this.client.request('PATCH', path, JSON.stringify(this._data));
    }
}

// ─── Delete Builder ─────────────────────────────────────────────────

export class DeleteBuilder {
    constructor(
        private client: QailClient,
        private table: string,
    ) { }

    /** Delete a row by primary key */
    async exec(id: string | number): Promise<{ deleted: boolean }> {
        return this.client.request('DELETE', `/api/${this.table}/${id}`);
    }
}
