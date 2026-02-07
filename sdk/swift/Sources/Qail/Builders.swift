import Foundation

// MARK: - Select Builder

/// Fluent builder for `GET /api/{table}` queries.
///
/// ```swift
/// let users: [User] = try await qail.from("users")
///     .select(["id", "name", "email"])
///     .where("active", .eq, "true")
///     .desc("created_at")
///     .limit(10)
///     .all()
/// ```
public final class SelectBuilder<T: Decodable> {
    private let client: QailClient
    private let table: String
    private var columns: String?
    private var filters: [String] = []
    private var sorts: [String] = []
    private var _limit: Int?
    private var _offset: Int?
    private var expands: [String] = []
    private var _distinct: String?
    private var _search: String?
    private var _searchColumns: String?
    private var _stream: Bool = false

    internal init(client: QailClient, table: String) {
        self.client = client
        self.table = table
    }

    /// Select specific columns.
    @discardableResult
    public func select(_ columns: [String]) -> Self {
        self.columns = columns.joined(separator: ",")
        return self
    }

    /// Add a filter condition.
    @discardableResult
    public func `where`(_ column: String, _ op: FilterOp, _ value: String) -> Self {
        let encoded = value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? value
        filters.append("\(column).\(op.rawValue)=\(encoded)")
        return self
    }

    /// Shorthand: where(column, .eq, value).
    @discardableResult
    public func eq(_ column: String, _ value: String) -> Self {
        self.where(column, .eq, value)
    }

    /// Sort ascending.
    @discardableResult
    public func asc(_ column: String) -> Self {
        sorts.append("\(column):asc")
        return self
    }

    /// Sort descending.
    @discardableResult
    public func desc(_ column: String) -> Self {
        sorts.append("\(column):desc")
        return self
    }

    /// Limit results.
    @discardableResult
    public func limit(_ n: Int) -> Self {
        _limit = n
        return self
    }

    /// Offset results.
    @discardableResult
    public func offset(_ n: Int) -> Self {
        _offset = n
        return self
    }

    /// Expand a FK relation via LEFT JOIN.
    @discardableResult
    public func expand(_ relation: String) -> Self {
        expands.append(relation)
        return self
    }

    /// Expand as nested JSON objects.
    @discardableResult
    public func nested(_ relation: String) -> Self {
        expands.append("nested:\(relation)")
        return self
    }

    /// Distinct on columns.
    @discardableResult
    public func distinct(_ columns: [String]) -> Self {
        _distinct = columns.joined(separator: ",")
        return self
    }

    /// Full-text search.
    @discardableResult
    public func search(_ term: String, columns: [String]? = nil) -> Self {
        _search = term
        if let columns { _searchColumns = columns.joined(separator: ",") }
        return self
    }

    /// Execute and return the full paginated response.
    public func exec() async throws -> ListResponse<T> {
        let path = "/api/\(table)\(buildQueryString())"
        return try await client.request(method: "GET", path: path)
    }

    /// Execute and return just the data array.
    public func all() async throws -> [T] {
        let res = try await exec()
        return res.data
    }

    /// Get the first matching row, or nil.
    public func first() async throws -> T? {
        let saved = _limit
        _limit = 1
        let res = try await exec()
        _limit = saved
        return res.data.first
    }

    /// Get exactly one row (throws if none found).
    public func single() async throws -> T {
        guard let row = try await first() else {
            throw QailError(code: "NOT_FOUND", message: "No rows found in \(table)", details: nil, requestId: nil, hint: nil, table: table, column: nil)
        }
        return row
    }

    /// Get the total count of matching rows.
    public func count() async throws -> Int {
        let res = try await exec()
        return res.total ?? res.count
    }

    /// Get a single row by primary key.
    public func get(id: CustomStringConvertible) async throws -> T {
        let res: SingleResponse<T> = try await client.request(
            method: "GET",
            path: "/api/\(table)/\(id)"
        )
        return res.data
    }

    /// Enable NDJSON streaming.
    @discardableResult
    public func stream() -> Self {
        _stream = true
        return self
    }

    /// Aggregate query (count, sum, avg, min, max).
    ///
    /// ```swift
    /// let agg = try await qail.from<User>("orders")
    ///     .where("status", .eq, "completed")
    ///     .aggregate(.count, groupBy: ["region"])
    /// ```
    public func aggregate(
        _ func_: AggregateFunc,
        column: String? = nil,
        groupBy: [String]? = nil
    ) async throws -> AggregateResponse {
        var params: [String] = ["func=\(func_.rawValue)"]
        if let column { params.append("column=\(column)") }
        if let groupBy { params.append("group_by=\(groupBy.joined(separator: ","))") }

        let filterQs = filters.joined(separator: "&")
        let paramQs = params.joined(separator: "&")
        let fullQs = [paramQs, filterQs].filter { !$0.isEmpty }.joined(separator: "&")

        return try await client.request(
            method: "GET",
            path: "/api/\(table)/aggregate?\(fullQs)"
        )
    }

    // MARK: - Internal

    private func buildQueryString() -> String {
        var components = URLComponents()
        var items: [URLQueryItem] = []

        if let columns { items.append(URLQueryItem(name: "select", value: columns)) }
        if !sorts.isEmpty { items.append(URLQueryItem(name: "sort", value: sorts.joined(separator: ","))) }
        if let _limit { items.append(URLQueryItem(name: "limit", value: String(_limit))) }
        if let _offset { items.append(URLQueryItem(name: "offset", value: String(_offset))) }
        if !expands.isEmpty { items.append(URLQueryItem(name: "expand", value: expands.joined(separator: ","))) }
        if let _distinct { items.append(URLQueryItem(name: "distinct", value: _distinct)) }
        if let _search { items.append(URLQueryItem(name: "search", value: _search)) }
        if let _searchColumns { items.append(URLQueryItem(name: "search_columns", value: _searchColumns)) }
        if _stream { items.append(URLQueryItem(name: "stream", value: "true")) }

        components.queryItems = items.isEmpty ? nil : items

        // Build the standard query string
        var qs = components.query ?? ""

        // Append raw filters (they're already percent-encoded)
        if !filters.isEmpty {
            let filterQs = filters.joined(separator: "&")
            qs = qs.isEmpty ? filterQs : "\(qs)&\(filterQs)"
        }

        return qs.isEmpty ? "" : "?\(qs)"
    }
}

// MARK: - Insert Builder

/// Fluent builder for `POST /api/{table}`.
///
/// ```swift
/// let user: MutationResponse<User> = try await qail.into("users")
///     .values(["name": "Alice", "email": "alice@test.com"])
///     .returning("*")
///     .exec()
/// ```
public final class InsertBuilder<T: Decodable> {
    private let client: QailClient
    private let table: String
    private var data: Any = [String: Any]()
    private var _returning: String?
    private var _onConflict: String?
    private var _onConflictAction: String?

    internal init(client: QailClient, table: String) {
        self.client = client
        self.table = table
    }

    /// Set the data to insert (single row or batch).
    @discardableResult
    public func values(_ data: [String: Any]) -> Self {
        self.data = data
        return self
    }

    /// Set batch data to insert.
    @discardableResult
    public func values(_ data: [[String: Any]]) -> Self {
        self.data = data
        return self
    }

    /// Return specific columns after insert.
    @discardableResult
    public func returning(_ columns: String) -> Self {
        _returning = columns
        return self
    }

    /// Upsert: on conflict column.
    @discardableResult
    public func onConflict(_ column: String, action: String = "update") -> Self {
        _onConflict = column
        _onConflictAction = action
        return self
    }

    /// Execute the insert.
    public func exec() async throws -> MutationResponse<T> {
        var components = URLComponents()
        var items: [URLQueryItem] = []
        if let _returning { items.append(URLQueryItem(name: "returning", value: _returning)) }
        if let _onConflict { items.append(URLQueryItem(name: "on_conflict", value: _onConflict)) }
        if let _onConflictAction { items.append(URLQueryItem(name: "on_conflict_action", value: _onConflictAction)) }
        components.queryItems = items.isEmpty ? nil : items
        let qs = components.query.map { "?\($0)" } ?? ""

        let body = try JSONSerialization.data(withJSONObject: data)
        return try await client.request(method: "POST", path: "/api/\(table)\(qs)", body: body)
    }
}

// MARK: - Update Builder

/// Fluent builder for `PATCH /api/{table}/{id}`.
///
/// ```swift
/// let res: MutationResponse<User> = try await qail.update("users")
///     .set(["name": "Updated"])
///     .returning("*")
///     .exec(id: 1)
/// ```
public final class UpdateBuilder<T: Decodable> {
    private let client: QailClient
    private let table: String
    private var data: [String: Any] = [:]
    private var _returning: String?

    internal init(client: QailClient, table: String) {
        self.client = client
        self.table = table
    }

    /// Set the fields to update.
    @discardableResult
    public func set(_ data: [String: Any]) -> Self {
        self.data = data
        return self
    }

    /// Return columns after update.
    @discardableResult
    public func returning(_ columns: String) -> Self {
        _returning = columns
        return self
    }

    /// Execute the update on a specific row.
    public func exec(id: CustomStringConvertible) async throws -> MutationResponse<T> {
        var components = URLComponents()
        var items: [URLQueryItem] = []
        if let _returning { items.append(URLQueryItem(name: "returning", value: _returning)) }
        components.queryItems = items.isEmpty ? nil : items
        let qs = components.query.map { "?\($0)" } ?? ""

        let body = try JSONSerialization.data(withJSONObject: data)
        return try await client.request(method: "PATCH", path: "/api/\(table)/\(id)\(qs)", body: body)
    }
}

// MARK: - Delete Builder

/// Fluent builder for `DELETE /api/{table}/{id}`.
///
/// ```swift
/// let res = try await qail.delete("users").exec(id: 42)
/// ```
public final class DeleteBuilder {
    private let client: QailClient
    private let table: String

    internal init(client: QailClient, table: String) {
        self.client = client
        self.table = table
    }

    /// Delete a row by primary key.
    public func exec(id: CustomStringConvertible) async throws -> DeleteResponse {
        try await client.request(method: "DELETE", path: "/api/\(table)/\(id)")
    }
}
