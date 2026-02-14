import Foundation

// MARK: - Configuration

/// Configuration for the Qail client.
public struct QailConfig: Sendable {
    /// Gateway base URL (e.g. `https://engine.example.com`).
    public let url: String
    /// JWT token or async token provider for refreshable auth.
    public let token: TokenSource?
    /// Additional default headers sent with every request.
    public let headers: [String: String]
    /// Request timeout in seconds (default: 30).
    public let timeout: TimeInterval
    /// Custom URLSession (defaults to `.shared`).
    public let session: URLSession
    /// Use snake_case ↔ camelCase key conversion (default: true).
    public let snakeCaseKeys: Bool

    public init(
        url: String,
        token: TokenSource? = nil,
        headers: [String: String] = [:],
        timeout: TimeInterval = 30,
        session: URLSession = .shared,
        snakeCaseKeys: Bool = true
    ) {
        self.url = url
        self.token = token
        self.headers = headers
        self.timeout = timeout
        self.session = session
        self.snakeCaseKeys = snakeCaseKeys
    }
}

/// Token source — either a static string or an async closure.
public enum TokenSource: Sendable {
    case string(String)
    case provider(@Sendable () async throws -> String)

    func resolve() async throws -> String {
        switch self {
        case .string(let t): return t
        case .provider(let fn): return try await fn()
        }
    }
}

// MARK: - Client

/// Qail Gateway client for iOS / macOS.
///
/// Provides fluent query builders, raw DSL execution, and utility
/// endpoints — mirroring the TypeScript SDK surface area.
///
/// ```swift
/// let qail = QailClient(config: .init(
///     url: "https://engine.example.com",
///     token: .string("my-jwt")
/// ))
///
/// // Fluent query
/// let users: [User] = try await qail.from("users")
///     .where("active", .eq, "true")
///     .limit(10)
///     .all()
///
/// // Raw DSL
/// let res = try await qail.query("get users fields id, name limit 10")
/// ```
public final class QailClient: Sendable {
    private let baseUrl: String
    private let defaultHeaders: [String: String]
    private let timeout: TimeInterval
    private let tokenSource: TokenSource?
    private let session: URLSession
    internal let decoder: JSONDecoder
    internal let encoder: JSONEncoder

    public init(config: QailConfig) {
        // Strip trailing slashes
        var url = config.url
        while url.hasSuffix("/") { url.removeLast() }
        self.baseUrl = url
        self.defaultHeaders = config.headers
        self.timeout = config.timeout
        self.tokenSource = config.token
        self.session = config.session

        let dec = JSONDecoder()
        let enc = JSONEncoder()
        if config.snakeCaseKeys {
            dec.keyDecodingStrategy = .convertFromSnakeCase
            enc.keyEncodingStrategy = .convertToSnakeCase
        }
        dec.dateDecodingStrategy = .iso8601
        enc.dateEncodingStrategy = .iso8601
        self.decoder = dec
        self.encoder = enc
    }

    // MARK: - Query builder entry points

    /// Start a SELECT query on a table.
    public func from<T: Decodable>(_ table: String) -> SelectBuilder<T> {
        SelectBuilder<T>(client: self, table: table)
    }

    /// Start an INSERT on a table.
    public func into<T: Decodable>(_ table: String) -> InsertBuilder<T> {
        InsertBuilder<T>(client: self, table: table)
    }

    /// Start an UPDATE on a table.
    public func update<T: Decodable>(_ table: String) -> UpdateBuilder<T> {
        UpdateBuilder<T>(client: self, table: table)
    }

    /// Start a DELETE on a table.
    public func delete(_ table: String) -> DeleteBuilder {
        DeleteBuilder(client: self, table: table)
    }

    // MARK: - Raw QAIL text protocol

    /// Execute raw Qail DSL text.
    ///
    /// ```swift
    /// let res = try await qail.query("get users fields id, name limit 10")
    /// ```
    public func query<T: Decodable>(_ dsl: String) async throws -> QueryResponse<T> {
        try await request(method: "POST", path: "/qail", body: dsl.data(using: .utf8), contentType: "text/plain")
    }

    /// Execute a batch of Qail DSL queries.
    public func batch<T: Decodable>(_ queries: [String]) async throws -> [BatchResult<T>] {
        let body = try JSONSerialization.data(withJSONObject: queries)
        return try await request(method: "POST", path: "/qail/batch", body: body)
    }

    // MARK: - Utilities

    /// Health check.
    public func health() async throws -> HealthResponse {
        try await request(method: "GET", path: "/health")
    }

    /// Get OpenAPI spec.
    public func openapi() async throws -> [String: Any] {
        try await requestRaw(method: "GET", path: "/api/_openapi")
    }

    /// Get schema introspection.
    public func schema() async throws -> [String: Any] {
        try await requestRaw(method: "GET", path: "/api/_schema")
    }

    /// Generate TypeScript interfaces from the gateway schema.
    ///
    /// Returns a string containing valid TypeScript interface declarations
    /// that can be written to a `.d.ts` file for type-safe queries.
    public func generateTypes() async throws -> String {
        try await requestText(method: "GET", path: "/api/_schema/typescript")
    }

    // MARK: - Realtime (WebSocket)

    /// Subscribe to a Postgres LISTEN channel via WebSocket.
    ///
    /// ```swift
    /// let sub = qail.subscribe("orders") { payload in
    ///     print("Got: \(payload)")
    /// }
    /// // Later...
    /// sub.unsubscribe()
    /// ```
    public func subscribe(channel: String, onMessage: @escaping @Sendable (String) -> Void) -> QailSubscription {
        let wsUrl = baseUrl.replacingOccurrences(of: "http", with: "ws") + "/ws"
        let url = URL(string: wsUrl)!
        let task = session.webSocketTask(with: url)
        let subscription = WebSocketSubscription(task: task, channel: channel, onMessage: onMessage)
        task.resume()

        // Send listen command once connected
        let listenMsg = #"{"action":"listen","channel":"\#(channel)"}"#
        task.send(.string(listenMsg)) { _ in }

        // Start receiving
        subscription.startReceiving()
        return subscription
    }

    // MARK: - Raw HTTP (for Workers endpoints)

    /// Make a typed request to any path (e.g. Workers endpoints).
    ///
    /// ```swift
    /// struct LoginResponse: Decodable { let token: String }
    /// let res: LoginResponse = try await qail.post("/public/auth/login", body: creds)
    /// ```
    public func get<T: Decodable>(_ path: String) async throws -> T {
        try await request(method: "GET", path: path)
    }

    public func post<T: Decodable, B: Encodable>(_ path: String, body: B) async throws -> T {
        let data = try encoder.encode(body)
        return try await request(method: "POST", path: path, body: data)
    }

    public func patch<T: Decodable, B: Encodable>(_ path: String, body: B) async throws -> T {
        let data = try encoder.encode(body)
        return try await request(method: "PATCH", path: path, body: data)
    }

    public func post<T: Decodable>(_ path: String) async throws -> T {
        try await request(method: "POST", path: path)
    }

    public func put<T: Decodable, B: Encodable>(_ path: String, body: B) async throws -> T {
        let data = try encoder.encode(body)
        return try await request(method: "PUT", path: path, body: data)
    }

    public func put<T: Decodable>(_ path: String) async throws -> T {
        try await request(method: "PUT", path: path)
    }

    public func delete<T: Decodable>(_ path: String) async throws -> T {
        try await request(method: "DELETE", path: path)
    }

    public func delete<T: Decodable, B: Encodable>(_ path: String, body: B) async throws -> T {
        let data = try encoder.encode(body)
        return try await request(method: "DELETE", path: path, body: data)
    }

    /// PUT with no response body expected.
    public func put(_ path: String) async throws {
        _ = try await performRequest(method: "PUT", path: path)
    }

    /// PUT with body, no response body expected.
    public func put<B: Encodable>(_ path: String, body: B) async throws {
        let data = try encoder.encode(body)
        _ = try await performRequest(method: "PUT", path: path, body: data)
    }

    /// DELETE with no response body expected.
    public func delete(_ path: String) async throws {
        _ = try await performRequest(method: "DELETE", path: path)
    }

    // MARK: - Internal HTTP

    /// Internal JSON request that returns a Decodable type.
    internal func request<T: Decodable>(
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String = "application/json"
    ) async throws -> T {
        let data = try await performRequest(method: method, path: path, body: body, contentType: contentType)
        return try decoder.decode(T.self, from: data)
    }

    /// Internal request that returns raw JSON dictionary.
    private func requestRaw(
        method: String,
        path: String
    ) async throws -> [String: Any] {
        let data = try await performRequest(method: method, path: path)
        guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw QailError(code: "PARSE_ERROR", message: "Response is not a JSON object", details: nil, requestId: nil, hint: nil, table: nil, column: nil)
        }
        return json
    }

    /// Internal request that returns plain text.
    internal func requestText(
        method: String,
        path: String
    ) async throws -> String {
        let data = try await performRequest(method: method, path: path)
        guard let text = String(data: data, encoding: .utf8) else {
            throw QailError(code: "PARSE_ERROR", message: "Response is not valid UTF-8", details: nil, requestId: nil, hint: nil, table: nil, column: nil)
        }
        return text
    }

    /// Core HTTP engine.
    private func performRequest(
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String = "application/json"
    ) async throws -> Data {
        guard let url = URL(string: "\(baseUrl)\(path)") else {
            throw QailError(code: "INVALID_URL", message: "Bad URL: \(baseUrl)\(path)", details: nil, requestId: nil, hint: nil, table: nil, column: nil)
        }

        var request = URLRequest(url: url, timeoutInterval: timeout)
        request.httpMethod = method
        request.setValue(contentType, forHTTPHeaderField: "Content-Type")

        // Default headers
        for (key, value) in defaultHeaders {
            request.setValue(value, forHTTPHeaderField: key)
        }

        // Auth token
        if let tokenSource {
            let token = try await tokenSource.resolve()
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        // Body
        if method != "GET", let body {
            request.httpBody = body
        }

        let (data, response) = try await session.data(for: request)

        guard let httpResponse = response as? HTTPURLResponse else {
            throw QailError(code: "NETWORK_ERROR", message: "Invalid response", details: nil, requestId: nil, hint: nil, table: nil, column: nil)
        }

        guard (200..<300).contains(httpResponse.statusCode) else {
            throw QailError.from(data: data, status: httpResponse.statusCode)
        }

        return data
    }
}
