import XCTest
@testable import Qail

// MARK: - Mock URLProtocol

/// A custom URLProtocol that intercepts all requests for testing.
final class MockURLProtocol: URLProtocol {
    /// Handler to provide mock responses. Set before each test.
    nonisolated(unsafe) static var handler: ((URLRequest) throws -> (Data, HTTPURLResponse))?

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let handler = Self.handler else {
            XCTFail("MockURLProtocol.handler not set")
            return
        }
        do {
            let (data, response) = try handler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

// MARK: - Helpers

private func mockSession() -> URLSession {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [MockURLProtocol.self]
    return URLSession(configuration: config)
}

private func jsonResponse(_ json: [String: Any], status: Int = 200) -> (Data, HTTPURLResponse) {
    let data = try! JSONSerialization.data(withJSONObject: json)
    let response = HTTPURLResponse(
        url: URL(string: "http://test")!,
        statusCode: status,
        httpVersion: nil,
        headerFields: nil
    )!
    return (data, response)
}

private func createClient(session: URLSession = mockSession()) -> QailClient {
    QailClient(config: .init(
        url: "http://localhost:8080",
        token: .string("test-jwt"),
        session: session
    ))
}

// MARK: - Test Models (Decodable)

private struct User: Decodable, Equatable {
    let id: Int
    let name: String
}

// MARK: - Tests

final class QailTests: XCTestCase {

    override func setUp() {
        MockURLProtocol.handler = nil
    }

    /// Helper to read request body from either httpBody or httpBodyStream.
    static func readBody(from request: URLRequest) -> Data? {
        if let httpBody = request.httpBody { return httpBody }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        var data = Data()
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: 1024)
        defer { buffer.deallocate() }
        while stream.hasBytesAvailable {
            let read = stream.read(buffer, maxLength: 1024)
            if read > 0 { data.append(buffer, count: read) }
            else { break }
        }
        stream.close()
        return data
    }

    // MARK: Health

    func testHealth() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.url?.path, "/health")
            XCTAssertEqual(request.httpMethod, "GET")
            return jsonResponse(["status": "ok", "version": "0.20.1"])
        }

        let client = createClient()
        let res = try await client.health()
        XCTAssertEqual(res.status, "ok")
        XCTAssertEqual(res.version, "0.20.1")
    }

    // MARK: Auth

    func testBearerToken() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer test-jwt")
            return jsonResponse(["status": "ok", "version": "0.20.1"])
        }

        let client = createClient()
        _ = try await client.health()
    }

    func testTokenProvider() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer dynamic-token")
            return jsonResponse(["status": "ok", "version": "0.20.1"])
        }

        let client = QailClient(config: .init(
            url: "http://localhost:8080",
            token: .provider { "dynamic-token" },
            session: mockSession()
        ))
        _ = try await client.health()
    }

    // MARK: Raw DSL

    func testRawQuery() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.url?.path, "/qail")
            XCTAssertEqual(request.httpMethod, "POST")

            return jsonResponse([
                "rows": [["id": 1, "name": "Alice"]],
                "count": 1,
                "metadata": ["request_id": "test-123"]
            ])
        }

        let client = createClient()
        let res: QueryResponse<User> = try await client.query("get users")
        XCTAssertEqual(res.rows.count, 1)
        XCTAssertEqual(res.rows[0].name, "Alice")
        XCTAssertEqual(res.metadata?.requestId, "test-123")
    }

    // MARK: Select Builder

    func testSelectBuilder() async throws {
        MockURLProtocol.handler = { request in
            let url = request.url!.absoluteString
            XCTAssert(url.contains("/api/users?"))
            XCTAssert(url.contains("select=id,name,email"))
            XCTAssert(url.contains("limit=10"))
            XCTAssert(url.contains("sort=created_at:desc"))
            XCTAssert(url.contains("active.eq=true"))
            XCTAssertEqual(request.httpMethod, "GET")
            return jsonResponse([
                "data": [["id": 1, "name": "Alice"]],
                "count": 1,
                "limit": 10,
                "offset": 0,
            ])
        }

        let client = createClient()
        let users: [User] = try await client.from("users")
            .select(["id", "name", "email"])
            .where("active", .eq, "true")
            .desc("created_at")
            .limit(10)
            .all()

        XCTAssertEqual(users.count, 1)
        XCTAssertEqual(users[0].name, "Alice")
    }

    func testGetById() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.url?.path, "/api/users/42")
            return jsonResponse(["data": ["id": 42, "name": "Bob"]])
        }

        let client = createClient()
        let user: User = try await client.from("users").get(id: 42)
        XCTAssertEqual(user.id, 42)
        XCTAssertEqual(user.name, "Bob")
    }

    func testExpand() async throws {
        MockURLProtocol.handler = { request in
            let url = request.url!.absoluteString
            XCTAssert(url.contains("expand=users,products"))
            return jsonResponse(["data": [], "count": 0, "limit": 50, "offset": 0])
        }

        let client = createClient()
        let _: [User] = try await client.from("orders")
            .expand("users")
            .expand("products")
            .all()
    }

    // MARK: Insert Builder

    func testInsert() async throws {
        MockURLProtocol.handler = { request in
            return jsonResponse(["data": ["id": 1, "name": "New"], "count": 1])
        }

        let client = createClient()
        let res: MutationResponse<User> = try await client.into("users")
            .values(["name": "New"])
            .exec()

        XCTAssertEqual(res.data.name, "New")
        XCTAssertEqual(res.count, 1)
    }

    func testUpsert() async throws {
        MockURLProtocol.handler = { request in
            return jsonResponse(["data": ["id": 1, "name": "Updated"], "count": 1])
        }

        let client = createClient()
        let _: MutationResponse<User> = try await client.into("users")
            .values(["id": 1, "name": "Updated"])
            .onConflict("id", action: "update")
            .exec()
    }

    // MARK: Update Builder

    func testUpdate() async throws {
        MockURLProtocol.handler = { request in
            return jsonResponse(["data": ["id": 1, "name": "Updated"], "count": 1])
        }

        let client = createClient()
        let res: MutationResponse<User> = try await client.update("users")
            .set(["name": "Updated"])
            .exec(id: 1)

        XCTAssertEqual(res.data.name, "Updated")
    }

    // MARK: Delete Builder

    func testDelete() async throws {
        MockURLProtocol.handler = { request in
            XCTAssertEqual(request.url?.path, "/api/users/42")
            XCTAssertEqual(request.httpMethod, "DELETE")
            return jsonResponse(["deleted": true])
        }

        let client = createClient()
        let res = try await client.delete("users").exec(id: 42)
        XCTAssertTrue(res.deleted)
    }

    // MARK: Error Handling

    func testErrorParsing() async throws {
        MockURLProtocol.handler = { _ in
            return jsonResponse([
                "code": "NOT_FOUND",
                "message": "Resource not found",
                "hint": "Check the ID",
                "table": "users",
                "column": "id",
            ], status: 404)
        }

        let client = createClient()
        do {
            let _: [User] = try await client.from("nonexistent").all()
            XCTFail("Should have thrown")
        } catch let error as QailError {
            XCTAssertEqual(error.status, 404)
            XCTAssertEqual(error.code, "NOT_FOUND")
            XCTAssertEqual(error.message, "Resource not found")
            XCTAssertEqual(error.hint, "Check the ID")
            XCTAssertEqual(error.table, "users")
            XCTAssertEqual(error.column, "id")
        }
    }

    func testErrorFallback() async throws {
        MockURLProtocol.handler = { _ in
            let data = "Internal Server Error".data(using: .utf8)!
            let response = HTTPURLResponse(
                url: URL(string: "http://test")!,
                statusCode: 500,
                httpVersion: nil,
                headerFields: nil
            )!
            return (data, response)
        }

        let client = createClient()
        do {
            let _: HealthResponse = try await client.health()
            XCTFail("Should have thrown")
        } catch let error as QailError {
            XCTAssertEqual(error.status, 500)
            XCTAssertEqual(error.code, "HTTP_500")
            XCTAssert(error.message.contains("Internal Server Error"))
        }
    }

    func testSelectBuilderEncodesFilterValueSafely() async throws {
        MockURLProtocol.handler = { request in
            let url = request.url!.absoluteString
            XCTAssert(url.contains("name.eq=A%26B%3D1"))
            XCTAssertFalse(url.contains("name.eq=A&B=1"))
            return jsonResponse([
                "data": [["id": 1, "name": "Alice"]],
                "count": 1,
                "limit": 50,
                "offset": 0,
            ])
        }

        let client = createClient()
        let _: [User] = try await client.from("users")
            .where("name", .eq, "A&B=1")
            .all()
    }

    func testWebSocketRequestUsesAuthorizationHeader() throws {
        let client = QailClient(config: .init(
            url: "http://localhost:8080",
            token: .string("ws-token"),
            webSocketAuthMode: .header
        ))

        let request = try client.makeWebSocketRequest(token: "ws-token")
        XCTAssertEqual(request.url?.absoluteString, "ws://localhost:8080/ws")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), "Bearer ws-token")
    }

    func testWebSocketRequestSupportsQueryTokenMode() throws {
        let client = QailClient(config: .init(
            url: "https://localhost:8443",
            token: .string("ws-token"),
            webSocketAuthMode: .query(parameter: "access_token")
        ))

        let request = try client.makeWebSocketRequest(token: "ws-token")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Authorization"), nil)
        let url = request.url!.absoluteString
        XCTAssert(url.hasPrefix("wss://localhost:8443/ws?"))
        XCTAssert(url.contains("access_token=ws-token"))
    }
}

    // MARK: Batch & Fast

    func testBatchResponse() async throws {
        MockURLProtocol.handler = { _ in
            return jsonResponse([
                "results": [["index": 0, "success": true, "rows": [["id": 1]], "count": 1]],
                "total": 1,
                "success": 1
            ])
        }
        let client = createClient()
        let res: BatchResponse<User> = try await client.batch(["get users"])
        XCTAssertEqual(res.total, 1)
        XCTAssertEqual(res.results.count, 1)
    }

    func testFastQuery() async throws {
        MockURLProtocol.handler = { _ in
            return jsonResponse([
                "rows": [[1, "Alice"]],
                "count": 1
            ])
        }
        let client = createClient()
        let res = try await client.queryFast("get users")
        XCTAssertEqual(res.count, 1)
        XCTAssertEqual(res.rows[0][0].value as? Int, 1)
    }

    // MARK: Transactions

    func testTransactions() async throws {
        var step = 0
        MockURLProtocol.handler = { request in
            switch step {
            case 0:
                step += 1
                XCTAssertEqual(request.url?.path, "/txn/begin")
                return jsonResponse(["txn_id": "txn-123"])
            case 1:
                step += 1
                XCTAssertEqual(request.url?.path, "/txn/query")
                XCTAssertEqual(request.value(forHTTPHeaderField: "X-Transaction-Id"), "txn-123")
                return jsonResponse(["rows": [["id": 1, "name": "Alice"]], "count": 1])
            case 2:
                step += 1
                XCTAssertEqual(request.url?.path, "/txn/commit")
                return jsonResponse(["status": "committed"])
            default:
                XCTFail("Unexpected step")
                return jsonResponse([:])
            }
        }

        let client = createClient()
        let txn = try await client.beginTxn()
        XCTAssertEqual(txn.txnId, "txn-123")

        let res: QueryResponse<User> = try await txn.query("get users")
        XCTAssertEqual(res.rows.count, 1)

        let end = try await txn.commit()
        XCTAssertEqual(end.status, "committed")
    }
}
