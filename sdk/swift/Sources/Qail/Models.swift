import Foundation

// MARK: - Filter Operators

/// Filter operators matching PostgREST-style query params.
public enum FilterOp: String, Sendable {
    case eq, ne, gt, gte, lt, lte
    case like, ilike
    case `in`
    case notIn = "not_in"
    case isNull = "is_null"
    case isNotNull = "is_not_null"
    case contains
    @available(*, deprecated, message: "Use .ne instead.")
    public static let neq: FilterOp = .ne
}

// MARK: - Sort Direction

public enum SortDirection: String, Sendable {
    case asc, desc
}

// MARK: - Responses

/// Paginated list response from `GET /api/{table}`.
public struct ListResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let data: [T]
    public let count: Int
    public let total: Int?
    public let limit: Int
    public let offset: Int
}

/// Single-row response from `GET /api/{table}/{id}`.
public struct SingleResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let data: T
}

/// Metadata included in successful API responses.
public struct ResponseMetadata: Decodable, Sendable {
    public let requestId: String
    public let durationMs: Double?

    enum CodingKeys: String, CodingKey {
        case requestId
        case durationMs
    }
}

/// Mutation response from POST/PATCH operations.
public struct MutationResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let data: T
    public let count: Int?
    public let metadata: ResponseMetadata?
}

/// Raw DSL query response from `POST /qail`.
public struct QueryResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let rows: [T]
    public let count: Int
    public let metadata: ResponseMetadata?
}

/// Fast query response (array-of-arrays) from `POST /qail/fast`.
public struct FastQueryResponse: Decodable, @unchecked Sendable {
    public let rows: [[AnyCodable]]
    public let count: Int
    public let metadata: ResponseMetadata?
}

/// Health check response.
public struct HealthResponse: Decodable, Sendable {
    public let status: String
    public let version: String
    public let poolActive: Int?
    public let poolIdle: Int?

    enum CodingKeys: String, CodingKey {
        case status, version
        case poolActive
        case poolIdle
    }
}

/// Batch query response.
public struct BatchResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let results: [BatchResult<T>]
    public let total: Int
    public let success: Int
    public let metadata: ResponseMetadata?
}

/// Batch result for multi-query execution.
public struct BatchResult<T: Decodable>: Decodable, @unchecked Sendable {
    public let index: Int
    public let success: Bool
    public let rows: [T]?
    public let count: Int?
    public let error: String?
}

/// Delete confirmation.
public struct DeleteResponse: Decodable, Sendable {
    public let deleted: Bool
}

/// Aggregate query response.
public struct AggregateResponse: Decodable, Sendable {
    public let data: [[String: AnyCodable]]
    public let count: Int
    public let metadata: ResponseMetadata?
}

/// Type-erased Codable wrapper for aggregate values.
public struct AnyCodable: Decodable, @unchecked Sendable {
    public let value: Any

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let v = try? container.decode(Int.self) { value = v }
        else if let v = try? container.decode(Double.self) { value = v }
        else if let v = try? container.decode(String.self) { value = v }
        else if let v = try? container.decode(Bool.self) { value = v }
        else if container.decodeNil() { value = NSNull() }
        else { value = "<unknown>" }
    }
}

/// Aggregate function type.
public enum AggregateFunc: String, Sendable {
    case count, sum, avg, min, max
}

/// Subscription handle for WebSocket LISTEN/NOTIFY.
public protocol QailSubscription {
    func unsubscribe()
    var active: Bool { get }
}

/// Concrete WebSocket subscription using URLSessionWebSocketTask.
public final class WebSocketSubscription: QailSubscription, @unchecked Sendable {
    private let channel: String
    private let onMessage: @Sendable (String) -> Void
    private let lock = NSLock()
    private var task: URLSessionWebSocketTask?
    private var isSubscribed = true

    init(channel: String, onMessage: @escaping @Sendable (String) -> Void) {
        self.channel = channel
        self.onMessage = onMessage
    }

    public var active: Bool {
        lock.lock()
        defer { lock.unlock() }
        return isSubscribed && (task?.state == .running)
    }

    func attach(task: URLSessionWebSocketTask) {
        lock.lock()
        guard isSubscribed else {
            lock.unlock()
            task.cancel(with: .goingAway, reason: nil)
            return
        }
        self.task = task
        lock.unlock()

        task.resume()
        send(action: "listen", on: task)
        startReceiving(task: task)
    }

    func markFailed() {
        lock.lock()
        isSubscribed = false
        lock.unlock()
    }

    public func unsubscribe() {
        let currentTask: URLSessionWebSocketTask? = {
            lock.lock()
            defer { lock.unlock() }
            isSubscribed = false
            return task
        }()

        guard let currentTask else { return }
        if currentTask.state == .running {
            send(action: "unlisten", on: currentTask)
        }
        currentTask.cancel(with: .goingAway, reason: nil)
    }

    private func send(action: String, on task: URLSessionWebSocketTask) {
        let msgObj = ["action": action, "channel": channel]
        if let data = try? JSONSerialization.data(withJSONObject: msgObj),
           let msg = String(data: data, encoding: .utf8) {
            task.send(.string(msg)) { _ in }
        }
    }

    private func shouldContinue(with task: URLSessionWebSocketTask) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return isSubscribed && self.task === task
    }

    private func parseAndDispatch(_ data: Data) {
        if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
           let ch = json["channel"] as? String, ch == channel,
           let payload = json["payload"] as? String {
            onMessage(payload)
        }
    }

    func startReceiving(task: URLSessionWebSocketTask) {
        task.receive { [weak self] result in
            guard let self else { return }
            guard shouldContinue(with: task) else { return }
            switch result {
            case .success(.string(let text)):
                if let data = text.data(using: .utf8) {
                    parseAndDispatch(data)
                }
                startReceiving(task: task) // Continue listening
            case .success(.data(let data)):
                parseAndDispatch(data)
                startReceiving(task: task)
            case .failure:
                markFailed()
            @unknown default:
                markFailed()
            }
        }
    }
}

// MARK: - Error

/// Structured error from the Qail Gateway.
///
/// Matches the gateway `ApiError` JSON shape including enriched
/// `hint`, `table`, and `column` fields for developer-friendly diagnostics.
public struct QailError: Error, Decodable, Sendable, CustomStringConvertible, LocalizedError {
    public let code: String
    public let message: String
    public let details: String?
    public let requestId: String?
    public let hint: String?
    public let table: String?
    public let column: String?

    /// HTTP status code (populated client-side, not from JSON).
    public var status: Int = 0

    enum CodingKeys: String, CodingKey {
        case code, message, details
        case requestId = "request_id"
        case hint, table, column
    }

    public var description: String {
        var parts = ["[\(code)] \(message)"]
        if let hint { parts.append("Hint: \(hint)") }
        if let table { parts.append("Table: \(table)") }
        if let column { parts.append("Column: \(column)") }
        if let details { parts.append("Details: \(details)") }
        return parts.joined(separator: " | ")
    }

    public var errorDescription: String? {
        description
    }
}

/// Internal wrapper to decode error responses from the gateway.
/// Falls back gracefully when the response isn't structured JSON.
extension QailError {
    static func from(data: Data, status: Int) -> QailError {
        let decoder = JSONDecoder()
        if var err = try? decoder.decode(QailError.self, from: data) {
            err.status = status
            return err
        }
        if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            let code = (json["code"] as? String)
                ?? (json["error"] as? String)
                ?? "HTTP_\(status)"
            let message = (json["message"] as? String)
                ?? (json["error_description"] as? String)
                ?? (json["error"] as? String)
                ?? HTTPURLResponse.localizedString(forStatusCode: status)
            let details = (json["details"] as? String)
                ?? (json["detail"] as? String)
            let requestId = (json["request_id"] as? String)
                ?? (json["requestId"] as? String)
            var err = QailError(
                code: code,
                message: message,
                details: details,
                requestId: requestId,
                hint: json["hint"] as? String,
                table: json["table"] as? String,
                column: json["column"] as? String
            )
            err.status = status
            return err
        }
        // Fallback: raw text
        let rawText = String(data: data, encoding: .utf8)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let text = rawText?.isEmpty == false
            ? rawText!
            : HTTPURLResponse.localizedString(forStatusCode: status)
        var err = QailError(
            code: "HTTP_\(status)",
            message: text,
            details: nil,
            requestId: nil,
            hint: nil,
            table: nil,
            column: nil
        )
        err.status = status
        return err
    }
}

// MARK: - Transactions

/// Transaction session start response.
public struct TxnBeginResponse: Decodable, Sendable {
    public let txnId: String

    enum CodingKeys: String, CodingKey {
        case txnId
    }
}

/// Transaction session end response.
public struct TxnEndResponse: Decodable, Sendable {
    public let status: String
}

/// Savepoint request.
public struct SavepointRequest: Encodable, Sendable {
    public let action: String
    public let name: String
}

/// Savepoint response.
public struct SavepointResponse: Decodable, Sendable {
    public let action: String
    public let name: String
}
