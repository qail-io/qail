import Foundation

// MARK: - Filter Operators

/// Filter operators matching PostgREST-style query params.
public enum FilterOp: String, Sendable {
    case eq, neq, gt, gte, lt, lte
    case like, ilike
    case `in`
    case `is`
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

/// Mutation response from POST/PATCH operations.
public struct MutationResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let data: T
    public let rowsAffected: Int

    enum CodingKeys: String, CodingKey {
        case data
        case rowsAffected = "rows_affected"
    }
}

/// Raw DSL query response from `POST /qail`.
public struct QueryResponse<T: Decodable>: Decodable, @unchecked Sendable {
    public let data: [T]
    public let rowsAffected: Int
    public let columns: [String]

    enum CodingKeys: String, CodingKey {
        case data
        case rowsAffected = "rows_affected"
        case columns
    }
}

/// Health check response.
public struct HealthResponse: Decodable, Sendable {
    public let status: String
    public let version: String
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
    private let task: URLSessionWebSocketTask
    private let channel: String
    private let onMessage: @Sendable (String) -> Void
    private var _active = true

    init(task: URLSessionWebSocketTask, channel: String, onMessage: @escaping @Sendable (String) -> Void) {
        self.task = task
        self.channel = channel
        self.onMessage = onMessage
    }

    public var active: Bool { _active && task.state == .running }

    public func unsubscribe() {
        _active = false
        let msg = #"{"action":"unlisten","channel":"\#(channel)"}"#
        task.send(.string(msg)) { [weak self] _ in
            self?.task.cancel(with: .goingAway, reason: nil)
        }
    }

    func startReceiving() {
        task.receive { [weak self] result in
            guard let self, self._active else { return }
            switch result {
            case .success(.string(let text)):
                if let data = text.data(using: .utf8),
                   let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let ch = json["channel"] as? String, ch == self.channel,
                   let payload = json["payload"] as? String {
                    self.onMessage(payload)
                }
                self.startReceiving() // Continue listening
            case .success(.data(let data)):
                if let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                   let ch = json["channel"] as? String, ch == self.channel,
                   let payload = json["payload"] as? String {
                    self.onMessage(payload)
                }
                self.startReceiving()
            case .failure:
                self._active = false
            @unknown default:
                break
            }
        }
    }
}

// MARK: - Error

/// Structured error from the Qail Gateway.
///
/// Matches the gateway `ApiError` JSON shape including enriched
/// `hint`, `table`, and `column` fields for developer-friendly diagnostics.
public struct QailError: Error, Decodable, Sendable, CustomStringConvertible {
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
        // Fallback: raw text
        let text = String(data: data, encoding: .utf8) ?? "Unknown error"
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
