// ─── Decoders ───────────────────────────────────────────────────────

/// Decodes one JSON row object into [T].
///
/// Dart generics are not reified, so typed methods take an explicit
/// decoder — usually a `fromJson` constructor tear-off:
///
/// ```dart
/// final users = await qail.from('users', User.fromJson).all();
/// ```
///
/// When omitted, rows are returned as `Map<String, dynamic>`.
typedef RowDecoder<T> = T Function(Map<String, dynamic> json);

/// Decodes an arbitrary JSON payload into [T] (used by the raw HTTP verbs,
/// where the response is not necessarily a JSON object).
typedef PayloadDecoder<T> = T Function(Object? json);

/// Fallback decoder when no [RowDecoder] is supplied: returns the raw map.
T defaultRowDecoder<T>(Map<String, dynamic> json) => json as T;

int _asInt(Object? value) => (value as num).toInt();
int? _asIntOrNull(Object? value) => (value as num?)?.toInt();

// ─── WebSocket Auth ─────────────────────────────────────────────────

/// Authentication mode for WebSocket connections.
enum WebSocketAuthMode {
  /// Do not include token during WS handshake.
  none,

  /// Send `Authorization: Bearer <token>` header during WS handshake.
  /// Not supported on web — browsers cannot set handshake headers.
  header,

  /// Append token as query parameter (default key: `access_token`).
  query,
}

// ─── Filter Operators ───────────────────────────────────────────────

/// Filter operators matching PostgREST-style query params.
enum FilterOp {
  eq('eq'),
  ne('ne'),
  gt('gt'),
  gte('gte'),
  lt('lt'),
  lte('lte'),
  like('like'),
  ilike('ilike'),
  inList('in'),
  notIn('not_in'),
  isNull('is_null'),
  isNotNull('is_not_null'),
  contains('contains');

  const FilterOp(this.value);

  /// Wire representation used in query params (e.g. `active.eq=true`).
  final String value;
}

/// Aggregate function type.
enum AggregateFunc {
  count('count'),
  sum('sum'),
  avg('avg'),
  min('min'),
  max('max');

  const AggregateFunc(this.value);

  final String value;
}

// ─── Responses ──────────────────────────────────────────────────────

/// Metadata included in successful API responses.
class ResponseMetadata {
  const ResponseMetadata({required this.requestId, this.durationMs});

  factory ResponseMetadata.fromJson(Map<String, dynamic> json) =>
      ResponseMetadata(
        requestId: json['request_id'] as String,
        durationMs: (json['duration_ms'] as num?)?.toDouble(),
      );

  final String requestId;
  final double? durationMs;
}

ResponseMetadata? _metadataFromJson(Object? json) => json == null
    ? null
    : ResponseMetadata.fromJson(json as Map<String, dynamic>);

/// Paginated list response from `GET /api/{table}`.
class ListResponse<T> {
  const ListResponse({
    required this.data,
    required this.count,
    this.total,
    required this.limit,
    required this.offset,
  });

  factory ListResponse.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      ListResponse(
        data: (json['data'] as List)
            .map((row) => decodeRow(row as Map<String, dynamic>))
            .toList(),
        count: _asInt(json['count']),
        total: _asIntOrNull(json['total']),
        limit: _asInt(json['limit']),
        offset: _asInt(json['offset']),
      );

  final List<T> data;
  final int count;
  final int? total;
  final int limit;
  final int offset;
}

/// Single-row response from `GET /api/{table}/{id}`.
class SingleResponse<T> {
  const SingleResponse({required this.data});

  factory SingleResponse.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      SingleResponse(data: decodeRow(json['data'] as Map<String, dynamic>));

  final T data;
}

/// Mutation response from POST/PATCH operations.
class MutationResponse<T> {
  const MutationResponse({required this.data, this.count, this.metadata});

  factory MutationResponse.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      MutationResponse(
        data: decodeRow(json['data'] as Map<String, dynamic>),
        count: _asIntOrNull(json['count']),
        metadata: _metadataFromJson(json['metadata']),
      );

  final T data;
  final int? count;
  final ResponseMetadata? metadata;
}

/// Raw DSL query response from `POST /qail`.
class QueryResponse<T> {
  const QueryResponse({required this.rows, required this.count, this.metadata});

  factory QueryResponse.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      QueryResponse(
        rows: (json['rows'] as List)
            .map((row) => decodeRow(row as Map<String, dynamic>))
            .toList(),
        count: _asInt(json['count']),
        metadata: _metadataFromJson(json['metadata']),
      );

  final List<T> rows;
  final int count;
  final ResponseMetadata? metadata;
}

/// Fast query response (array-of-arrays) from `POST /qail/fast`.
class FastQueryResponse {
  const FastQueryResponse({
    required this.rows,
    required this.count,
    this.metadata,
  });

  factory FastQueryResponse.fromJson(Map<String, dynamic> json) =>
      FastQueryResponse(
        rows: (json['rows'] as List)
            .map((row) => (row as List).cast<Object?>())
            .toList(),
        count: _asInt(json['count']),
        metadata: _metadataFromJson(json['metadata']),
      );

  final List<List<Object?>> rows;
  final int count;
  final ResponseMetadata? metadata;
}

/// Health check response.
class HealthResponse {
  const HealthResponse({
    required this.status,
    required this.version,
    this.poolActive,
    this.poolIdle,
  });

  factory HealthResponse.fromJson(Map<String, dynamic> json) => HealthResponse(
        status: json['status'] as String,
        version: json['version'] as String,
        poolActive: _asIntOrNull(json['pool_active']),
        poolIdle: _asIntOrNull(json['pool_idle']),
      );

  final String status;
  final String version;
  final int? poolActive;
  final int? poolIdle;
}

/// Batch query response.
class BatchResponse<T> {
  const BatchResponse({
    required this.results,
    required this.total,
    required this.success,
    this.metadata,
  });

  factory BatchResponse.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      BatchResponse(
        results: (json['results'] as List)
            .map((r) =>
                BatchResult.fromJson(r as Map<String, dynamic>, decodeRow))
            .toList(),
        total: _asInt(json['total']),
        success: _asInt(json['success']),
        metadata: _metadataFromJson(json['metadata']),
      );

  final List<BatchResult<T>> results;
  final int total;
  final int success;
  final ResponseMetadata? metadata;
}

/// Batch result for multi-query execution.
class BatchResult<T> {
  const BatchResult({
    required this.index,
    required this.success,
    this.rows,
    this.count,
    this.error,
  });

  factory BatchResult.fromJson(
    Map<String, dynamic> json,
    RowDecoder<T> decodeRow,
  ) =>
      BatchResult(
        index: _asInt(json['index']),
        success: json['success'] as bool,
        rows: (json['rows'] as List?)
            ?.map((row) => decodeRow(row as Map<String, dynamic>))
            .toList(),
        count: _asIntOrNull(json['count']),
        error: json['error'] as String?,
      );

  final int index;
  final bool success;
  final List<T>? rows;
  final int? count;
  final String? error;
}

/// Delete confirmation.
class DeleteResponse {
  const DeleteResponse({required this.deleted});

  factory DeleteResponse.fromJson(Map<String, dynamic> json) =>
      DeleteResponse(deleted: json['deleted'] as bool);

  final bool deleted;
}

/// Aggregate query response.
class AggregateResponse {
  const AggregateResponse({
    required this.data,
    required this.count,
    this.metadata,
  });

  factory AggregateResponse.fromJson(Map<String, dynamic> json) =>
      AggregateResponse(
        data: (json['data'] as List).cast<Map<String, dynamic>>(),
        count: _asInt(json['count']),
        metadata: _metadataFromJson(json['metadata']),
      );

  final List<Map<String, dynamic>> data;
  final int count;
  final ResponseMetadata? metadata;
}

// ─── Transactions ───────────────────────────────────────────────────

/// Transaction session start response.
class TxnBeginResponse {
  const TxnBeginResponse({required this.txnId});

  factory TxnBeginResponse.fromJson(Map<String, dynamic> json) =>
      TxnBeginResponse(txnId: json['txn_id'] as String);

  final String txnId;
}

/// Transaction session end response.
class TxnEndResponse {
  const TxnEndResponse({required this.status});

  factory TxnEndResponse.fromJson(Map<String, dynamic> json) =>
      TxnEndResponse(status: json['status'] as String);

  final String status;
}

/// Savepoint response.
class SavepointResponse {
  const SavepointResponse({required this.action, required this.name});

  factory SavepointResponse.fromJson(Map<String, dynamic> json) =>
      SavepointResponse(
        action: json['action'] as String,
        name: json['name'] as String,
      );

  final String action;
  final String name;
}

// ─── Realtime ───────────────────────────────────────────────────────

/// Subscription handle for WebSocket LISTEN/NOTIFY.
abstract interface class QailSubscription {
  /// Stop listening and close the underlying connection.
  void unsubscribe();

  /// Whether the subscription is still receiving messages.
  bool get active;
}

// ─── Error ──────────────────────────────────────────────────────────

/// Structured error body from the Qail Gateway.
///
/// Matches the gateway `ApiError` JSON shape including enriched
/// `hint`, `table`, and `column` fields.
class QailErrorBody {
  const QailErrorBody({
    required this.code,
    required this.message,
    this.details,
    this.requestId,
    this.hint,
    this.table,
    this.column,
  });

  factory QailErrorBody.fromJson(Map<String, dynamic> json) => QailErrorBody(
        code: json['code'] as String,
        message: json['message'] as String,
        details: json['details'] as String?,
        requestId: json['request_id'] as String?,
        hint: json['hint'] as String?,
        table: json['table'] as String?,
        column: json['column'] as String?,
      );

  final String code;
  final String message;
  final String? details;
  final String? requestId;
  final String? hint;
  final String? table;
  final String? column;
}

/// Exception wrapping a structured gateway error.
class QailError implements Exception {
  const QailError({required this.status, required this.body});

  final int status;
  final QailErrorBody body;

  String get code => body.code;
  String get message => body.message;
  String? get hint => body.hint;
  String? get table => body.table;
  String? get column => body.column;
  String? get details => body.details;
  String? get requestId => body.requestId;

  @override
  String toString() {
    final parts = ['[${body.code}] ${body.message}'];
    if (body.hint != null) parts.add('Hint: ${body.hint}');
    if (body.table != null) parts.add('Table: ${body.table}');
    if (body.column != null) parts.add('Column: ${body.column}');
    if (body.details != null) parts.add('Details: ${body.details}');
    return parts.join(' | ');
  }
}
