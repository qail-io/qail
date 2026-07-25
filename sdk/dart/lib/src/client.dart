import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

import 'builders.dart';
import 'models.dart';
import 'ws_connect_default.dart' if (dart.library.io) 'ws_connect_io.dart'
    as ws;

// ─── Configuration ──────────────────────────────────────────────────

/// Creates the WebSocket connection for [QailClient.subscribe].
/// Override in [QailConfig] for custom runtimes or testing.
typedef WebSocketConnector = WebSocketChannel Function(
  Uri uri,
  Map<String, String> headers,
);

/// Supplies headers at request time.
///
/// Use this for values that can change after [QailClient] is constructed,
/// such as an active tenant or locale.
typedef HeadersProvider = Map<String, String> Function();

/// Configuration for the Qail client.
class QailConfig {
  const QailConfig({
    required this.url,
    this.token,
    this.tokenProvider,
    this.headers = const {},
    this.headersProvider,
    this.timeout = const Duration(seconds: 30),
    this.wsAuthMode,
    this.wsTokenQueryParam = 'access_token',
    this.httpClient,
    this.webSocketConnector,
  });

  /// Gateway base URL (e.g. `https://engine.example.com`).
  final String url;

  /// Static JWT token.
  final String? token;

  /// Async token provider for refreshable auth (used when [token] is null).
  final Future<String> Function()? tokenProvider;

  /// Additional default headers sent with every request.
  final Map<String, String> headers;

  /// Dynamic headers evaluated for every HTTP request and WebSocket connect.
  ///
  /// These override [headers]. Request-specific headers override both.
  final HeadersProvider? headersProvider;

  /// Request timeout (default: 30 seconds).
  final Duration timeout;

  /// WebSocket auth mode for [QailClient.subscribe]. When null (the
  /// default), resolves per platform: `header` on mobile/desktop/server,
  /// `query` on web (browsers cannot set handshake headers).
  final WebSocketAuthMode? wsAuthMode;

  /// Query param key for [WebSocketAuthMode.query] (default: `access_token`).
  final String wsTokenQueryParam;

  /// Custom HTTP client (optional; useful for testing).
  final http.Client? httpClient;

  /// Custom WebSocket connector (optional; useful for testing).
  final WebSocketConnector? webSocketConnector;
}

// ─── Client ─────────────────────────────────────────────────────────

/// Qail Gateway client for Dart / Flutter.
///
/// Provides fluent query builders, raw DSL execution, and utility
/// endpoints — mirroring the Swift and Kotlin SDK surface area.
///
/// ```dart
/// final qail = QailClient(QailConfig(
///   url: 'https://engine.example.com',
///   token: 'my-jwt',
/// ));
///
/// // Fluent query (decoder is a fromJson tear-off)
/// final users = await qail.from('users', User.fromJson)
///     .where('active', FilterOp.eq, 'true')
///     .limit(10)
///     .all();
///
/// // Raw DSL
/// final res = await qail.query('get users fields id, name limit 10');
/// ```
class QailClient {
  QailClient(this.config)
      : _baseUrl = config.url.replaceAll(RegExp(r'/+$'), ''),
        _http = config.httpClient ?? http.Client();

  final QailConfig config;
  final String _baseUrl;
  final http.Client _http;

  /// Closes the underlying HTTP client. Call when the client is no
  /// longer needed (skip if you injected your own [QailConfig.httpClient]).
  void close() => _http.close();

  /// The effective WebSocket auth mode: [QailConfig.wsAuthMode] when set,
  /// otherwise the platform default (`header` on `dart:io` platforms,
  /// `query` on web).
  WebSocketAuthMode get wsAuthMode =>
      config.wsAuthMode ?? ws.defaultWebSocketAuthMode;

  // ── Query builder entry points ──────────────────────────────────

  /// Start a SELECT query on a table. Without [fromJson], rows are
  /// returned as `Map<String, dynamic>`.
  SelectBuilder<T> from<T>(String table, [RowDecoder<T>? fromJson]) =>
      SelectBuilder(this, table, fromJson ?? defaultRowDecoder<T>);

  /// Start an INSERT on a table.
  InsertBuilder<T> into<T>(String table, [RowDecoder<T>? fromJson]) =>
      InsertBuilder(this, table, fromJson ?? defaultRowDecoder<T>);

  /// Start an UPDATE on a table.
  UpdateBuilder<T> update<T>(String table, [RowDecoder<T>? fromJson]) =>
      UpdateBuilder(this, table, fromJson ?? defaultRowDecoder<T>);

  /// Start a DELETE on a table.
  DeleteBuilder delete(String table) => DeleteBuilder(this, table);

  // ── Raw QAIL text protocol ──────────────────────────────────────

  /// Execute raw Qail DSL text.
  Future<QueryResponse<T>> query<T>(String dsl, [RowDecoder<T>? fromJson]) =>
      requestJson(
        'POST',
        '/qail',
        body: dsl,
        contentType: 'text/plain',
        decode: (json) => QueryResponse.fromJson(
          json as Map<String, dynamic>,
          fromJson ?? defaultRowDecoder<T>,
        ),
      );

  /// Execute raw Qail DSL via fast protocol (array-of-arrays).
  Future<FastQueryResponse> queryFast(String dsl) => requestJson(
        'POST',
        '/qail/fast',
        body: dsl,
        contentType: 'text/plain',
        decode: (json) =>
            FastQueryResponse.fromJson(json as Map<String, dynamic>),
      );

  /// Execute a batch of Qail DSL queries.
  Future<BatchResponse<T>> batch<T>(
    List<String> queries, [
    RowDecoder<T>? fromJson,
  ]) =>
      requestJson(
        'POST',
        '/qail/batch',
        body: {'queries': queries},
        decode: (json) => BatchResponse.fromJson(
          json as Map<String, dynamic>,
          fromJson ?? defaultRowDecoder<T>,
        ),
      );

  // ── Utilities ───────────────────────────────────────────────────

  /// Health check.
  Future<HealthResponse> health() => requestJson(
        'GET',
        '/health',
        decode: (json) => HealthResponse.fromJson(json as Map<String, dynamic>),
      );

  /// Get OpenAPI spec.
  Future<Map<String, dynamic>> openapi() => requestJson(
        'GET',
        '/api/_openapi',
        decode: (json) => json as Map<String, dynamic>,
      );

  /// Get schema introspection.
  Future<Map<String, dynamic>> schema() => requestJson(
        'GET',
        '/api/_schema',
        decode: (json) => json as Map<String, dynamic>,
      );

  /// Generate TypeScript interfaces from the gateway schema.
  Future<String> generateTypes() =>
      requestText('GET', '/api/_schema/typescript');

  // ── Transactions ────────────────────────────────────────────────

  /// Start a new transaction session.
  Future<QailTxnSession> beginTxn() async {
    final res = await requestJson(
      'POST',
      '/txn/begin',
      decode: (json) => TxnBeginResponse.fromJson(json as Map<String, dynamic>),
    );
    return QailTxnSession(this, res.txnId);
  }

  // ── Realtime (WebSocket) ────────────────────────────────────────

  /// Subscribe to a Postgres LISTEN channel via WebSocket.
  ///
  /// ```dart
  /// final sub = qail.subscribe('orders', (payload) {
  ///   print('Got: $payload');
  /// });
  /// // Later...
  /// sub.unsubscribe();
  /// ```
  QailSubscription subscribe(
    String channel,
    void Function(String payload) onMessage,
  ) {
    final sub = _WebSocketSubscription(channel);

    Future(() async {
      try {
        final token = await _resolveToken();
        if (!sub._alive) return;

        final uri = buildWebSocketUri(token);
        final headers = _resolveHeaders();
        if (wsAuthMode == WebSocketAuthMode.header && token != null) {
          headers['Authorization'] = 'Bearer $token';
        }

        final connect = config.webSocketConnector ?? ws.connectWebSocket;
        final wsChannel = connect(uri, headers);
        if (!sub._alive) {
          await wsChannel.sink.close();
          return;
        }
        sub._attach(wsChannel);

        wsChannel.sink
            .add(jsonEncode({'action': 'listen', 'channel': channel}));

        await for (final frame in wsChannel.stream) {
          if (!sub._alive) break;
          if (frame is! String) continue;
          try {
            final msg = jsonDecode(frame);
            if (msg is! Map<String, dynamic>) continue;
            if (msg['channel'] != channel) continue;
            final payload = msg['payload'];
            if (payload == null) continue;
            onMessage(payload is String ? payload : jsonEncode(payload));
          } catch (_) {
            // Non-JSON message, ignore.
          }
        }
      } catch (_) {
        // Connection failed or dropped.
      } finally {
        sub._markClosed();
      }
    });

    return sub;
  }

  /// Builds the WebSocket URL for [subscribe], honoring
  /// [QailConfig.wsAuthMode]. Exposed for testing.
  Uri buildWebSocketUri([String? token]) {
    var wsBase = _baseUrl;
    if (wsBase.startsWith('https')) {
      wsBase = wsBase.replaceFirst('https', 'wss');
    } else if (wsBase.startsWith('http')) {
      wsBase = wsBase.replaceFirst('http', 'ws');
    }
    final basePath = '$wsBase/ws';
    if (token == null || wsAuthMode != WebSocketAuthMode.query) {
      return Uri.parse(basePath);
    }
    final sep = basePath.contains('?') ? '&' : '?';
    final key = Uri.encodeComponent(config.wsTokenQueryParam);
    final value = Uri.encodeComponent(token);
    return Uri.parse('$basePath$sep$key=$value');
  }

  // ── Raw HTTP (for Workers endpoints) ────────────────────────────
  // Mirrors the Swift/Kotlin SDKs' raw-HTTP surface so app code can reach
  // arbitrary Worker routes that are not gateway auto-REST tables.

  /// GET an arbitrary path, decoding the JSON response with [decoder].
  Future<T> get<T>(String path, {PayloadDecoder<T>? decoder}) =>
      requestJson('GET', path, decode: decoder ?? _defaultPayloadDecoder<T>);

  /// POST an arbitrary path with an optional JSON body.
  Future<T> post<T>(String path, {Object? body, PayloadDecoder<T>? decoder}) =>
      requestJson('POST', path,
          body: body, decode: decoder ?? _defaultPayloadDecoder<T>);

  /// PATCH an arbitrary path with an optional JSON body.
  Future<T> patch<T>(String path, {Object? body, PayloadDecoder<T>? decoder}) =>
      requestJson('PATCH', path,
          body: body, decode: decoder ?? _defaultPayloadDecoder<T>);

  /// PUT an arbitrary path with an optional JSON body.
  Future<T> put<T>(String path, {Object? body, PayloadDecoder<T>? decoder}) =>
      requestJson('PUT', path,
          body: body, decode: decoder ?? _defaultPayloadDecoder<T>);

  /// DELETE an arbitrary path with an optional JSON body.
  /// Named `deleteRaw` to avoid colliding with [delete] (the table
  /// CRUD builder).
  Future<T> deleteRaw<T>(
    String path, {
    Object? body,
    PayloadDecoder<T>? decoder,
  }) =>
      requestJson('DELETE', path,
          body: body, decode: decoder ?? _defaultPayloadDecoder<T>);

  static T _defaultPayloadDecoder<T>(Object? json) => json as T;

  // ── Internal HTTP ───────────────────────────────────────────────

  /// Internal typed JSON request. Public for use by the builders;
  /// not intended for application code.
  Future<T> requestJson<T>(
    String method,
    String path, {
    Object? body,
    String contentType = 'application/json',
    Map<String, String>? extraHeaders,
    required PayloadDecoder<T> decode,
  }) async {
    final response = await _send(
      method,
      path,
      body: body,
      contentType: contentType,
      extraHeaders: extraHeaders,
    );
    return decode(jsonDecode(utf8.decode(response.bodyBytes)));
  }

  /// Internal text request.
  Future<String> requestText(String method, String path) async {
    final response = await _send(method, path);
    return utf8.decode(response.bodyBytes);
  }

  Future<http.Response> _send(
    String method,
    String path, {
    Object? body,
    String contentType = 'application/json',
    Map<String, String>? extraHeaders,
  }) async {
    final request = http.Request(method, Uri.parse('$_baseUrl$path'));

    request.headers['Content-Type'] = contentType;
    _resolveHeaders().forEach((k, v) => request.headers[k] = v);

    final token = await _resolveToken();
    if (token != null) {
      request.headers['Authorization'] = 'Bearer $token';
    }

    extraHeaders?.forEach((k, v) => request.headers[k] = v);

    if (body != null && method != 'GET') {
      request.body = body is String ? body : jsonEncode(body);
    }

    final streamed = await _http.send(request).timeout(config.timeout);
    final response = await http.Response.fromStream(streamed);

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw _parseError(response);
    }
    return response;
  }

  QailError _parseError(http.Response response) {
    final text = utf8.decode(response.bodyBytes);
    QailErrorBody body;
    try {
      body = QailErrorBody.fromJson(jsonDecode(text) as Map<String, dynamic>);
    } catch (_) {
      body = QailErrorBody(
        code: 'HTTP_${response.statusCode}',
        message: text,
      );
    }
    return QailError(status: response.statusCode, body: body);
  }

  Future<String?> _resolveToken() async =>
      config.token ?? await config.tokenProvider?.call();

  Map<String, String> _resolveHeaders() => {
        ...config.headers,
        ...?config.headersProvider?.call(),
      };
}

// ─── Transaction Session ────────────────────────────────────────────

/// Handle for an active transaction session.
class QailTxnSession {
  QailTxnSession(this._client, this.txnId);

  final QailClient _client;
  final String txnId;

  Map<String, String> get _txnHeader => {'X-Transaction-Id': txnId};

  /// Execute a query within this transaction.
  Future<QueryResponse<T>> query<T>(String dsl, [RowDecoder<T>? fromJson]) =>
      _client.requestJson(
        'POST',
        '/txn/query',
        body: dsl,
        contentType: 'text/plain',
        extraHeaders: _txnHeader,
        decode: (json) => QueryResponse.fromJson(
          json as Map<String, dynamic>,
          fromJson ?? defaultRowDecoder<T>,
        ),
      );

  /// Commit the transaction.
  Future<TxnEndResponse> commit() => _client.requestJson(
        'POST',
        '/txn/commit',
        extraHeaders: _txnHeader,
        decode: (json) => TxnEndResponse.fromJson(json as Map<String, dynamic>),
      );

  /// Rollback the transaction.
  Future<TxnEndResponse> rollback() => _client.requestJson(
        'POST',
        '/txn/rollback',
        extraHeaders: _txnHeader,
        decode: (json) => TxnEndResponse.fromJson(json as Map<String, dynamic>),
      );

  /// Create or release a savepoint within this transaction.
  Future<SavepointResponse> savepoint(String action, String name) =>
      _client.requestJson(
        'POST',
        '/txn/savepoint',
        body: {'action': action, 'name': name},
        extraHeaders: _txnHeader,
        decode: (json) =>
            SavepointResponse.fromJson(json as Map<String, dynamic>),
      );
}

// ─── Subscription ───────────────────────────────────────────────────

class _WebSocketSubscription implements QailSubscription {
  _WebSocketSubscription(this._channel);

  final String _channel;
  bool _alive = true;
  WebSocketChannel? _ws;

  void _attach(WebSocketChannel ws) => _ws = ws;

  void _markClosed() => _alive = false;

  @override
  bool get active => _alive && _ws != null;

  @override
  void unsubscribe() {
    _alive = false;
    final ws = _ws;
    if (ws != null) {
      try {
        ws.sink.add(jsonEncode({'action': 'unlisten', 'channel': _channel}));
      } catch (_) {
        // Sink already closed.
      }
      ws.sink.close();
    }
  }
}
