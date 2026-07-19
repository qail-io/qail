import 'package:web_socket_channel/web_socket_channel.dart';

import 'models.dart';

/// Browsers cannot set handshake headers, so web defaults to query auth.
const defaultWebSocketAuthMode = WebSocketAuthMode.query;

/// Platform-default WebSocket connector for runtimes without `dart:io`
/// (e.g. Flutter web). Browsers cannot set handshake headers, so
/// [headers] is ignored — token auth happens via query param.
WebSocketChannel connectWebSocket(Uri uri, Map<String, String> headers) =>
    WebSocketChannel.connect(uri);
