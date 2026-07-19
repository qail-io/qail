import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'models.dart';

/// `dart:io` runtimes support handshake headers, matching Kotlin/Swift.
const defaultWebSocketAuthMode = WebSocketAuthMode.header;

/// Platform-default WebSocket connector for `dart:io` runtimes
/// (mobile, desktop, server) — supports handshake headers.
WebSocketChannel connectWebSocket(Uri uri, Map<String, String> headers) =>
    IOWebSocketChannel.connect(uri, headers: headers.isEmpty ? null : headers);
