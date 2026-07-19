/// Dart client SDK for the Qail Gateway.
///
/// Mirrors the TypeScript, Swift, and Kotlin SDK surface area: fluent
/// query builders, raw DSL execution, transactions, and realtime
/// WebSocket subscriptions.
library;

export 'src/builders.dart'
    show DeleteBuilder, InsertBuilder, SelectBuilder, UpdateBuilder;
export 'src/client.dart'
    show QailClient, QailConfig, QailTxnSession, WebSocketConnector;
export 'src/models.dart';
