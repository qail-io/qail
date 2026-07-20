# qail (Dart)

Dart SDK for the [Qail Gateway](https://github.com/qail-io/qail) — a zero-overhead database gateway with compile-time safety. Pure Dart over HTTP/WebSocket (no FFI), works in Flutter (iOS, Android, web, desktop) and server-side Dart.

Mirrors the Swift and Kotlin SDK surface area.

## Install

```yaml
dependencies:
  qail:
    path: ../qail.rs/sdk/dart # or git/pub source
```

## Quick Start

```dart
import 'package:qail/qail.dart';

final qail = QailClient(QailConfig(
  url: 'http://localhost:8080',
  token: 'your-jwt-token',
));

// Typed rows: pass a fromJson tear-off (Dart generics are not reified)
final users = await qail.from('users', User.fromJson)
    .select(['id', 'name', 'email'])
    .where('active', FilterOp.eq, 'true')
    .desc('created_at')
    .limit(10)
    .all();

// Untyped rows: omit the decoder to get Map<String, dynamic>
final rows = await qail.from('users').limit(10).all();

// Get by ID
final user = await qail.from('users', User.fromJson).get('uuid-123');

// Insert
await qail.into('users', User.fromJson)
    .values({'name': 'Alice', 'email': 'alice@example.com'})
    .returning('*')
    .exec();

// Update
await qail.update('users', User.fromJson)
    .set({'name': 'Alice Updated'})
    .returning('*')
    .exec('uuid-123');

// Delete
await qail.delete('users').exec('uuid-123');
```

A `User.fromJson` decoder is any `T Function(Map<String, dynamic>)` — a
hand-written factory or a `json_serializable` constructor tear-off. JSON keys
pass through verbatim; the gateway speaks snake_case.

## Refreshable auth

```dart
final qail = QailClient(QailConfig(
  url: 'https://engine.example.com',
  tokenProvider: () async => await myAuth.freshAccessToken(),
));
```

`tokenProvider` is called per request, so single-flight refresh logic can live
behind it.

## Dynamic headers

Use `headersProvider` when a header can change after the client is created:

```dart
final qail = QailClient(QailConfig(
  url: 'https://engine.example.com',
  headers: const {'X-App': 'deck'},
  headersProvider: () => activeTenantId == null
      ? const {}
      : {'X-Impersonate-Tenant': activeTenantId!},
));
```

The provider is evaluated for every HTTP request and WebSocket connection.
Static `headers` are applied first, then provider headers, then any
request-specific headers.

## Raw DSL, batch, transactions

```dart
final res = await qail.query('get users fields id, name limit 10', User.fromJson);
final fast = await qail.queryFast('get users');
final batch = await qail.batch(['get users', 'get orders']);

final txn = await qail.beginTxn();
await txn.query("add users name = 'X'");
await txn.commit(); // or txn.rollback()
```

## Realtime

```dart
final sub = qail.subscribe('orders', (payload) {
  print('Got: $payload');
});
// Later...
sub.unsubscribe();
```

The WebSocket auth mode resolves per platform automatically: `header` on
mobile/desktop/server, `query` on web (browsers cannot set handshake
headers). Set `wsAuthMode` explicitly in `QailConfig` to override.

## Raw HTTP (Workers endpoints)

For routes that are not gateway auto-REST tables:

```dart
final result = await qail.post(
  '/booking/orders/draft',
  body: {'sailing_id': 'abc'},
  decoder: (json) => Order.fromJson(json as Map<String, dynamic>),
);
```

`get` / `post` / `patch` / `put` / `deleteRaw` are available.

## Errors

All non-2xx responses throw `QailError` with the gateway's structured
`ApiError` shape: `status`, `code`, `message`, plus enriched `hint`, `table`,
`column`, `details`, and `requestId` when present.

## Test

```bash
dart test
```
