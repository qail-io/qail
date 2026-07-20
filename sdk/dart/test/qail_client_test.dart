import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:qail/qail.dart';
import 'package:test/test.dart';

// ─── Test Model ─────────────────────────────────────────────────────

class User {
  const User({required this.id, required this.name});

  factory User.fromJson(Map<String, dynamic> json) => User(
        id: (json['id'] as num).toInt(),
        name: json['name'] as String,
      );

  final int id;
  final String name;
}

// ─── Helpers ────────────────────────────────────────────────────────

QailClient mockClient(
  Future<http.Response> Function(http.Request request) handler, {
  String? token = 'test-jwt',
  Future<String> Function()? tokenProvider,
}) {
  return QailClient(QailConfig(
    url: 'http://localhost:8080',
    token: token,
    tokenProvider: tokenProvider,
    httpClient: MockClient(handler),
  ));
}

http.Response jsonResponse(String json, {int status = 200}) =>
    http.Response(json, status, headers: {'content-type': 'application/json'});

void main() {
  // ── Health ──────────────────────────────────────────────────────

  test('health', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/health');
      expect(request.method, 'GET');
      return jsonResponse('{"status":"ok","version":"0.20.1"}');
    });
    final res = await qail.health();
    expect(res.status, 'ok');
    expect(res.version, '0.20.1');
  });

  // ── Auth ────────────────────────────────────────────────────────

  test('bearer token', () async {
    final qail = mockClient((request) async {
      expect(request.headers['Authorization'], 'Bearer test-jwt');
      return jsonResponse('{"status":"ok","version":"0.20.1"}');
    });
    await qail.health();
  });

  test('token provider', () async {
    final qail = mockClient(
      (request) async {
        expect(request.headers['Authorization'], 'Bearer dynamic-token');
        return jsonResponse('{"status":"ok","version":"0.20.1"}');
      },
      token: null,
      tokenProvider: () async => 'dynamic-token',
    );
    await qail.health();
  });

  test('headers provider is evaluated per request with explicit precedence',
      () async {
    var dynamicValue = 'first';
    final seen = <Map<String, String>>[];
    final qail = QailClient(QailConfig(
      url: 'http://localhost:8080',
      token: 'test-jwt',
      headers: const {
        'X-Static': 'static',
        'X-Precedence': 'static',
      },
      headersProvider: () => {
        'X-Dynamic': dynamicValue,
        'X-Precedence': 'provider',
      },
      httpClient: MockClient((request) async {
        seen.add(Map.of(request.headers));
        return jsonResponse('{}');
      }),
    ));

    await qail.requestJson<Map<String, dynamic>>(
      'GET',
      '/first',
      extraHeaders: const {'X-Precedence': 'request'},
      decode: (json) => json as Map<String, dynamic>,
    );
    dynamicValue = 'second';
    await qail.requestJson<Map<String, dynamic>>(
      'GET',
      '/second',
      decode: (json) => json as Map<String, dynamic>,
    );

    expect(seen[0]['X-Static'], 'static');
    expect(seen[0]['X-Dynamic'], 'first');
    expect(seen[0]['X-Precedence'], 'request');
    expect(seen[0]['Authorization'], 'Bearer test-jwt');
    expect(seen[1]['X-Dynamic'], 'second');
    expect(seen[1]['X-Precedence'], 'provider');
  });

  // ── Raw DSL ─────────────────────────────────────────────────────

  test('raw query', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/qail');
      expect(request.method, 'POST');
      expect(request.headers['Content-Type'], startsWith('text/plain'));
      expect(request.body, 'get users fields id, name limit 10');
      return jsonResponse('{"rows":[{"id":1,"name":"Alice"}],"count":1,'
          '"metadata":{"request_id":"test-123"}}');
    });
    final res =
        await qail.query('get users fields id, name limit 10', User.fromJson);
    expect(res.rows, hasLength(1));
    expect(res.rows[0].name, 'Alice');
    expect(res.metadata?.requestId, 'test-123');
  });

  test('raw query without decoder returns maps', () async {
    final qail = mockClient((request) async {
      return jsonResponse('{"rows":[{"id":1,"name":"Alice"}],"count":1}');
    });
    final res = await qail.query('get users');
    expect(res.rows[0], {'id': 1, 'name': 'Alice'});
  });

  // ── Select Builder ──────────────────────────────────────────────

  test('select builder', () async {
    final qail = mockClient((request) async {
      final url = request.url.toString();
      expect(url, contains('/api/users?'));
      expect(url, contains('select=id%2Cname%2Cemail'));
      expect(url, contains('limit=10'));
      expect(url, contains('sort=created_at%3Adesc'));
      expect(url, contains('active.eq=true'));
      expect(request.method, 'GET');
      return jsonResponse(
          '{"data":[{"id":1,"name":"Alice"}],"count":1,"limit":10,"offset":0}');
    });
    final users = await qail
        .from('users', User.fromJson)
        .select(['id', 'name', 'email'])
        .where('active', FilterOp.eq, 'true')
        .desc('created_at')
        .limit(10)
        .all();
    expect(users, hasLength(1));
    expect(users[0].name, 'Alice');
  });

  test('get by id', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/api/users/42');
      return jsonResponse('{"data":{"id":42,"name":"Bob"}}');
    });
    final user = await qail.from('users', User.fromJson).get(42);
    expect(user.id, 42);
    expect(user.name, 'Bob');
  });

  test('get by id encodes path separators', () async {
    final qail = mockClient((request) async {
      expect(request.url.toString(),
          'http://localhost:8080/api/users/tenant%2Fa%3Fb%23c');
      return jsonResponse('{"data":{"id":42,"name":"Bob"}}');
    });
    await qail.from('users', User.fromJson).get('tenant/a?b#c');
  });

  test('expand', () async {
    final qail = mockClient((request) async {
      expect(request.url.toString(), contains('expand=users%2Cproducts'));
      return jsonResponse('{"data":[],"count":0,"limit":50,"offset":0}');
    });
    await qail
        .from('orders', User.fromJson)
        .expand('users')
        .expand('products')
        .all();
  });

  test('filter encoding escapes reserved characters', () async {
    final qail = mockClient((request) async {
      final url = request.url.toString();
      expect(url, contains('name.eq=A%26B%3D1%20C'));
      expect(url, isNot(contains('name.eq=A&B=1 C')));
      return jsonResponse(
          '{"data":[{"id":1,"name":"Alice"}],"count":1,"limit":50,"offset":0}');
    });
    await qail
        .from('users', User.fromJson)
        .where('name', FilterOp.eq, 'A&B=1 C')
        .all();
  });

  test('in filter joins list values', () async {
    final qail = mockClient((request) async {
      expect(request.url.toString(), contains('id.in=1%2C2%2C3'));
      return jsonResponse('{"data":[],"count":0,"limit":50,"offset":0}');
    });
    await qail
        .from('users', User.fromJson)
        .where('id', FilterOp.inList, [1, 2, 3]).all();
  });

  test('first returns null on empty and restores limit', () async {
    var requestCount = 0;
    final qail = mockClient((request) async {
      requestCount++;
      if (requestCount == 1) {
        expect(request.url.queryParameters['limit'], '1');
        return jsonResponse('{"data":[],"count":0,"limit":1,"offset":0}');
      }
      expect(request.url.queryParameters.containsKey('limit'), isFalse);
      return jsonResponse('{"data":[],"count":0,"limit":50,"offset":0}');
    });
    final builder = qail.from('users', User.fromJson);
    expect(await builder.first(), isNull);
    await builder.all();
  });

  test('single throws NOT_FOUND on empty', () async {
    final qail = mockClient((request) async {
      return jsonResponse('{"data":[],"count":0,"limit":1,"offset":0}');
    });
    await expectLater(
      qail.from('users', User.fromJson).single(),
      throwsA(isA<QailError>()
          .having((e) => e.code, 'code', 'NOT_FOUND')
          .having((e) => e.table, 'table', 'users')),
    );
  });

  test('count prefers total', () async {
    final qail = mockClient((request) async {
      return jsonResponse('{"data":[{"id":1,"name":"A"}],"count":1,"total":99,'
          '"limit":50,"offset":0}');
    });
    expect(await qail.from('users', User.fromJson).count(), 99);
  });

  // ── Insert Builder ──────────────────────────────────────────────

  test('insert', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/api/users');
      expect(request.url.queryParameters['returning'], '*');
      expect(request.method, 'POST');
      expect(
          jsonDecode(request.body), {'name': 'New', 'email': 'new@test.com'});
      return jsonResponse('{"data":{"id":1,"name":"New"},"count":1}');
    });
    final res = await qail
        .into('users', User.fromJson)
        .values({'name': 'New', 'email': 'new@test.com'})
        .returning('*')
        .exec();
    expect(res.data.name, 'New');
    expect(res.count, 1);
  });

  test('upsert', () async {
    final qail = mockClient((request) async {
      expect(request.url.queryParameters['on_conflict'], 'id');
      expect(request.url.queryParameters['on_conflict_action'], 'update');
      return jsonResponse('{"data":{"id":1,"name":"Updated"},"count":1}');
    });
    await qail
        .into('users', User.fromJson)
        .values({'name': 'Updated'})
        .onConflict('id')
        .exec();
  });

  // ── Update Builder ──────────────────────────────────────────────

  test('update', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/api/users/1');
      expect(request.url.queryParameters['returning'], '*');
      expect(request.method, 'PATCH');
      return jsonResponse('{"data":{"id":1,"name":"Updated"},"count":1}');
    });
    final res = await qail
        .update('users', User.fromJson)
        .set({'name': 'Updated'})
        .returning('*')
        .exec(1);
    expect(res.data.name, 'Updated');
  });

  test('update encodes path separators', () async {
    final qail = mockClient((request) async {
      expect(request.url.toString(),
          'http://localhost:8080/api/users/tenant%2Fa%3Fb%23c');
      expect(request.method, 'PATCH');
      return jsonResponse('{"data":{"id":1,"name":"Updated"},"count":1}');
    });
    await qail
        .update('users', User.fromJson)
        .set({'name': 'Updated'}).exec('tenant/a?b#c');
  });

  // ── Delete Builder ──────────────────────────────────────────────

  test('delete', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/api/users/42');
      expect(request.method, 'DELETE');
      return jsonResponse('{"deleted":true}');
    });
    final res = await qail.delete('users').exec(42);
    expect(res.deleted, isTrue);
  });

  test('delete encodes path separators', () async {
    final qail = mockClient((request) async {
      expect(request.url.toString(),
          'http://localhost:8080/api/users/tenant%2Fa%3Fb%23c');
      expect(request.method, 'DELETE');
      return jsonResponse('{"deleted":true}');
    });
    await qail.delete('users').exec('tenant/a?b#c');
  });

  // ── Error Handling ──────────────────────────────────────────────

  test('error parsing', () async {
    final qail = mockClient((request) async {
      return jsonResponse(
          '{"code":"NOT_FOUND","message":"Resource not found",'
          '"hint":"Check the ID","table":"users","column":"id"}',
          status: 404);
    });
    await expectLater(
      qail.from('nonexistent', User.fromJson).all(),
      throwsA(isA<QailError>()
          .having((e) => e.status, 'status', 404)
          .having((e) => e.code, 'code', 'NOT_FOUND')
          .having((e) => e.hint, 'hint', 'Check the ID')
          .having((e) => e.table, 'table', 'users')
          .having((e) => e.column, 'column', 'id')),
    );
  });

  test('error fallback', () async {
    final qail = mockClient((request) async {
      return http.Response('Internal Server Error', 500,
          headers: {'content-type': 'text/plain'});
    });
    await expectLater(
      qail.health(),
      throwsA(isA<QailError>()
          .having((e) => e.status, 'status', 500)
          .having((e) => e.code, 'code', 'HTTP_500')
          .having(
              (e) => e.message, 'message', contains('Internal Server Error'))),
    );
  });

  // ── WebSocket URL ───────────────────────────────────────────────

  test('websocket url query mode', () {
    final qail = QailClient(QailConfig(
      url: 'https://localhost:8080',
      token: 'ws token',
      wsAuthMode: WebSocketAuthMode.query,
    ));
    final url = qail.buildWebSocketUri('ws token').toString();
    expect(url, startsWith('wss://localhost:8080/ws?'));
    expect(url, contains('access_token=ws%20token'));
  });

  test('websocket auth mode defaults to header on dart:io platforms', () {
    final qail = QailClient(QailConfig(
      url: 'http://localhost:8080',
      token: 'ws-token',
    ));
    // This test runs on the VM, so the platform default resolves to header:
    // no token in the URL (it goes in the handshake header instead).
    expect(qail.wsAuthMode, WebSocketAuthMode.header);
    expect(qail.buildWebSocketUri('ws-token').toString(),
        'ws://localhost:8080/ws');
  });

  test('websocket url header mode', () {
    final qail = QailClient(QailConfig(
      url: 'http://localhost:8080',
      token: 'ws-token',
      wsAuthMode: WebSocketAuthMode.header,
    ));
    expect(qail.buildWebSocketUri('ws-token').toString(),
        'ws://localhost:8080/ws');
  });

  // ── Batch & Fast ────────────────────────────────────────────────

  test('batch response', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/qail/batch');
      expect(jsonDecode(request.body), {
        'queries': ['get users']
      });
      return jsonResponse('''{
        "results": [{"index":0,"success":true,"rows":[{"id":1,"name":"Alice"}],"count":1}],
        "total": 1,
        "success": 1,
        "metadata": {"request_id": "batch-1"}
      }''');
    });
    final res = await qail.batch(['get users'], User.fromJson);
    expect(res.total, 1);
    expect(res.results, hasLength(1));
    expect(res.results[0].rows?[0].name, 'Alice');
    expect(res.metadata?.requestId, 'batch-1');
  });

  test('fast query', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/qail/fast');
      return jsonResponse('''{
        "rows": [[1, "Alice"]],
        "count": 1,
        "metadata": {"request_id": "fast-1"}
      }''');
    });
    final res = await qail.queryFast('get users');
    expect(res.rows, hasLength(1));
    expect(res.rows[0], [1, 'Alice']);
    expect(res.metadata?.requestId, 'fast-1');
  });

  // ── Transactions ────────────────────────────────────────────────

  test('transactions', () async {
    var step = 0;
    final qail = mockClient((request) async {
      switch (step++) {
        case 0:
          expect(request.url.path, '/txn/begin');
          return jsonResponse('{"txn_id":"txn-123"}');
        case 1:
          expect(request.url.path, '/txn/query');
          expect(request.headers['X-Transaction-Id'], 'txn-123');
          return jsonResponse('{"rows":[{"id":1,"name":"Alice"}],"count":1}');
        case 2:
          expect(request.url.path, '/txn/commit');
          expect(request.headers['X-Transaction-Id'], 'txn-123');
          return jsonResponse('{"status":"committed"}');
        default:
          fail('Unexpected step');
      }
    });

    final txn = await qail.beginTxn();
    expect(txn.txnId, 'txn-123');

    final res = await txn.query('get users', User.fromJson);
    expect(res.rows, hasLength(1));

    final end = await txn.commit();
    expect(end.status, 'committed');
  });

  // ── Raw HTTP verbs ──────────────────────────────────────────────

  test('raw post with body and decoder', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/booking/orders/draft');
      expect(request.method, 'POST');
      expect(request.headers['Content-Type'], 'application/json');
      expect(jsonDecode(request.body), {'sailing_id': 'abc'});
      return jsonResponse('{"data":{"id":7,"name":"Order"}}');
    });
    final user = await qail.post(
      '/booking/orders/draft',
      body: {'sailing_id': 'abc'},
      decoder: (json) => User.fromJson(
          (json as Map<String, dynamic>)['data'] as Map<String, dynamic>),
    );
    expect(user.id, 7);
  });

  test('raw get without decoder returns dynamic json', () async {
    final qail = mockClient((request) async {
      expect(request.url.path, '/search');
      return jsonResponse('{"results":[1,2,3]}');
    });
    final Map<String, dynamic> res = await qail.get('/search');
    expect(res['results'], [1, 2, 3]);
  });

  // ── Integer decode tolerance ────────────────────────────────────

  test('numeric fields tolerate float-encoded ints', () async {
    final qail = mockClient((request) async {
      return jsonResponse('{"data":[{"id":1.0,"name":"Alice"}],"count":1.0,'
          '"limit":50.0,"offset":0.0}');
    });
    final res = await qail.from('users', User.fromJson).exec();
    expect(res.count, 1);
    expect(res.data[0].id, 1);
  });
}
