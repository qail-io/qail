import 'client.dart';
import 'models.dart';

String encodeQueryComponent(String value) => Uri.encodeComponent(value);

String encodePathSegment(Object value) => Uri.encodeComponent(value.toString());

String buildEncodedQuery(List<MapEntry<String, String>> params) {
  if (params.isEmpty) return '';
  final qs = params
      .map((p) =>
          '${encodeQueryComponent(p.key)}=${encodeQueryComponent(p.value)}')
      .join('&');
  return '?$qs';
}

class _FilterClause {
  const _FilterClause(this.column, this.op, this.value);

  final String column;
  final FilterOp op;
  final String value;
}

String _filterValue(Object value) =>
    value is List ? value.join(',') : value.toString();

// ─── Select Builder ─────────────────────────────────────────────────

/// Fluent builder for `GET /api/{table}` queries.
///
/// ```dart
/// final users = await qail.from('users', User.fromJson)
///     .select(['id', 'name', 'email'])
///     .where('active', FilterOp.eq, 'true')
///     .desc('created_at')
///     .limit(10)
///     .all();
/// ```
class SelectBuilder<T> {
  SelectBuilder(this._client, this._table, this._decodeRow);

  final QailClient _client;
  final String _table;
  final RowDecoder<T> _decodeRow;

  String? _columns;
  final List<_FilterClause> _filters = [];
  final List<String> _sorts = [];
  int? _limit;
  int? _offset;
  final List<String> _expands = [];
  String? _distinct;
  String? _search;
  String? _searchColumns;
  bool _stream = false;

  /// Select specific columns.
  SelectBuilder<T> select(List<String> columns) {
    _columns = columns.join(',');
    return this;
  }

  /// Add a filter condition. List values are joined with commas
  /// (for [FilterOp.inList] / [FilterOp.notIn]).
  SelectBuilder<T> where(String column, FilterOp op, Object value) {
    _filters.add(_FilterClause(column, op, _filterValue(value)));
    return this;
  }

  /// Shorthand: `where(column, FilterOp.eq, value)`.
  SelectBuilder<T> eq(String column, Object value) =>
      where(column, FilterOp.eq, value);

  /// Sort ascending.
  SelectBuilder<T> asc(String column) {
    _sorts.add('$column:asc');
    return this;
  }

  /// Sort descending.
  SelectBuilder<T> desc(String column) {
    _sorts.add('$column:desc');
    return this;
  }

  /// Limit results.
  SelectBuilder<T> limit(int n) {
    _limit = n;
    return this;
  }

  /// Offset results.
  SelectBuilder<T> offset(int n) {
    _offset = n;
    return this;
  }

  /// Expand a FK relation via LEFT JOIN.
  SelectBuilder<T> expand(String relation) {
    _expands.add(relation);
    return this;
  }

  /// Expand as nested JSON objects.
  SelectBuilder<T> nested(String relation) {
    _expands.add('nested:$relation');
    return this;
  }

  /// Distinct on columns.
  SelectBuilder<T> distinct(List<String> columns) {
    _distinct = columns.join(',');
    return this;
  }

  /// Full-text search.
  SelectBuilder<T> search(String term, [List<String>? columns]) {
    _search = term;
    if (columns != null) _searchColumns = columns.join(',');
    return this;
  }

  /// Enable NDJSON streaming.
  SelectBuilder<T> stream() {
    _stream = true;
    return this;
  }

  /// Execute and return the full paginated response.
  Future<ListResponse<T>> exec() {
    final path = '/api/${encodePathSegment(_table)}${_buildQueryString()}';
    return _client.requestJson(
      'GET',
      path,
      decode: (json) =>
          ListResponse.fromJson(json as Map<String, dynamic>, _decodeRow),
    );
  }

  /// Execute and return just the data list.
  Future<List<T>> all() async => (await exec()).data;

  /// Get the first matching row, or null.
  Future<T?> first() async {
    final saved = _limit;
    _limit = 1;
    try {
      final res = await exec();
      return res.data.isEmpty ? null : res.data.first;
    } finally {
      _limit = saved;
    }
  }

  /// Get exactly one row (throws [QailError] if none found).
  Future<T> single() async {
    final row = await first();
    if (row == null) {
      throw QailError(
        status: 404,
        body: QailErrorBody(
          code: 'NOT_FOUND',
          message: 'No rows found in $_table',
          table: _table,
        ),
      );
    }
    return row;
  }

  /// Get the total count of matching rows.
  Future<int> count() async {
    final res = await exec();
    return res.total ?? res.count;
  }

  /// Get a single row by primary key.
  Future<T> get(Object id) async {
    final res = await _client.requestJson(
      'GET',
      '/api/${encodePathSegment(_table)}/${encodePathSegment(id)}',
      decode: (json) =>
          SingleResponse.fromJson(json as Map<String, dynamic>, _decodeRow),
    );
    return res.data;
  }

  /// Aggregate query (count, sum, avg, min, max).
  Future<AggregateResponse> aggregate(
    AggregateFunc func, {
    String? column,
    List<String>? groupBy,
  }) {
    final params = [MapEntry('func', func.value)];
    if (column != null) params.add(MapEntry('column', column));
    if (groupBy != null) params.add(MapEntry('group_by', groupBy.join(',')));
    for (final filter in _filters) {
      params.add(MapEntry('${filter.column}.${filter.op.value}', filter.value));
    }

    return _client.requestJson(
      'GET',
      '/api/${encodePathSegment(_table)}/aggregate${buildEncodedQuery(params)}',
      decode: (json) =>
          AggregateResponse.fromJson(json as Map<String, dynamic>),
    );
  }

  String _buildQueryString() {
    final params = <MapEntry<String, String>>[];
    if (_columns != null) params.add(MapEntry('select', _columns!));
    if (_sorts.isNotEmpty) params.add(MapEntry('sort', _sorts.join(',')));
    if (_limit != null) params.add(MapEntry('limit', _limit.toString()));
    if (_offset != null) params.add(MapEntry('offset', _offset.toString()));
    if (_expands.isNotEmpty) params.add(MapEntry('expand', _expands.join(',')));
    if (_distinct != null) params.add(MapEntry('distinct', _distinct!));
    if (_search != null) params.add(MapEntry('search', _search!));
    if (_searchColumns != null) {
      params.add(MapEntry('search_columns', _searchColumns!));
    }
    if (_stream) params.add(const MapEntry('stream', 'true'));

    for (final filter in _filters) {
      params.add(MapEntry('${filter.column}.${filter.op.value}', filter.value));
    }

    return buildEncodedQuery(params);
  }
}

// ─── Insert Builder ─────────────────────────────────────────────────

/// Fluent builder for `POST /api/{table}`.
///
/// ```dart
/// final res = await qail.into('users', User.fromJson)
///     .values({'name': 'Alice', 'email': 'alice@test.com'})
///     .returning('*')
///     .exec();
/// ```
class InsertBuilder<T> {
  InsertBuilder(this._client, this._table, this._decodeRow);

  final QailClient _client;
  final String _table;
  final RowDecoder<T> _decodeRow;

  Object _data = const <String, Object?>{};
  String? _returning;
  String? _onConflict;
  String? _onConflictAction;

  /// Set the data to insert — a single row map, or a list of maps for batch.
  InsertBuilder<T> values(Object data) {
    _data = data;
    return this;
  }

  /// Return specific columns after insert.
  InsertBuilder<T> returning(String columns) {
    _returning = columns;
    return this;
  }

  /// Upsert: on conflict column.
  InsertBuilder<T> onConflict(String column, [String action = 'update']) {
    _onConflict = column;
    _onConflictAction = action;
    return this;
  }

  /// Execute the insert.
  Future<MutationResponse<T>> exec() {
    final params = <MapEntry<String, String>>[];
    if (_returning != null) params.add(MapEntry('returning', _returning!));
    if (_onConflict != null) params.add(MapEntry('on_conflict', _onConflict!));
    if (_onConflictAction != null) {
      params.add(MapEntry('on_conflict_action', _onConflictAction!));
    }
    final qs = buildEncodedQuery(params);

    return _client.requestJson(
      'POST',
      '/api/${encodePathSegment(_table)}$qs',
      body: _data,
      decode: (json) =>
          MutationResponse.fromJson(json as Map<String, dynamic>, _decodeRow),
    );
  }
}

// ─── Update Builder ─────────────────────────────────────────────────

/// Fluent builder for `PATCH /api/{table}/{id}`.
///
/// ```dart
/// final res = await qail.update('users', User.fromJson)
///     .set({'name': 'Updated'})
///     .returning('*')
///     .exec(1);
/// ```
class UpdateBuilder<T> {
  UpdateBuilder(this._client, this._table, this._decodeRow);

  final QailClient _client;
  final String _table;
  final RowDecoder<T> _decodeRow;

  Map<String, Object?> _data = const {};
  String? _returning;

  /// Set the fields to update.
  UpdateBuilder<T> set(Map<String, Object?> data) {
    _data = data;
    return this;
  }

  /// Return columns after update.
  UpdateBuilder<T> returning(String columns) {
    _returning = columns;
    return this;
  }

  /// Execute the update on a specific row.
  Future<MutationResponse<T>> exec(Object id) {
    final params = <MapEntry<String, String>>[];
    if (_returning != null) params.add(MapEntry('returning', _returning!));
    final qs = buildEncodedQuery(params);

    return _client.requestJson(
      'PATCH',
      '/api/${encodePathSegment(_table)}/${encodePathSegment(id)}$qs',
      body: _data,
      decode: (json) =>
          MutationResponse.fromJson(json as Map<String, dynamic>, _decodeRow),
    );
  }
}

// ─── Delete Builder ─────────────────────────────────────────────────

/// Fluent builder for `DELETE /api/{table}/{id}`.
///
/// ```dart
/// final res = await qail.delete('users').exec(42);
/// ```
class DeleteBuilder {
  DeleteBuilder(this._client, this._table);

  final QailClient _client;
  final String _table;

  /// Delete a row by primary key.
  Future<DeleteResponse> exec(Object id) {
    return _client.requestJson(
      'DELETE',
      '/api/${encodePathSegment(_table)}/${encodePathSegment(id)}',
      decode: (json) => DeleteResponse.fromJson(json as Map<String, dynamic>),
    );
  }
}
