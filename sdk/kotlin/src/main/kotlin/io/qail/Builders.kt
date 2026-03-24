package io.qail

import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

@PublishedApi
internal fun encodeQueryComponent(value: String): String =
    URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")

@PublishedApi
internal fun buildEncodedQuery(params: List<Pair<String, String>>): String {
    if (params.isEmpty()) return ""
    val qs = params.joinToString("&") { (key, value) ->
        "${encodeQueryComponent(key)}=${encodeQueryComponent(value)}"
    }
    return "?$qs"
}

// ─── Select Builder ─────────────────────────────────────────────────

/**
 * Fluent builder for `GET /api/{table}` queries.
 *
 * ```kotlin
 * val users = qail.from<User>("users")
 *     .select("id", "name", "email")
 *     .where("active", FilterOp.EQ, "true")
 *     .desc("created_at")
 *     .limit(10)
 *     .all()
 * ```
 */
class SelectBuilder<T>(
    @PublishedApi internal val client: QailClient,
    @PublishedApi internal val table: String,
    @PublishedApi internal val type: Class<T>,
) {
    private data class FilterClause(
        val column: String,
        val op: FilterOp,
        val value: String,
    )

    private var columns: String? = null
    private val filters = mutableListOf<FilterClause>()
    private val sorts = mutableListOf<String>()
    private var _limit: Int? = null
    private var _offset: Int? = null
    private val expands = mutableListOf<String>()
    private var _distinct: String? = null
    private var _search: String? = null
    private var _searchColumns: String? = null
    private var _stream: Boolean = false

    /** Select specific columns. */
    fun select(vararg cols: String) = apply {
        columns = cols.joinToString(",")
    }

    /** Add a filter condition. */
    fun where(column: String, op: FilterOp, value: String) = apply {
        filters += FilterClause(column = column, op = op, value = value)
    }

    /** Shorthand: where(column, EQ, value). */
    fun eq(column: String, value: String) = where(column, FilterOp.EQ, value)

    /** Sort ascending. */
    fun asc(column: String) = apply { sorts += "$column:asc" }

    /** Sort descending. */
    fun desc(column: String) = apply { sorts += "$column:desc" }

    /** Limit results. */
    fun limit(n: Int) = apply { _limit = n }

    /** Offset results. */
    fun offset(n: Int) = apply { _offset = n }

    /** Expand a FK relation via LEFT JOIN. */
    fun expand(relation: String) = apply { expands += relation }

    /** Expand as nested JSON objects. */
    fun nested(relation: String) = apply { expands += "nested:$relation" }

    /** Distinct on columns. */
    fun distinct(vararg cols: String) = apply {
        _distinct = cols.joinToString(",")
    }

    /** Full-text search. */
    fun search(term: String, columns: List<String>? = null) = apply {
        _search = term
        columns?.let { _searchColumns = it.joinToString(",") }
    }

    /** Execute and return the full paginated response. */
    suspend inline fun <reified R : T> exec(): ListResponse<R> {
        val path = "/api/$table${buildQueryString()}"
        return client.request(HttpMethod.Get, path)
    }

    /** Execute and return just the data list. */
    suspend inline fun <reified R : T> all(): List<R> {
        val res: ListResponse<R> = exec()
        return res.data
    }

    /** Get a single row by primary key. */
    suspend inline fun <reified R : T> get(id: Any): R {
        val res: SingleResponse<R> = client.request(HttpMethod.Get, "/api/$table/$id")
        return res.data
    }

    /** Enable NDJSON streaming. */
    fun stream() = apply { _stream = true }

    /** Aggregate query (count, sum, avg, min, max). */
    suspend fun aggregate(
        func: AggregateFunc,
        column: String? = null,
        groupBy: List<String>? = null,
    ): AggregateResponse {
        val params = mutableListOf<Pair<String, String>>("func" to func.value)
        column?.let { params += "column" to it }
        groupBy?.let { params += "group_by" to it.joinToString(",") }
        filters.forEach { filter ->
            params += "${filter.column}.${filter.op.value}" to filter.value
        }

        return client.request(HttpMethod.Get, "/api/$table/aggregate${buildEncodedQuery(params)}")
    }

    // Internal

    @PublishedApi
    internal fun buildQueryString(): String {
        val params = mutableListOf<Pair<String, String>>()
        columns?.let { params += "select" to it }
        if (sorts.isNotEmpty()) params += "sort" to sorts.joinToString(",")
        _limit?.let { params += "limit" to it.toString() }
        _offset?.let { params += "offset" to it.toString() }
        if (expands.isNotEmpty()) params += "expand" to expands.joinToString(",")
        _distinct?.let { params += "distinct" to it }
        _search?.let { params += "search" to it }
        _searchColumns?.let { params += "search_columns" to it }
        if (_stream) params += "stream" to "true"

        filters.forEach { filter ->
            params += "${filter.column}.${filter.op.value}" to filter.value
        }

        return buildEncodedQuery(params)
    }
}

// ─── Insert Builder ─────────────────────────────────────────────────

/**
 * Fluent builder for `POST /api/{table}`.
 *
 * ```kotlin
 * val res = qail.into<User>("users")
 *     .values(mapOf("name" to "Alice", "email" to "alice@test.com"))
 *     .returning("*")
 *     .exec()
 * ```
 */
class InsertBuilder<T>(
    @PublishedApi internal val client: QailClient,
    @PublishedApi internal val table: String,
    @PublishedApi internal val type: Class<T>,
) {
    @PublishedApi internal var data: Any = emptyMap<String, Any?>()
    @PublishedApi internal var _returning: String? = null
    @PublishedApi internal var _onConflict: String? = null
    @PublishedApi internal var _onConflictAction: String? = null

    /** Set the data to insert (single row). */
    fun values(data: Map<String, Any?>) = apply { this.data = data }

    /** Set batch data to insert. */
    fun values(data: List<Map<String, Any?>>) = apply { this.data = data }

    /** Return specific columns after insert. */
    fun returning(columns: String) = apply { _returning = columns }

    /** Upsert: on conflict column. */
    fun onConflict(column: String, action: String = "update") = apply {
        _onConflict = column
        _onConflictAction = action
    }

    /** Execute the insert. */
    suspend inline fun <reified R : T> exec(): MutationResponse<R> {
        val params = mutableListOf<Pair<String, String>>()
        _returning?.let { params += "returning" to it }
        _onConflict?.let { params += "on_conflict" to it }
        _onConflictAction?.let { params += "on_conflict_action" to it }
        val qs = buildEncodedQuery(params)

        return client.request(HttpMethod.Post, "/api/$table$qs") {
            setBody(data)
            contentType(ContentType.Application.Json)
        }
    }
}

// ─── Update Builder ─────────────────────────────────────────────────

/**
 * Fluent builder for `PATCH /api/{table}/{id}`.
 *
 * ```kotlin
 * val res = qail.update<User>("users")
 *     .set(mapOf("name" to "Updated"))
 *     .returning("*")
 *     .exec(id = 1)
 * ```
 */
class UpdateBuilder<T>(
    @PublishedApi internal val client: QailClient,
    @PublishedApi internal val table: String,
    @PublishedApi internal val type: Class<T>,
) {
    @PublishedApi internal var data: Map<String, Any?> = emptyMap()
    @PublishedApi internal var _returning: String? = null

    /** Set the fields to update. */
    fun set(data: Map<String, Any?>) = apply { this.data = data }

    /** Return columns after update. */
    fun returning(columns: String) = apply { _returning = columns }

    /** Execute the update on a specific row. */
    suspend inline fun <reified R : T> exec(id: Any): MutationResponse<R> {
        val params = mutableListOf<Pair<String, String>>()
        _returning?.let { params += "returning" to it }
        val qs = buildEncodedQuery(params)

        return client.request(HttpMethod.Patch, "/api/$table/$id$qs") {
            setBody(data)
            contentType(ContentType.Application.Json)
        }
    }
}

// ─── Delete Builder ─────────────────────────────────────────────────

/**
 * Fluent builder for `DELETE /api/{table}/{id}`.
 *
 * ```kotlin
 * val res = qail.delete("users").exec(id = 42)
 * ```
 */
class DeleteBuilder(
    @PublishedApi internal val client: QailClient,
    @PublishedApi internal val table: String,
) {
    /** Delete a row by primary key. */
    suspend fun exec(id: Any): DeleteResponse =
        client.request(HttpMethod.Delete, "/api/$table/$id")
}
