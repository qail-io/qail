package io.qail

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// ─── Filter Operators ───────────────────────────────────────────────

/** Filter operators matching PostgREST-style query params. */
enum class FilterOp(val value: String) {
    EQ("eq"),
    NE("ne"),
    @Deprecated("Use NE instead")
    NEQ("ne"),
    GT("gt"), GTE("gte"),
    LT("lt"), LTE("lte"),
    LIKE("like"), ILIKE("ilike"),
    IN("in"),
    NOT_IN("not_in"),
    IS_NULL("is_null"),
    IS_NOT_NULL("is_not_null"),
    CONTAINS("contains"),
    @Deprecated("Use IS_NULL / IS_NOT_NULL instead")
    IS("is");
}

// ─── Responses ──────────────────────────────────────────────────────

/** Paginated list response from `GET /api/{table}`. */
@Serializable
data class ListResponse<T>(
    val data: List<T>,
    val count: Int,
    val total: Int? = null,
    val limit: Int,
    val offset: Int,
)

/** Single-row response from `GET /api/{table}/{id}`. */
@Serializable
data class SingleResponse<T>(
    val data: T,
)

/** Mutation response from POST/PATCH operations. */
@Serializable
data class MutationResponse<T>(
    val data: T,
    @SerialName("rows_affected") val rowsAffected: Int,
)

/** Raw DSL query response from `POST /qail`. */
@Serializable
data class QueryResponse<T>(
    val data: List<T>,
    @SerialName("rows_affected") val rowsAffected: Int,
    val columns: List<String>,
)

/** Health check response. */
@Serializable
data class HealthResponse(
    val status: String,
    val version: String,
)

/** Batch result for multi-query execution. */
@Serializable
data class BatchResult<T>(
    val index: Int,
    val success: Boolean,
    val rows: List<T>? = null,
    val count: Int? = null,
    val error: String? = null,
)

/** Delete confirmation. */
@Serializable
data class DeleteResponse(
    val deleted: Boolean,
)

/** Aggregate query response. */
@Serializable
data class AggregateResponse(
    val data: List<Map<String, kotlinx.serialization.json.JsonElement>>,
    val count: Int,
)

/** Aggregate function type. */
enum class AggregateFunc(val value: String) {
    COUNT("count"),
    SUM("sum"),
    AVG("avg"),
    MIN("min"),
    MAX("max"),
}

/** Subscription handle for WebSocket LISTEN/NOTIFY. */
interface QailSubscription {
    fun unsubscribe()
    val active: Boolean
}

// ─── Error ──────────────────────────────────────────────────────────

/**
 * Structured error from the Qail Gateway.
 *
 * Matches the gateway `ApiError` JSON shape including enriched
 * `hint`, `table`, and `column` fields.
 */
@Serializable
data class QailErrorBody(
    val code: String,
    val message: String,
    val details: String? = null,
    @SerialName("request_id") val requestId: String? = null,
    val hint: String? = null,
    val table: String? = null,
    val column: String? = null,
)

/** Exception wrapping a structured gateway error. */
class QailError(
    val status: Int,
    val body: QailErrorBody,
) : Exception("[${body.code}] ${body.message}") {

    val code: String get() = body.code
    val hint: String? get() = body.hint
    val table: String? get() = body.table
    val column: String? get() = body.column
    val details: String? get() = body.details
    val requestId: String? get() = body.requestId

    override fun toString(): String {
        val parts = mutableListOf("[${body.code}] ${body.message}")
        body.hint?.let { parts += "Hint: $it" }
        body.table?.let { parts += "Table: $it" }
        body.column?.let { parts += "Column: $it" }
        body.details?.let { parts += "Details: $it" }
        return parts.joinToString(" | ")
    }
}
