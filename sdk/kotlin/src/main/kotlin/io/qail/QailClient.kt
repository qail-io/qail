package io.qail

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

// ─── Configuration ──────────────────────────────────────────────────

enum class WebSocketAuthMode {
    NONE,
    HEADER,
    QUERY,
}

/**
 * Configuration for the Qail client.
 *
 * @param url Gateway base URL (e.g. `https://engine.example.com`)
 * @param token Static JWT token
 * @param tokenProvider Async token provider for refreshable auth
 * @param headers Additional default headers
 * @param timeoutMs Request timeout in milliseconds (default: 30000)
 * @param httpClient Custom Ktor HttpClient (optional)
 */
data class QailConfig(
    val url: String,
    val token: String? = null,
    val tokenProvider: (suspend () -> String)? = null,
    val headers: Map<String, String> = emptyMap(),
    val timeoutMs: Long = 30_000,
    val wsAuthMode: WebSocketAuthMode = WebSocketAuthMode.HEADER,
    val wsTokenQueryParam: String = "access_token",
    val httpClient: HttpClient? = null,
)

// ─── Client ─────────────────────────────────────────────────────────

/**
 * Qail Gateway client for Kotlin / Android.
 *
 * Provides fluent query builders, raw DSL execution, and utility
 * endpoints — mirroring the TypeScript and Swift SDK surface area.
 *
 * ```kotlin
 * val qail = QailClient(QailConfig(
 *     url = "https://engine.example.com",
 *     token = "my-jwt"
 * ))
 *
 * // Fluent query
 * val users = qail.from<User>("users")
 *     .where("active", FilterOp.EQ, "true")
 *     .limit(10)
 *     .all()
 *
 * // Raw DSL
 * val res = qail.query<User>("get users fields id, name limit 10")
 * ```
 */
class QailClient(@PublishedApi internal val config: QailConfig) {

    @PublishedApi
    internal val baseUrl: String = config.url.trimEnd('/')

    @PublishedApi
    internal val json: Json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }

    @PublishedApi
    internal val client: HttpClient = config.httpClient ?: HttpClient {
        install(ContentNegotiation) {
            json(this@QailClient.json)
        }
        install(HttpTimeout) {
            requestTimeoutMillis = config.timeoutMs
        }
    }

    // ── Query builder entry points ──────────────────────────────────

    /** Start a SELECT query on a table. */
    inline fun <reified T> from(table: String): SelectBuilder<T> =
        SelectBuilder(this, table, T::class.java)

    /** Start an INSERT on a table. */
    inline fun <reified T> into(table: String): InsertBuilder<T> =
        InsertBuilder(this, table, T::class.java)

    /** Start an UPDATE on a table. */
    inline fun <reified T> update(table: String): UpdateBuilder<T> =
        UpdateBuilder(this, table, T::class.java)

    /** Start a DELETE on a table. */
    fun delete(table: String): DeleteBuilder =
        DeleteBuilder(this, table)

    // ── Raw QAIL text protocol ──────────────────────────────────────

    /** Execute raw Qail DSL text. */
    suspend inline fun <reified T> query(dsl: String): QueryResponse<T> =
        request(HttpMethod.Post, "/qail") {
            setBody(dsl)
            contentType(ContentType.Text.Plain)
        }

    /** Execute a batch of Qail DSL queries. */
    suspend inline fun <reified T> batch(queries: List<String>): List<BatchResult<T>> =
        request(HttpMethod.Post, "/qail/batch") {
            setBody(queries)
            contentType(ContentType.Application.Json)
        }

    // ── Utilities ───────────────────────────────────────────────────

    /** Health check. */
    suspend fun health(): HealthResponse = request(HttpMethod.Get, "/health")

    /** Get OpenAPI spec. */
    suspend fun openapi(): JsonObject = request(HttpMethod.Get, "/api/_openapi")

    /** Get schema introspection. */
    suspend fun schema(): JsonObject = request(HttpMethod.Get, "/api/_schema")

    /**
     * Generate TypeScript interfaces from the gateway schema.
     *
     * Returns a string containing valid TypeScript interface declarations.
     */
    suspend fun generateTypes(): String = requestText(HttpMethod.Get, "/api/_schema/typescript")

    // ── Realtime (WebSocket) ────────────────────────────────────────

    /**
     * Subscribe to a Postgres LISTEN channel via WebSocket.
     *
     * ```kotlin
     * val sub = qail.subscribe("orders") { payload ->
     *     println("Got: $payload")
     * }
     * // Later...
     * sub.unsubscribe()
     * ```
     */
    fun subscribe(
        channel: String,
        scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
        onMessage: (String) -> Unit,
    ): QailSubscription {
        val sub = WebSocketSubscriptionImpl()

        sub.job = scope.launch {
            val token = config.token ?: config.tokenProvider?.invoke()
            val wsUrl = buildWebSocketUrl(token)

            try {
                client.webSocket({
                    url(wsUrl)
                    config.headers.forEach { (k, v) -> header(k, v) }
                    if (config.wsAuthMode == WebSocketAuthMode.HEADER && token != null) {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                }) {
                    // Send listen command
                    send(Frame.Text("""{ "action": "listen", "channel": "$channel"}"""))

                    for (frame in incoming) {
                        if (!sub.active) break
                        if (frame is Frame.Text) {
                            try {
                                val msg = json.decodeFromString<JsonObject>(frame.readText())
                                val ch = msg["channel"]?.jsonPrimitive?.contentOrNull
                                val payloadElement = msg["payload"]
                                val payload = payloadElement?.jsonPrimitive?.contentOrNull ?: payloadElement?.toString()
                                if (ch == channel && payload != null) {
                                    onMessage(payload)
                                }
                            } catch (_: Exception) { }
                        }
                    }
                }
            } finally {
                sub.markClosed()
            }
        }

        return sub
    }

    // ── Internal HTTP ───────────────────────────────────────────────

    /** Internal typed request. */
    @PublishedApi
    internal suspend inline fun <reified T> request(
        method: HttpMethod,
        path: String,
        noinline block: HttpRequestBuilder.() -> Unit = {},
    ): T {
        val response = client.request("$baseUrl$path") {
            this.method = method

            // Default headers
            config.headers.forEach { (k, v) -> header(k, v) }

            // Auth token
            val token = config.token
                ?: config.tokenProvider?.invoke()
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }

            block()
        }

        if (!response.status.isSuccess()) {
            val text = response.bodyAsText()
            val body = try {
                json.decodeFromString<QailErrorBody>(text)
            } catch (_: Exception) {
                QailErrorBody(
                    code = "HTTP_${response.status.value}",
                    message = text,
                )
            }
            throw QailError(status = response.status.value, body = body)
        }

        return response.body()
    }

    /** Internal text request. */
    @PublishedApi
    internal suspend fun requestText(
        method: HttpMethod,
        path: String,
    ): String {
        val response = client.request("$baseUrl$path") {
            this.method = method
            config.headers.forEach { (k, v) -> header(k, v) }
            val token = config.token ?: config.tokenProvider?.invoke()
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
        }
        if (!response.status.isSuccess()) {
            val text = response.bodyAsText()
            val body = try {
                json.decodeFromString<QailErrorBody>(text)
            } catch (_: Exception) {
                QailErrorBody(code = "HTTP_${response.status.value}", message = text)
            }
            throw QailError(status = response.status.value, body = body)
        }
        return response.bodyAsText()
    }

    @PublishedApi
    internal fun buildWebSocketUrl(token: String?): String {
        val wsBase = baseUrl
            .replace(Regex("^https"), "wss")
            .replace(Regex("^http"), "ws")
        val basePath = "$wsBase/ws"
        if (token == null || config.wsAuthMode != WebSocketAuthMode.QUERY) {
            return basePath
        }
        val sep = if (basePath.contains("?")) "&" else "?"
        val key = encodeQueryComponent(config.wsTokenQueryParam)
        val value = encodeQueryComponent(token)
        return "$basePath$sep$key=$value"
    }

    private fun encodeQueryComponent(value: String): String =
        URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20")
}
