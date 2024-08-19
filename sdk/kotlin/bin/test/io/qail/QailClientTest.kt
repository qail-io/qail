package io.qail

import io.ktor.client.*
import io.ktor.client.engine.mock.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.*

// ─── Test Model ─────────────────────────────────────────────────────

@Serializable
data class User(val id: Int, val name: String)

// ─── Helpers ────────────────────────────────────────────────────────

private fun mockClient(handler: MockRequestHandler): QailClient {
    val engine = MockEngine(handler)
    val httpClient = HttpClient(engine) {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true; isLenient = true })
        }
    }
    return QailClient(QailConfig(
        url = "http://localhost:8080",
        token = "test-jwt",
        httpClient = httpClient,
    ))
}

private fun MockRequestHandleScope.jsonResponse(
    json: String,
    status: HttpStatusCode = HttpStatusCode.OK,
) = respond(
    content = json,
    status = status,
    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
)

// ─── Tests ──────────────────────────────────────────────────────────

class QailClientTest {

    // MARK: Health

    @Test
    fun testHealth() = runTest {
        val qail = mockClient { request ->
            assertEquals("/health", request.url.encodedPath)
            assertEquals(HttpMethod.Get, request.method)
            jsonResponse("""{"status":"ok","version":"0.20.1"}""")
        }
        val res = qail.health()
        assertEquals("ok", res.status)
        assertEquals("0.20.1", res.version)
    }

    // MARK: Auth

    @Test
    fun testBearerToken() = runTest {
        val qail = mockClient { request ->
            assertEquals("Bearer test-jwt", request.headers[HttpHeaders.Authorization])
            jsonResponse("""{"status":"ok","version":"0.20.1"}""")
        }
        qail.health()
    }

    @Test
    fun testTokenProvider() = runTest {
        val engine = MockEngine { request ->
            assertEquals("Bearer dynamic-token", request.headers[HttpHeaders.Authorization])
            respond(
                content = """{"status":"ok","version":"0.20.1"}""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }
        val httpClient = HttpClient(engine) {
            install(ContentNegotiation) {
                json(Json { ignoreUnknownKeys = true })
            }
        }
        val qail = QailClient(QailConfig(
            url = "http://localhost:8080",
            tokenProvider = { "dynamic-token" },
            httpClient = httpClient,
        ))
        qail.health()
    }

    // MARK: Raw DSL

    @Test
    fun testRawQuery() = runTest {
        val qail = mockClient { request ->
            assertEquals("/qail", request.url.encodedPath)
            assertEquals(HttpMethod.Post, request.method)
            jsonResponse("""{ "data":[{"id":1,"name":"Alice"}],"rows_affected":1,"columns":["id","name"]}""")
        }
        val res = qail.query<User>("get users fields id, name limit 10")
        assertEquals(1, res.data.size)
        assertEquals("Alice", res.data[0].name)
        assertEquals(listOf("id", "name"), res.columns)
    }

    // MARK: Select Builder

    @Test
    fun testSelectBuilder() = runTest {
        val qail = mockClient { request ->
            val url = request.url.toString()
            assertTrue(url.contains("/api/users?"))
            assertTrue(url.contains("select=id,name,email"))
            assertTrue(url.contains("limit=10"))
            assertTrue(url.contains("sort=created_at:desc"))
            assertTrue(url.contains("active.eq=true"))
            assertEquals(HttpMethod.Get, request.method)
            jsonResponse("""{"data":[{"id":1,"name":"Alice"}],"count":1,"limit":10,"offset":0}""")
        }
        val users = qail.from<User>("users")
            .select("id", "name", "email")
            .where("active", FilterOp.EQ, "true")
            .desc("created_at")
            .limit(10)
            .all<User>()
        assertEquals(1, users.size)
        assertEquals("Alice", users[0].name)
    }

    @Test
    fun testGetById() = runTest {
        val qail = mockClient { request ->
            assertEquals("/api/users/42", request.url.encodedPath)
            jsonResponse("""{"data":{"id":42,"name":"Bob"}}""")
        }
        val user = qail.from<User>("users").get<User>(42)
        assertEquals(42, user.id)
        assertEquals("Bob", user.name)
    }

    @Test
    fun testExpand() = runTest {
        val qail = mockClient { request ->
            val url = request.url.toString()
            assertTrue(url.contains("expand=users,products"))
            jsonResponse("""{"data":[],"count":0,"limit":50,"offset":0}""")
        }
        qail.from<User>("orders")
            .expand("users")
            .expand("products")
            .all<User>()
    }

    // MARK: Insert Builder

    @Test
    fun testInsert() = runTest {
        val qail = mockClient { request ->
            val url = request.url.toString()
            assertTrue(url.contains("/api/users"))
            assertTrue(url.contains("returning=*"))
            assertEquals(HttpMethod.Post, request.method)
            jsonResponse("""{"data":{"id":1,"name":"New"},"rows_affected":1}""")
        }
        val res = qail.into<User>("users")
            .values(mapOf("name" to "New", "email" to "new@test.com"))
            .returning("*")
            .exec<User>()
        assertEquals("New", res.data.name)
        assertEquals(1, res.rowsAffected)
    }

    @Test
    fun testUpsert() = runTest {
        val qail = mockClient { request ->
            val url = request.url.toString()
            assertTrue(url.contains("on_conflict=id"))
            assertTrue(url.contains("on_conflict_action=update"))
            jsonResponse("""{"data":{"id":1,"name":"Updated"},"rows_affected":1}""")
        }
        qail.into<User>("users")
            .values(mapOf("name" to "Updated"))
            .onConflict("id", "update")
            .exec<User>()
    }

    // MARK: Update Builder

    @Test
    fun testUpdate() = runTest {
        val qail = mockClient { request ->
            assertEquals("/api/users/1", request.url.encodedPath)
            assertTrue(request.url.toString().contains("returning=*"))
            assertEquals(HttpMethod.Patch, request.method)
            jsonResponse("""{"data":{"id":1,"name":"Updated"},"rows_affected":1}""")
        }
        val res = qail.update<User>("users")
            .set(mapOf("name" to "Updated"))
            .returning("*")
            .exec<User>(id = 1)
        assertEquals("Updated", res.data.name)
    }

    // MARK: Delete Builder

    @Test
    fun testDelete() = runTest {
        val qail = mockClient { request ->
            assertEquals("/api/users/42", request.url.encodedPath)
            assertEquals(HttpMethod.Delete, request.method)
            jsonResponse("""{"deleted":true}""")
        }
        val res = qail.delete("users").exec(id = 42)
        assertTrue(res.deleted)
    }

    // MARK: Error Handling

    @Test
    fun testErrorParsing() = runTest {
        val qail = mockClient { _ ->
            jsonResponse(
                """{"code":"NOT_FOUND","message":"Resource not found","hint":"Check the ID","table":"users","column":"id"}""",
                HttpStatusCode.NotFound,
            )
        }
        try {
            qail.from<User>("nonexistent").all<User>()
            fail("Should have thrown")
        } catch (e: QailError) {
            assertEquals(404, e.status)
            assertEquals("NOT_FOUND", e.code)
            assertEquals("Check the ID", e.hint)
            assertEquals("users", e.table)
            assertEquals("id", e.column)
        }
    }

    @Test
    fun testErrorFallback() = runTest {
        val qail = mockClient { _ ->
            respond(
                content = "Internal Server Error",
                status = HttpStatusCode.InternalServerError,
                headers = headersOf(HttpHeaders.ContentType, ContentType.Text.Plain.toString()),
            )
        }
        try {
            qail.health()
            fail("Should have thrown")
        } catch (e: QailError) {
            assertEquals(500, e.status)
            assertEquals("HTTP_500", e.code)
            assertTrue(e.message!!.contains("Internal Server Error"))
        }
    }
}
