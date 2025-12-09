# Android / Kotlin Integration Guide

Two ways to use Qail from Android / Kotlin: the **Native Kotlin SDK** or **OpenAPI codegen**.

---

## Option 1: Native Kotlin SDK

### Install

Add to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.qail:qail-sdk:0.1.0")
}
```

Or use the local module directly:

```kotlin
implementation(project(":sdk:kotlin"))
```

### Setup

```kotlin
import io.qail.*

val qail = QailClient(QailConfig(
    url = "https://engine.example.com",
    token = "your-jwt"
))

// With async token refresh (e.g. from DataStore)
val qail = QailClient(QailConfig(
    url = "https://engine.example.com",
    tokenProvider = { tokenRepository.getAccessToken() }
))
```

### Queries

```kotlin
@Serializable
data class User(val id: Int, val name: String, val email: String)

// SELECT with filters
val users = qail.from<User>("users")
    .select("id", "name", "email")
    .where("active", FilterOp.EQ, "true")
    .desc("created_at")
    .limit(10)
    .all<User>()

// GET by ID
val user = qail.from<User>("users").get<User>(42)

// INSERT
val res = qail.into<User>("users")
    .values(mapOf("name" to "Alice", "email" to "alice@test.com"))
    .returning("*")
    .exec<User>()

// UPDATE
val updated = qail.update<User>("users")
    .set(mapOf("name" to "Alice Updated"))
    .returning("*")
    .exec<User>(id = 42)

// DELETE
val deleted = qail.delete("users").exec(id = 42)

// Raw DSL
val result = qail.query<User>("get users fields id, name where active = true limit 5")

// FK expansion
val orders = qail.from<Order>("orders")
    .expand("users")
    .expand("products")
    .limit(20)
    .all<Order>()

// Upsert
val upserted = qail.into<User>("users")
    .values(mapOf("name" to "Upserted"))
    .onConflict("email", "update")
    .returning("*")
    .exec<User>()
```

### Error Handling

```kotlin
try {
    qail.into<User>("users")
        .values(mapOf("email" to "duplicate@test.com"))
        .exec<User>()
} catch (e: QailError) {
    println(e.code)      // "CONFLICT"
    println(e.message)   // "[CONFLICT] A record with this value already exists."
    println(e.hint)      // "Use a different value or update the existing record"
    println(e.table)     // "users"
    println(e.column)    // "email"
    println(e.status)    // 409
}
```

### Health Check

```kotlin
val health = qail.health()
println(health.status)   // "ok"
println(health.version)  // "0.20.1"
```

### Android ViewModel Example

```kotlin
class UsersViewModel(private val qail: QailClient) : ViewModel() {
    private val _users = MutableStateFlow<List<User>>(emptyList())
    val users: StateFlow<List<User>> = _users

    fun loadUsers() = viewModelScope.launch {
        try {
            _users.value = qail.from<User>("users")
                .where("active", FilterOp.EQ, "true")
                .limit(20)
                .all()
        } catch (e: QailError) {
            Log.e("Users", "Failed: ${e.code} — ${e.message}")
        }
    }
}
```

---

## Option 2: OpenAPI Codegen

Use [openapi-generator](https://openapi-generator.tech/) to auto-generate a Kotlin client.

### Step 1: Download the spec

```bash
curl -H "Authorization: Bearer <token>" \
     https://engine.example.com/api/_openapi \
     -o openapi.json
```

### Step 2: Generate

```bash
npx @openapitools/openapi-generator-cli generate \
    -i openapi.json \
    -g kotlin \
    -o generated-client \
    --additional-properties=library=jvm-ktor
```

### Step 3: Use

```kotlin
val api = DefaultApi("https://engine.example.com")
val users = api.listUsers(limit = 10, activeEq = true)
```

---

## When to use which?

| | Native SDK | OpenAPI Codegen |
|---|---|---|
| **Setup time** | 5 min | 15 min |
| **Fluent builders** | ✅ | ❌ (method-per-endpoint) |
| **Raw DSL** | ✅ | ❌ |
| **Auto-typed from schema** | ❌ (manual `@Serializable`) | ✅ |
| **Coroutines** | ✅ native | ✅ (with Ktor lib) |
| **Android-optimized** | ✅ | ⚠️ (may need tuning) |

**Recommendation:** Use the native SDK for most apps. Use OpenAPI codegen if you want auto-generated types without maintaining `@Serializable` models.
