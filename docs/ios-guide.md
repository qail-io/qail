# iOS Integration Guide

Two ways to use Qail from iOS/macOS: the **Native Swift SDK** or **OpenAPI codegen**.

---

## Option 1: Native Swift SDK

### Install (Swift Package Manager)

```
File → Add Package Dependencies → https://github.com/qail-io/qail → sdk/swift
```

Or in `Package.swift`:

```swift
.package(path: "../qail.rs/sdk/swift"),
// then add "Qail" to your target dependencies
```

### Setup

```swift
import Qail

let qail = QailClient(config: .init(
    url: "https://engine.example.com",
    token: .string("your-jwt")
))

// Or with async token refresh
let qail = QailClient(config: .init(
    url: "https://engine.example.com",
    token: .provider { await getTokenFromKeychain() }
))
```

### Queries

```swift
// Define a Codable model
struct User: Decodable {
    let id: Int
    let name: String
    let email: String
    let active: Bool
}

// SELECT with filters
let users: [User] = try await qail.from("users")
    .select(["id", "name", "email"])
    .where("active", .eq, "true")
    .desc("created_at")
    .limit(10)
    .all()

// GET by ID
let user: User = try await qail.from("users").get(id: 42)

// INSERT
let res: MutationResponse<User> = try await qail.into("users")
    .values(["name": "Alice", "email": "alice@example.com"])
    .returning("*")
    .exec()

// UPDATE
let updated: MutationResponse<User> = try await qail.update("users")
    .set(["name": "Alice Updated"])
    .returning("*")
    .exec(id: 42)

// DELETE
let deleted = try await qail.delete("users").exec(id: 42)

// Raw DSL
let result: QueryResponse<User> = try await qail.query(
    "get users fields id, name where active = true limit 5"
)

// FK expansion (LEFT JOIN)
let orders: [Order] = try await qail.from("orders")
    .expand("users")
    .expand("products")
    .limit(20)
    .all()
```

### Error Handling

```swift
do {
    let _ = try await qail.into("users")
        .values(["email": "duplicate@test.com"])
        .exec()
} catch let error as QailError {
    print(error.code)      // "CONFLICT"
    print(error.message)   // "A record with this value already exists."
    print(error.hint)      // "Use a different value or update the existing record"
    print(error.table)     // "users"
    print(error.column)    // "email"
    print(error.status)    // 409
}
```

### Health Check

```swift
let health = try await qail.health()
print(health.status)   // "ok"
print(health.version)  // "0.20.1"
```

---

## Option 2: OpenAPI Codegen

Apple's [swift-openapi-generator](https://github.com/apple/swift-openapi-generator) can auto-generate typed Swift clients from the gateway's built-in OpenAPI spec.

### Step 1: Download the spec

```bash
curl -H "Authorization: Bearer <token>" \
     https://engine.example.com/api/_openapi \
     -o openapi.json
```

### Step 2: Add dependencies

In your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/apple/swift-openapi-generator", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-openapi-runtime", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-openapi-urlsession", from: "1.0.0"),
],
```

### Step 3: Configure

Create `openapi-generator-config.yaml`:

```yaml
generate:
  - types
  - client
accessModifier: public
```

### Step 4: Add the plugin

```swift
targets: [
    .target(
        name: "MyApp",
        dependencies: [
            .product(name: "OpenAPIRuntime", package: "swift-openapi-runtime"),
            .product(name: "OpenAPIURLSession", package: "swift-openapi-urlsession"),
        ],
        plugins: [
            .plugin(name: "OpenAPIGenerator", package: "swift-openapi-generator"),
        ]
    ),
]
```

### Step 5: Use

```swift
import OpenAPIRuntime
import OpenAPIURLSession

let client = Client(
    serverURL: URL(string: "https://engine.example.com")!,
    transport: URLSessionTransport()
)

// Auto-generated typed methods
let response = try await client.listUsers(.init(
    query: .init(limit: 10, active_eq: true)
))
```

---

## When to use which?

| | Native SDK | OpenAPI Codegen |
|---|---|---|
| **Setup time** | 5 min | 30 min |
| **Fluent builders** | ✅ | ❌ (method-per-endpoint) |
| **Raw DSL** | ✅ | ❌ |
| **Auto-typed from schema** | ❌ (manual Codable) | ✅ |
| **WebSocket / Realtime** | Planned | ❌ |
| **Dependencies** | Zero | 3 Apple packages |

**Recommendation:** Use the native SDK for most apps. Use OpenAPI codegen if you want auto-generated types without maintaining Codable models.
