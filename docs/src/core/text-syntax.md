# Text Syntax

For CLI, LSP, and WASM playground. Parses to AST internally.

## Keywords

| Keyword | Description | Example |
|---------|-------------|---------|
| `get` | SELECT query | `get users fields *` |
| `set` | UPDATE query | `set users values ...` |
| `del` | DELETE query | `del users where ...` |
| `add` | INSERT query | `add users values ...` |
| `fields` | Select columns | `fields id, email` |
| `where` | Filter conditions | `where active = true` |
| `order by` | Sort results | `order by name desc` |
| `limit` | Limit rows | `limit 10` |
| `offset` | Skip rows | `offset 20` |
| `left join` | Left outer join | `left join profiles` |

## Examples

### Simple Select
```
get users fields *
```
→ `SELECT * FROM users`

### Filtered Query
```
get users 
    fields id, email, name
    where active = true
    order by created_at desc
    limit 50
```

### Join Query
```
get users 
    inner join bookings
    fields id, email, bookings.total
    where created_at >= 2024-01-01
```

### Insert
```
add users values (email = "alice@example.com", name = "Alice")
```

### Update
```
set users values (status = "active") where id = 42
```
