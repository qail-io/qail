# Text Syntax

> **This page is a quick lookup table.** The authoritative specification is the
> [QAIL Language Reference](./qail-language-reference.md) — clause order, preprocessing
> (comment stripping, `table[filter]` desugaring, the 64 KB input limit), every construct with
> verified SQL output, what the parser rejects, a SQL → QAIL translation table, and the schema
> dialects. Read that page when you need to get syntax right.

For CLI and LSP usage. Parses to AST internally.

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

Each of these is verified — the SQL shown is what the transpiler actually produces.

### Simple select
```
get users fields *
```
→ `SELECT * FROM users`

### Filtered query
```
get users fields id, name, email where active = true order by created_at desc limit 10
```
→ `SELECT id, name, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10`

### Join query

Joins come **before** `fields`, and a bare `join` means `LEFT JOIN`:
```
get users join posts on users.id = posts.user_id fields id, title
```
→ `SELECT id, title FROM users LEFT JOIN posts ON users.id = posts.user_id`

### Update

Assignments go in a `values` clause, before `where`:
```
set users values name = "John", active = true where id = $1
```
→ `UPDATE users SET name = 'John', active = true WHERE id = $1`

### Delete
```
del sessions where expired_at < $1
```
→ `DELETE FROM sessions WHERE expired_at < $1`

For `add` (INSERT), `merge`, CTEs, transactions, session commands, expressions, and schema
files, see the [QAIL Language Reference](./qail-language-reference.md).
