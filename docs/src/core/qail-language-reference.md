# QAIL Language Reference

The authoritative reference for QAIL v2 text syntax. Read top to bottom.

Every QAIL snippet on this page is **verified**: it was run through the real parser and
transpiler, and the SQL shown is the exact SQL that was produced. Nothing here is invented.

Examples are fenced as ```qail and are checked on every build by
`cargo run -p qail-core --example knowledge_export` — if one stops parsing, the build fails.
Every QAIL snippet on this page carries a ```qail fence. The exceptions are §6, which documents
inputs the parser *rejects*, and the grammar skeletons in §1 and §4.14, which are shapes rather
than queries; both are fenced as ```text precisely so they are not checked.
Source of truth: `docs/generated/verified-examples.json`, `docs/generated/invalid-examples.json`,
`docs/generated/grammar-productions.json`, and `core/src/parser/`.

---

## 1. Shape of a query

```text
[with <ctes>] <action> [distinct [on (...)]] <table> [joins] [values ...] [fields ...]
              [from (...) | values ...] [where ...] [having ...] [conflict ...]
              [order by ...] [limit N] [offset N]
```

The action and the table are separated by whitespace. Everything after the table is optional.

### Actions

| Action | Aliases | Produces |
|--------|---------|----------|
| `get` | — | `SELECT` |
| `add` | `insert` | `INSERT` |
| `set` | — | `UPDATE` |
| `del` | `delete` | `DELETE` |
| `merge` | — | `MERGE INTO` |
| `export` | — | `SELECT` (export pipeline) |
| `cnt` | `count` | aggregate count |
| `make` | `create` | `CREATE TABLE` |

`set` has **no** alias. `update` is *not* accepted as an action — `update users values ...`
is a parse error. The complete alias table is `core/src/parser/grammar/base.rs`; `update` appears
in the grammar only as a MERGE arm keyword and inside a `conflict (...) update` clause.

Standalone commands that do **not** take a table: `begin`, `commit`, `rollback`,
`session set/show/reset`, `call`, `do`, and `index ... on ...` (§4.14).

### Clause order is fixed

The parser is a recursive-descent pipeline that tries clauses in a **fixed order**
(`core/src/parser/grammar/mod.rs`, `parse_root`). Writing clauses out of order does not
reorder them — it fails, and the leftover text is reported as trailing content.

The order is:

1. `join` / `inner join` / `left join` / `right join`
2. `values` — **`set` only** (the assignment list)
3. `fields`
4. `from (...)` or `values ...` — **`add` only**
5. `where`
6. `having` — non-aggregate conditions only, see §6.4
7. `conflict` — **`add` only**
8. `order by`
9. `limit`
10. `offset`

Note that **joins come before `fields`**, unlike SQL:

```qail
get users join posts on users.id = posts.user_id fields id, title
```
```sql
SELECT id, title FROM users LEFT JOIN posts ON users.id = posts.user_id
```

A full chain in canonical order:

```qail
get users fields id, name, email where active = true order by created_at desc limit 10
```
```sql
SELECT id, name, email FROM users WHERE active = true ORDER BY created_at DESC LIMIT 10
```

---

## 2. Preprocessing (happens before the grammar sees your query)

Three transformations run before parsing. **None of them are visible in the grammar**, so you
cannot infer them from the clause rules. All three are in `core/src/parser/`.

### 2.1 Input size limit

`MAX_INPUT_LENGTH = 64 * 1024` bytes (`core/src/parser/mod.rs`). Input is trimmed, then
rejected before recursive descent if it exceeds 64 KB:

```text
Input too large: N bytes (max 65536 bytes)
```

The limit exists because the recursive-descent parser has no depth limit; 64 KB is not enough
to encode enough nesting to blow the stack.

### 2.2 Comment stripping

`strip_sql_comments` removes `-- line comments` and `/* block comments */` **before** parsing.
It is quote-aware: comment markers inside single quotes, double quotes, and triple-quoted
strings are preserved as data.

```qail
get docs -- outside line comment
fields id /* outside block comment */ where active = true
```
```sql
SELECT id FROM docs WHERE active = true
```

Comment markers inside a string literal are **not** stripped:

```qail
get docs fields id where body = "alpha -- beta /* gamma */"
```
```sql
SELECT id FROM docs WHERE body = 'alpha -- beta /* gamma */'
```

The same holds inside a triple-quoted string:

```qail
get docs fields id where body = '''alpha -- beta /* gamma */'''
```
```sql
SELECT id FROM docs WHERE body = 'alpha -- beta /* gamma */'
```

Dollar-quoted bodies are also protected, which is what makes `do` blocks safe:

```qail
do $$ BEGIN RAISE NOTICE '-- not a comment /* still body */'; END; $$ language plpgsql
```
```sql
DO $$  BEGIN RAISE NOTICE '-- not a comment /* still body */'; END;  $$ LANGUAGE plpgsql
```

### 2.3 `table[filter]` desugaring

`desugar_bracket_filter` rewrites a bracket immediately after the table name into a `where`
clause: `action table[cond] rest` becomes `action table rest where cond`. If the query already
has a `where`, the bracket filter is appended with `AND`.

So `get users[active = true]` is textually rewritten to `get users where active = true`
before parsing — there is no bracket production in the grammar at all.

The rewrite is guarded. It does **not** fire when the text before the bracket already contains
` where `, ` fields `, ` having `, ` order `, ` limit `, ` offset `, or ` join `. This is what
keeps array and JSON literals in clause position intact:

```qail
get users fields id where tags && '["a","b"]'
```
```sql
SELECT id FROM users WHERE tags && '["a","b"]'
```

---

## 3. Keyword reference

Grouped by construct, extracted from the grammar productions.

| Construct | Keywords |
|-----------|----------|
| Actions | `get`, `export`, `set`, `del`, `delete`, `add`, `insert`, `merge`, `make`, `create`, `cnt`, `count`, `distinct` |
| Projection | `fields`, `*`, `as` |
| Filtering | `where`, `and`, `or`, `exists`, `not exists` |
| Aggregate filtering | `having`, `filter` |
| Joins | `join`, `inner`, `left`, `right`, `on` |
| Ordering / paging | `order`, `by`, `asc`, `desc`, `limit`, `offset` |
| Insert | `values`, `from`, `conflict`, `update`, `nothing` |
| Update | `values` |
| Merge | `using`, `on`, `when`, `matched`, `not`, `by`, `source`, `target`, `then`, `update`, `insert`, `delete`, `do`, `nothing` |
| CTEs | `with`, `recursive`, `as` |
| Transactions | `begin`, `commit`, `rollback` |
| Session | `session`, `set`, `show`, `reset` |
| Procedural | `call`, `do` |
| DDL | `index`, `unique`, `on`, `primary`, `key` (in query position `unique` *trails* the column list — §4.14) |
| Window functions | `over`, `partition`, `by`, `order`, `rows`, `range`, `between`, `unbounded`, `preceding`, `current`, `row`, `following` |
| Case expressions | `case`, `when`, `then`, `else`, `end` |
| Special functions | `extract`, `substring`, `from`, `for` |
| Literals | `true`, `false`, `null` |
| Interval units | `s`, `m`, `h`, `d`, `w`, `mo`, `y` |

Keywords are case-insensitive (`tag_no_case` throughout). Lowercase is conventional.

---

## 4. Constructs

### 4.1 `get` — SELECT

Bare `get` selects everything. `fields *` is equivalent to omitting `fields`.

```qail
get users
```
```sql
SELECT * FROM users
```

```qail
get users fields *
```
```sql
SELECT * FROM users
```

```qail
get users fields id, email
```
```sql
SELECT id, email FROM users
```

### 4.2 `fields` — projection

Comma-separated expressions, or `*`. Expressions are allowed, not just column names.

```qail
get users fields COALESCE(name, 'fallback'), CASE WHEN active = true THEN 1 ELSE 0 END
```
```sql
SELECT COALESCE(name, 'fallback'), CASE WHEN active = true THEN 1 ELSE 0 END FROM users
```

### 4.3 `where` — filtering

`where col op value [and|or col op value ...]`.

```qail
get users fields * where active = true
```
```sql
SELECT * FROM users WHERE active = true
```

```qail
get users fields * where active = true and role = "admin"
```
```sql
SELECT * FROM users WHERE active = true AND role = 'admin'
```

`or` groups are parenthesised in the output:

```qail
get users fields * where active = true or role = "admin"
```
```sql
SELECT * FROM users WHERE (active = true OR role = 'admin')
```

**Mixing `and` and `or` in one flat chain is not supported.** See §6.

### 4.4 `order by`, `limit`, `offset`

Sort direction defaults to `ASC` when omitted.

```qail
get users fields * order by name
```
```sql
SELECT * FROM users ORDER BY name ASC
```

```qail
get users fields * order by created_at desc
```
```sql
SELECT * FROM users ORDER BY created_at DESC
```

```qail
get users fields * order by id asc
```
```sql
SELECT * FROM users ORDER BY id ASC
```

```qail
get users fields * limit 10
```
```sql
SELECT * FROM users LIMIT 10
```

```qail
get users fields * offset 20
```
```sql
SELECT * FROM users OFFSET 20
```

```qail
get users fields * limit 10 offset 20
```
```sql
SELECT * FROM users LIMIT 10 OFFSET 20
```

### 4.5 Joins

`[left|right|inner] join <table> on <col> = <col>`. **A bare `join` means `LEFT JOIN`.**

```qail
get users join posts on users.id = posts.user_id fields id, title
```
```sql
SELECT id, title FROM users LEFT JOIN posts ON users.id = posts.user_id
```

```qail
get users inner join posts on users.id = posts.user_id fields id, title
```
```sql
SELECT id, title FROM users INNER JOIN posts ON users.id = posts.user_id
```

```qail
get orders right join customers on orders.customer_id = customers.id
```
```sql
SELECT * FROM orders RIGHT JOIN customers ON orders.customer_id = customers.id
```

### 4.6 `set` — UPDATE

`set <table> values col = val, col2 = val2 where ...`. The `values` clause carries the
assignments and must come before `where`.

```qail
set users values verified = true where id = $1
```
```sql
UPDATE users SET verified = true WHERE id = $1
```

```qail
set users values name = "John", active = true where id = $1
```
```sql
UPDATE users SET name = 'John', active = true WHERE id = $1
```

### 4.7 `del` — DELETE

```qail
del sessions where expired_at < $1
```
```sql
DELETE FROM sessions WHERE expired_at < $1
```

```qail
del sessions where user_id = $1 and expired = true
```
```sql
DELETE FROM sessions WHERE user_id = $1 AND expired = true
```

```qail
del sessions where user_id = $1 or expired = true
```
```sql
DELETE FROM sessions WHERE (user_id = $1 OR expired = true)
```

### 4.8 `add` — INSERT

`add <table> [fields ...] values <val>, <val> [conflict (...) update ... | conflict (...) nothing]`,
or `add <table> from (get ...)` for INSERT…SELECT.

The `values` clause for `add` is a **positional list of values**, not `col = val` pairs — that
form belongs to `set`. Because of this, `add` requires an explicit `fields` list to name the
target columns. Omitting it produces invalid SQL (see §6).

### 4.9 `merge` — MERGE INTO

`merge <target> [as alias] using <source> [as alias] on <cond>` followed by one or more
`when [not] matched [by source|target] [and <cond>] then <action>` arms. Actions are
`update set ...`, `insert (...) values (...)`, `delete`, and `do nothing`.

```qail
merge users as u using staging_users as s on u.id = s.id when matched and u.name != s.name then update set name = s.name, email = s.email when not matched then insert (id, name, email) values (s.id, s.name, s.email)
```
```sql
MERGE INTO users AS u USING staging_users AS s ON u.id = s.id WHEN MATCHED AND u.name != s.name THEN UPDATE SET name = s.name, email = s.email WHEN NOT MATCHED BY TARGET THEN INSERT (id, name, email) VALUES (s.id, s.name, s.email)
```

A bare `when not matched` defaults to `BY TARGET`. `by source` must be stated explicitly:

```qail
merge users using staging_users on users.id = staging_users.id when not matched by source then delete
```
```sql
MERGE INTO users USING staging_users ON users.id = staging_users.id WHEN NOT MATCHED BY SOURCE THEN DELETE
```

### 4.10 `export`

Parses like `get` and accepts the same clauses; it drives the export pipeline rather than a
plain read.

```qail
export users
```
```sql
SELECT * FROM users
```

```qail
export users fields id, email where active = true limit 100
```
```sql
SELECT id, email FROM users WHERE active = true LIMIT 100
```

### 4.11 Transactions

Three standalone keywords, no table.

```qail
begin
```
```sql
BEGIN TRANSACTION;
```

```qail
commit
```
```sql
COMMIT;
```

```qail
rollback
```
```sql
ROLLBACK;
```

### 4.12 Session commands

`session set <key> = <value>`, `session show <key>`, `session reset <key>`.

```qail
session set statement_timeout = '5000'
```
```sql
SET statement_timeout = '5000'
```

Dotted keys work, which is how RLS tenant context is set:

```qail
session set app.current_tenant_id = 'tenant-1'
```
```sql
SET app.current_tenant_id = 'tenant-1'
```

```qail
session show statement_timeout
```
```sql
SHOW statement_timeout
```

```qail
session reset statement_timeout
```
```sql
RESET statement_timeout
```

### 4.13 Procedural commands

```qail
call refresh_materialized_views()
```
```sql
CALL refresh_materialized_views()
```

```qail
do $$ BEGIN RAISE NOTICE 'ok'; END; $$ language plpgsql
```
```sql
DO $$  BEGIN RAISE NOTICE 'ok'; END;  $$ LANGUAGE plpgsql
```

### 4.14 DDL: `index` and `make`

No verified example covers these two, so this section gives the **grammar shape** rather than a
worked example — the shapes below are transcribed from the doc comments on `parse_create_index`
and `parse_create_table` in `core/src/parser/grammar/ddl.rs`. Run any concrete query through
`qail_parse_query` (or `cargo run -p qail-core --example test_query_parse`) before relying on it.

`index` takes a **bare, unparenthesised** column list, and `unique` **trails** the columns:

```text
index <name> on <table> <col>[, <col>...] [unique]
```

The SQL-shaped reading — `index idx on users (email)` — is a parse error. Note that this is the
opposite of the schema-file form (§8), where `unique` *prefixes* `index` and the column list *is*
parenthesised. The two are different grammars for the same concept; do not carry one into the other.

`make` is colon-delimited per column, and shares nothing with either the SQL paren form or the
brace schema dialect of §8:

```text
make <table> <col>:<type>[:<constraint>...][, <col>:<type>[:<constraint>...]...]
```

Constraint shorthands are `pk` / `primarykey`, `unique` / `uniq`, `nullable` / `null`,
`default=<value>` / `def=<value>`, and `check=<expr>`. Columns are `NOT NULL` unless marked
`nullable`. Every SQL-shaped guess fails: `make users (id uuid)`, `make table users (...)`, and
`create table users (...)` are all parse errors.

`make` is the ad-hoc query form for creating a table. It is unrelated to the brace schema dialect
in §8, which is what you should actually be writing for anything persistent.

---

## 5. Expressions

### 5.1 Operator precedence

From `parse_expression`, lowest to highest:

1. `||` (concatenation)
2. `+` `-`
3. `*` `/` `%`
4. atoms — functions, `CASE`, literals, identifiers, `*`, parenthesised groups

### 5.2 Comparison operators

Recognised by `parse_operator`:

`=` `!=` `<>` `<` `<=` `>` `>=` · `like` `not like` `ilike` `not ilike` `similar to` ·
`in` `not in` · `between` `not between` · `is null` `is not null` ·
`~` `~*` `regex` · `@>` `<@` `&&` `?` `?|` `?&` `#>` `#>>` `@@` ·
`json_exists` `json_query` `json_value`

`~` is fuzzy match and expands to a wrapped `ILIKE`:

```qail
get users fields id where name ~ "john"
```
```sql
SELECT id FROM users WHERE name ILIKE '%john%'
```

`in` and `not in` against a named parameter become array predicates:

```qail
get users fields id where id in :ids
```
```sql
SELECT id FROM users WHERE id = ANY(:ids)
```

```qail
get users fields id where id not in :blocked_ids
```
```sql
SELECT id FROM users WHERE id != ALL(:blocked_ids)
```

### 5.3 Parameters

`$1`-style positional parameters and `:name`-style named parameters both pass through:

```qail
get users fields id where email = $1
```
```sql
SELECT id FROM users WHERE email = $1
```

### 5.4 String literals

Double-quoted and single-quoted strings both become SQL single-quoted strings. Doubling the
delimiter escapes it.

```qail
get users fields id where quote = "say ""hi"""
```
```sql
SELECT id FROM users WHERE quote = 'say "hi"'
```

```qail
get users fields id where name = 'O''Reilly'
```
```sql
SELECT id FROM users WHERE name = 'O''Reilly'
```

Triple-quoted strings (`'''...'''` or `"""..."""`) hold multi-line content verbatim — see §2.2.

### 5.5 Interval shorthand

A bare number plus a unit suffix becomes an SQL `INTERVAL`. Units: `s` seconds, `m` minutes,
`h` hours, `d` days, `w` weeks, `mo` months, `y` years.

```qail
get subscriptions fields id where age = 6mo
```
```sql
SELECT id FROM subscriptions WHERE age = INTERVAL '6 months'
```

### 5.6 JSON access

Dotted paths on a column become `->>` text extraction:

```qail
get users fields * where metadata.theme = "dark"
```
```sql
SELECT * FROM users WHERE metadata->>'theme' = 'dark'
```

Explicit `->`, `->>`, `#>`, `#>>` and `::` casts are also accepted directly.

A brace/bracket literal in value position is captured as JSON and cast to `jsonb`:

```qail
get docs fields id where metadata @> {"tags":["a",{"b":true}],"n":1}
```
```sql
SELECT id FROM docs WHERE metadata @> '{"tags":["a",{"b":true}],"n":1}'::jsonb
```

### 5.7 Functions and aggregates

Function calls parse generically as `name(arg, ...)`, with `*` allowed as an argument, an
optional `as` alias, an optional `filter (where ...)` clause on aggregates, and an optional
`over (partition by ... order by ... rows|range between ...)` window spec.

`CASE WHEN cond THEN expr [ELSE expr] END` is a first-class expression — see §4.2 for a
verified example.

Keyword-argument functions `EXTRACT(field FROM expr)` and `SUBSTRING(expr FROM pos [FOR len])`
have dedicated productions.

---

## 6. What the language rejects

These are real failures with the real error text, from `docs/generated/invalid-examples.json`.

### 6.1 Flat `and` + `or` in the same condition chain

The condition chain cannot mix `and` and `or` at the same level. The parser stops at the
mixing point and the rest of the query is reported as trailing content.

```text
get users fields * where active = true and role = "admin" or age > 18
```
```text
Parse error at position 0: Unexpected trailing content: 'where active = true and role = "admin" or age > 18'
```

Same failure on an update:

```text
set users values verified = true where id = $1 and active = true or role = "admin"
```
```text
Parse error at position 0: Unexpected trailing content: 'where id = $1 and active = true or role = "admin"'
```

Note what this error looks like: the offending text is echoed back, and the reported position
is `0`. Use a single connective per chain, or build the query with the AST builder API.

### 6.2 `add` without a `fields` list

This is the dangerous class: it **parses successfully** but transpiles to invalid SQL, with the
error embedded as a SQL comment in the column list. Always give `add` an explicit `fields`.

```text
add users values 1 conflict (id) update name = excluded.name
```
```text
parses, but transpiles to invalid SQL:
INSERT INTO users (/* ERROR: Invalid insert column */) VALUES (1) ON CONFLICT (id) DO UPDATE SET name = excluded.name RETURNING *
```

```text
add users values 1, "Ana" conflict (id) update name = '''O'Reilly'''
```
```text
parses, but transpiles to invalid SQL:
INSERT INTO users (/* ERROR: Invalid insert column */) VALUES (1, 'Ana') ON CONFLICT (id) DO UPDATE SET name = 'O''Reilly' RETURNING *
```

### 6.3 Oversized input

Anything over 64 KB is rejected before parsing (§2.1).

### 6.4 `having` with an aggregate

Same dangerous class as §6.2: it **parses successfully** but transpiles to invalid SQL, with the
error embedded as a SQL comment where the left operand belongs. `having` currently accepts only a
non-aggregate left-hand side.

```text
get orders fields status having count(*) > 1
```
```text
parses, but transpiles to invalid SQL:
SELECT status FROM orders HAVING /* ERROR: Invalid condition expression */ > 1
```

`having sum(total) > 100` fails the same way. A plain column — `having total > 100` — works.

### 6.5 `group by`

QAIL has **no** `group by` clause. There is no group-by production in the grammar, and a trailing
`group by ...` is reported as trailing content:

```text
get orders fields status group by status
```
```text
Parse error at position 0: Unexpected trailing content: 'group by status'
```

Grouping is reachable only through the builder API's `group_by_mode`, never through text syntax.

---

## 7. SQL → QAIL translation

### Statements

| SQL | QAIL |
|-----|------|
| `SELECT` | `get` |
| `INSERT` | `add` (alias `insert`) |
| `UPDATE` | `set` (no alias — `update` is not an action) |
| `DELETE` | `del` (alias `delete`) |
| `MERGE INTO` | `merge` |
| `CREATE TABLE` | `make` (alias `create`) — colon-delimited columns, see §4.14 |
| `CREATE INDEX` | `index <name> on <table> <cols>` — no parens, `unique` trails, see §4.14 |
| `BEGIN` / `COMMIT` / `ROLLBACK` | `begin` / `commit` / `rollback` |
| `SET` / `SHOW` / `RESET` | `session set` / `session show` / `session reset` |
| `CALL` | `call` |
| `DO $$ ... $$` | `do $$ ... $$` |

### Clauses

| SQL | QAIL | Note |
|-----|------|------|
| `SELECT a, b` | `fields a, b` | comes **after** joins |
| `FROM t` | the table name after the action | |
| `WHERE` | `where` | one connective per chain |
| `LEFT JOIN` | `join` or `left join` | bare `join` is LEFT |
| `INNER JOIN` | `inner join` | |
| `RIGHT JOIN` | `right join` | |
| `ORDER BY x DESC` | `order by x desc` | `asc` is the default |
| `LIMIT` / `OFFSET` | `limit` / `offset` | |
| `HAVING` | `having` | non-aggregate conditions only — see §6.4 | |
| `SET a = 1` (UPDATE) | `values a = 1` | before `where` |
| `VALUES (1, 'x')` (INSERT) | `values 1, "x"` | positional; pair with `fields` |
| `INSERT ... SELECT` | `from (get ...)` | |
| `ON CONFLICT (c) DO UPDATE` | `conflict (c) update ...` | |
| `ON CONFLICT (c) DO NOTHING` | `conflict (c) nothing` | |
| `WITH` / `WITH RECURSIVE` | `with` / `with recursive` | precedes the action |
| `DISTINCT` / `DISTINCT ON` | `get distinct` / `get distinct on (...)` | |
| `LIKE '%x%'` | `~ "x"` | fuzzy → `ILIKE '%x%'` |
| `= ANY(:p)` | `in :p` | |
| `!= ALL(:p)` | `not in :p` | |
| `col->>'k'` | `col.k` | |
| `INTERVAL '6 months'` | `6mo` | |

---

## 8. Schema files

Schema files use the **brace dialect**:

```text
table <name> {
  <column> <type> [options...]
  ...
}
```

One declaration per line. **No commas.** Column options are space-separated. Top-level
`extension "name"`, `index`, and `unique index` declarations sit outside table blocks.

The canonical sample is `examples/schema/single/schema.qail`:

```text
extension "pgcrypto"

table tenants {
  id uuid primary_key default gen_random_uuid()
  slug text unique not_null
  name text not_null
  active bool not_null default true
  created_at timestamptz not_null default now()
}

table users {
  id uuid primary_key default gen_random_uuid()
  tenant_id uuid not_null references tenants(id) on_delete cascade
  email text unique not_null
  full_name text not_null
  role text not_null default 'member'
  created_at timestamptz not_null default now()
  enable_rls
  force_rls
}

table bookings {
  id uuid primary_key default gen_random_uuid()
  tenant_id uuid not_null references tenants(id) on_delete cascade
  user_id uuid not_null references users(id) on_delete cascade
  status text not_null default 'pending'
  total_cents bigint not_null
  travel_date date not_null
  created_at timestamptz not_null default now()
}

unique index idx_users_tenant_email on users (tenant_id,email)
index idx_bookings_tenant_date on bookings (tenant_id,travel_date)
```

### 8.1 Two brace parsers coexist — this matters

There is no single schema parser. Two independent brace parsers read the same file shape for
different purposes, and **they do not accept exactly the same options**:

| Parser | File | Drives |
|--------|------|--------|
| `parse_qail` | `core/src/migrate/parser.rs` | migrations |
| `Schema::parse` | `core/src/build/schema.rs` | typed codegen |

Both accept: `primary_key`, `not_null`, `nullable`, `unique`, `default <expr>`,
`references <table>(<col>)`, `on_delete` / `on_update` actions, `generated_identity`,
`generated_by_default_identity`, plus the `enable_rls` and `force_rls` table-block lines and
the top-level `extension` / `index` / `unique index` forms.

Two options are **codegen-only**:

- `ref:table.column` — foreign-key shorthand, e.g. `user_id uuid ref:users.id`
- `protected` — column access-policy marker, e.g. `password_hash text protected`

`Schema::parse` accepts both. `parse_qail` rejects them with
`unknown column option '<opt>' for column '<name>'`.

**Consequence:** a schema block can be valid for one brace parser and rejected by the other.
`ref:` and `protected` are *not* invalid syntax — they are valid for typed codegen and
unavailable in the migration path. If a block must be consumed by both, write the long
`references <table>(<col>)` form instead of `ref:`.

### 8.2 `default(true)` is invalid everywhere

The correct form is a space, not parentheses:

```text
active bool not_null default true
```

`default(true)` is a single token that matches no option in either parser and fails in both.
`default` requires a following value token; the value may be a literal (`true`, `'member'`) or
a function call (`gen_random_uuid()`, `now()`).

### 8.3 The paren dialect is legacy — do not write it

A third parser, `core/src/parser/schema.rs`, reads an older **parenthesised** dialect:

```text
table users ( id uuid primary_key, ... )
```

It survives only for `cli/src/migrations/mod.rs` and `cli/src/backup.rs`. It is not the schema
language. Do not author new schemas in it and do not convert brace schemas to it.

### 8.4 Authoring workflow

Do not hand-edit a generated `schema.qail`. Author changes as deltas under `db/deltas` and
regenerate. See the migrations documentation for the full workflow.
