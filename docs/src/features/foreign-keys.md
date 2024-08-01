# Foreign Key Validation

QAIL provides compile-time validation for foreign key references, ensuring your schema is consistent before migrations run.

## Defining Foreign Keys

Use the builder API to define foreign key constraints:

```rust
use qail_core::migrate::{Column, ColumnType, ForeignKey, FkAction};

let user_id = Column::new("user_id", ColumnType::Uuid)
    .references("users", "id")
    .on_delete(FkAction::Cascade)
    .on_update(FkAction::NoAction);
```

## FK Actions

| Action | SQL | Description |
|--------|-----|-------------|
| `FkAction::NoAction` | `NO ACTION` | Reject if referenced row exists (default) |
| `FkAction::Cascade` | `CASCADE` | Delete/update child rows |
| `FkAction::SetNull` | `SET NULL` | Set FK column to NULL |
| `FkAction::SetDefault` | `SET DEFAULT` | Set FK column to default value |
| `FkAction::Restrict` | `RESTRICT` | Same as NO ACTION but checked immediately |

## Schema Validation

Call `validate()` to check all FK references exist:

```rust
let mut schema = Schema::new();

schema.add_table(Table::new("users")
    .column(Column::new("id", ColumnType::Uuid).primary_key()));

schema.add_table(Table::new("posts")
    .column(Column::new("id", ColumnType::Uuid).primary_key())
    .column(Column::new("user_id", ColumnType::Uuid)
        .references("users", "id")));

// Validate all FK references
match schema.validate() {
    Ok(()) => println!("Schema is valid"),
    Err(errors) => {
        for e in errors {
            eprintln!("Error: {}", e);
        }
    }
}
```

## Error Messages

If a FK references a non-existent table or column:

```
FK error: posts.user_id references non-existent table 'users'
FK error: posts.author_id references non-existent column 'users.author_id'
```

## Best Practices

1. **Always validate before migrating**
   ```rust
   let schema = parse_qail(&content)?;
   schema.validate()?;
   ```

2. **Use Cascade carefully** - it can delete more data than expected

3. **Prefer SetNull for optional relationships**
   ```rust
   .references("categories", "id")
   .on_delete(FkAction::SetNull)
   ```
