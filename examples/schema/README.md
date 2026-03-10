# Schema Examples

This directory contains copy-paste schema samples for both supported layouts:

1. Single-file schema (`schema.qail`)
2. Modular schema directory (`schema/*.qail` + optional `_order.qail`)

## 1) Single-file sample

Path:

`examples/schema/single/schema.qail`

Validate:

```bash
qail check examples/schema/single/schema.qail
```

## 2) Modular sample (strict manifest)

Path:

`examples/schema/modular/schema/`

Validate directly as a directory:

```bash
qail check examples/schema/modular/schema
```

Or validate through fallback resolution (`schema.qail` -> sibling `schema/` directory):

```bash
qail check examples/schema/modular/schema.qail
```

The modular sample includes `_order.qail` with strict manifest enabled:

- `-- qail: strict-manifest`
- every discovered module must be listed (directly or through listed directories)
