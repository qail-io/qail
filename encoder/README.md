# qail-encoder

Lightweight QAIL protocol encoder crate.

[![Crates.io](https://img.shields.io/crates/v/qail-encoder.svg)](https://crates.io/crates/qail-encoder)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```toml
[dependencies]
qail-encoder = "1.3.3"
```

## Scope

- QAIL text transpilation and validation
- PostgreSQL wire-message encoding for simple and extended query protocol
- Optional response decoding via the `response` feature
- Minimal dependency surface by default

## ABI Boundary

`qail-encoder` is intentionally a wire/query encoding ABI only. It does not
open sockets, negotiate TLS, authenticate users, manage SSO, or control
Kerberos/GSS state. Non-Rust callers bring their own transport and identity
stack, then pass protocol/query data through this crate.

Enterprise database authentication belongs in the Rust PostgreSQL driver layer
(`qail-pg`) through `ConnectOptions` and token-provider callbacks, not in this
encoder ABI.

## C Header

The public C ABI is declared in [`include/qail_encoder.h`](include/qail_encoder.h).
Keep the header, Rust exports, and ABI tests in sync when adding or removing
symbols.

Ownership rules:

- Strings returned by `qail_transpile` must be freed with `qail_free`.
- Byte buffers returned by `qail_encode_*` must be freed with `qail_free_bytes`
  using the exact pointer and length returned by the call.
- Response handles returned by `qail_decode_response` must be freed with
  `qail_response_free`.
- `qail_response_get_string` and `qail_response_error_message` return borrowed
  pointers valid only until the response handle is freed.

## License

Apache-2.0
