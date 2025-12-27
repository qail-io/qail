# QAIL-Zig

**High-performance PostgreSQL driver for Zig** using Rust-powered wire protocol encoding.

## Performance

| Mode | Queries/Second | vs pg.zig |
|------|----------------|-----------|
| Simple Query (single) | 35,305 q/s | **2.1x faster** |
| Prepared (pipeline) | 315,708 q/s | 95% of native Rust |

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .qail = .{
        .url = "https://github.com/qail-io/qail/releases/download/v0.10.2/qail-zig-0.10.2.tar.gz",
        .hash = "...", // Run `zig fetch` to get the hash
    },
},
```

Then fetch the dependency:
```bash
zig fetch --save https://github.com/qail-io/qail/releases/download/v0.10.2/qail-zig-0.10.2.tar.gz
```

## Quick Start

```zig
const std = @import("std");
const qail = @import("qail");

pub fn main() !void {
    // Connect to PostgreSQL
    const socket = try std.net.tcpConnectToHost("127.0.0.1", 5432);
    defer socket.close();
    
    // Encode a query using QAIL
    const query = qail.encodeGet("users", "id,name", 10);
    defer query.deinit();
    
    // Send to PostgreSQL
    try socket.stream.writeAll(query.bytes());
    
    // Read response...
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Your Zig Application                                   │
├─────────────────────────────────────────────────────────┤
│  qail-zig (Native Zig I/O)                              │
│  └── std.net.tcpConnectToHost + stream.write            │
├─────────────────────────────────────────────────────────┤
│  qail-encoder (Rust FFI, ~60MB static lib)              │
│  └── PostgreSQL wire protocol encoding                  │
├─────────────────────────────────────────────────────────┤
│  PostgreSQL                                             │
└─────────────────────────────────────────────────────────┘
```

## License

MIT License - See [LICENSE](LICENSE)
