pub const packages = struct {
    pub const @"N-V-__8AAEGLAAB4JS8S1rWwdvXUTwnt7gRNthhJanWx4AvP" = struct {
        pub const build_root = "/Users/orion/.cache/zig/p/N-V-__8AAEGLAAB4JS8S1rWwdvXUTwnt7gRNthhJanWx4AvP";
        pub const build_zig = @import("N-V-__8AAEGLAAB4JS8S1rWwdvXUTwnt7gRNthhJanWx4AvP");
        pub const deps: []const struct { []const u8, []const u8 } = &.{};
    };
    pub const @"metrics-0.0.0-W7G4eP2_AQBKsaql3dhLJ-pkf-RdP-zV3vflJy4N34jC" = struct {
        pub const build_root = "/Users/orion/.cache/zig/p/metrics-0.0.0-W7G4eP2_AQBKsaql3dhLJ-pkf-RdP-zV3vflJy4N34jC";
        pub const build_zig = @import("metrics-0.0.0-W7G4eP2_AQBKsaql3dhLJ-pkf-RdP-zV3vflJy4N34jC");
        pub const deps: []const struct { []const u8, []const u8 } = &.{
        };
    };
    pub const @"pg-0.0.0-Wp_7gag6BgD_QAZrPhNNEGpnUZR_LEkKT40Ura3p-4yX" = struct {
        pub const build_root = "/Users/orion/.cache/zig/p/pg-0.0.0-Wp_7gag6BgD_QAZrPhNNEGpnUZR_LEkKT40Ura3p-4yX";
        pub const build_zig = @import("pg-0.0.0-Wp_7gag6BgD_QAZrPhNNEGpnUZR_LEkKT40Ura3p-4yX");
        pub const deps: []const struct { []const u8, []const u8 } = &.{
            .{ "buffer", "N-V-__8AAEGLAAB4JS8S1rWwdvXUTwnt7gRNthhJanWx4AvP" },
            .{ "metrics", "metrics-0.0.0-W7G4eP2_AQBKsaql3dhLJ-pkf-RdP-zV3vflJy4N34jC" },
        };
    };
};

pub const root_deps: []const struct { []const u8, []const u8 } = &.{
    .{ "pg_zig", "pg-0.0.0-Wp_7gag6BgD_QAZrPhNNEGpnUZR_LEkKT40Ura3p-4yX" },
};
