pub use qail_core::parse;
pub use qail_core::prelude;
pub use qail_core::{ast, error, parser, transpiler};

// CLI modules
pub mod backup;
pub mod exec;
pub mod introspection;
pub mod lint;
pub mod migrations;
#[cfg(feature = "repl")]
pub mod repl;
pub mod schema;
pub mod shadow;
pub mod sql_gen;
pub mod types;
pub mod util;
pub mod resolve;
pub mod colors;
pub mod time;
#[cfg(feature = "vector")]
pub mod vector;
#[cfg(feature = "vector")]
pub mod snapshot;
pub mod init;
pub mod sync;
#[cfg(feature = "vector")]
pub mod worker;

