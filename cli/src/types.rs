//! Type-safe schema code generation CLI wrapper.
//!
//! Thin wrapper around `qail_core::codegen` for CLI usage.

use anyhow::Result;

/// Generate typed Rust code from a schema.qail file
pub fn generate_types(schema_path: &str, output_path: Option<&str>) -> Result<()> {
    match output_path {
        Some(out) => {
            qail_core::codegen::generate_to_file(schema_path, out)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            println!("✓ Generated types to {}", out);
        }
        None => {
            let code = qail_core::codegen::generate_from_file(schema_path)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            println!("{}", code);
        }
    }
    Ok(())
}
