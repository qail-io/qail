//! Migration types and classification
//!
//! Shared types for migration operations.

use qail_core::prelude::{Action, Expr, Qail};

/// Classification of migration operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationClass {
    /// Safe to apply and rollback (e.g., ADD COLUMN nullable, CREATE INDEX)
    Reversible,
    /// Can apply, rollback may lose data (e.g., DROP COLUMN, DROP TABLE)
    DataLosing,
    /// Cannot rollback without manual intervention (e.g., TEXT → INT)
    Irreversible,
}

impl std::fmt::Display for MigrationClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationClass::Reversible => write!(f, "reversible"),
            MigrationClass::DataLosing => write!(f, "data-losing"),
            MigrationClass::Irreversible => write!(f, "irreversible"),
        }
    }
}

/// Classify a migration operation.
pub fn classify_migration(cmd: &Qail) -> MigrationClass {
    match cmd.action {
        // CREATE operations are generally reversible
        Action::Make => MigrationClass::Reversible,
        Action::Index => MigrationClass::Reversible,
        
        // DROP operations lose data
        Action::Drop => MigrationClass::DataLosing,
        Action::AlterDrop => MigrationClass::DataLosing,
        Action::DropIndex => MigrationClass::Reversible, // Indexes can be recreated
        
        // Type changes depend on direction
        Action::AlterType => {
            if let Some(Expr::Def { data_type, .. }) = cmd.columns.first() {
                let target = data_type.as_str();
                if is_narrowing_type(target) {
                    MigrationClass::Irreversible
                } else {
                    MigrationClass::Reversible
                }
            } else {
                MigrationClass::Reversible
            }
        }
        
        // ADD COLUMN is generally reversible
        Action::Alter => MigrationClass::Reversible,
        
        // RENAME is reversible
        Action::Mod => MigrationClass::Reversible,
        
        // Default to reversible for unknown actions
        _ => MigrationClass::Reversible,
    }
}

/// Check if a target type is a "narrowing" type that requires explicit USING clause.
/// These types cannot be automatically cast from TEXT.
pub fn is_narrowing_type(target: &str) -> bool {
    matches!(
        target.to_uppercase().as_str(),
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" 
        | "BOOLEAN" | "BOOL" 
        | "UUID" 
        | "NUMERIC" | "DECIMAL"
        | "REAL" | "FLOAT" | "DOUBLE PRECISION"
        | "DATE" | "TIME" | "TIMESTAMP" | "TIMESTAMPTZ"
    )
}

/// Check if a type cast is safe (widening) or unsafe (narrowing).
pub fn is_safe_cast(from: &str, to: &str) -> bool {
    let from_upper = from.to_uppercase();
    let to_upper = to.to_uppercase();
    
    // Same type is always safe
    if from_upper == to_upper {
        return true;
    }
    
    // TEXT can accept anything (widening)
    if to_upper == "TEXT" || to_upper == "VARCHAR" {
        return true;
    }
    
    // INT -> BIGINT is safe (widening)
    if from_upper == "INT" || from_upper == "INTEGER" {
        if to_upper == "BIGINT" || to_upper == "TEXT" {
            return true;
        }
    }
    
    // SMALLINT -> INT/BIGINT is safe
    if from_upper == "SMALLINT" {
        if to_upper == "INT" || to_upper == "INTEGER" || to_upper == "BIGINT" {
            return true;
        }
    }
    
    // If target is a narrowing type and source is TEXT/VARCHAR, it's unsafe
    // (TEXT -> INT requires USING clause)
    if is_narrowing_type(&to_upper) && (from_upper == "TEXT" || from_upper == "VARCHAR") {
        return false;
    }
    
    // Other cases - generally safer
    !is_narrowing_type(&to_upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_narrowing_type() {
        assert!(is_narrowing_type("INT"));
        assert!(is_narrowing_type("integer"));
        assert!(is_narrowing_type("BOOLEAN"));
        assert!(is_narrowing_type("UUID"));
        assert!(!is_narrowing_type("TEXT"));
        assert!(!is_narrowing_type("VARCHAR"));
    }

    #[test]
    fn test_is_safe_cast() {
        // Widening is safe
        assert!(is_safe_cast("INT", "TEXT"));
        assert!(is_safe_cast("INT", "BIGINT"));
        assert!(is_safe_cast("SMALLINT", "INT"));
        
        // Same type is safe
        assert!(is_safe_cast("TEXT", "TEXT"));
        
        // Narrowing is unsafe
        assert!(!is_safe_cast("TEXT", "INT"));
    }

    #[test]
    fn test_classify_alter_type_to_int() {
        // AlterType TEXT -> INT should be Irreversible
        let mut cmd = Qail::make("inquiries");
        cmd.action = Action::AlterType;
        cmd.columns = vec![Expr::Def {
            name: "priority".to_string(),
            data_type: "INT".to_string(),
            constraints: vec![],
        }];
        
        assert_eq!(classify_migration(&cmd), MigrationClass::Irreversible);
    }
}
