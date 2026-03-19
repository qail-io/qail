//! Public N+1 diagnostic types.

/// Diagnostic rule code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NPlusOneCode {
    /// Query execution inside a loop.
    N1001,
    /// Loop variable used in query-building chain — suggests batching.
    N1002,
    /// Function/method that executes a query called inside a work loop.
    N1003,
    /// Query execution inside nested loops (loop_depth ≥ 2).
    N1004,
}

impl NPlusOneCode {
    /// Human-readable code string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::N1001 => "N1-001",
            Self::N1002 => "N1-002",
            Self::N1003 => "N1-003",
            Self::N1004 => "N1-004",
        }
    }
}

impl std::fmt::Display for NPlusOneCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Diagnostic severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NPlusOneSeverity {
    Warning,
    Error,
}

impl std::fmt::Display for NPlusOneSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warning => f.write_str("warning"),
            Self::Error => f.write_str("error"),
        }
    }
}

/// A single N+1 diagnostic.
#[derive(Debug, Clone)]
pub struct NPlusOneDiagnostic {
    pub code: NPlusOneCode,
    pub severity: NPlusOneSeverity,
    pub file: String,
    pub line: usize,
    pub column: usize,
    /// End column of the highlighted token (for LSP range precision).
    pub end_column: usize,
    pub message: String,
    pub hint: Option<String>,
}

impl std::fmt::Display for NPlusOneDiagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}:{}:{}: {}",
            self.code, self.file, self.line, self.column, self.message
        )?;
        if let Some(ref hint) = self.hint {
            write!(f, " (hint: {})", hint)?;
        }
        Ok(())
    }
}
