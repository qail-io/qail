//! Transaction control methods for PostgreSQL connection.

use super::{PgConnection, PgError, PgResult};

/// Quote a SQL identifier (for savepoint names).
/// Wraps in double-quotes and escapes embedded double-quotes.
fn quote_savepoint_name(name: &str) -> PgResult<String> {
    if name.is_empty() {
        return Err(PgError::Query("savepoint name is empty".to_string()));
    }
    if name.contains('\0') {
        return Err(PgError::Query(
            "savepoint name contains NUL byte".to_string(),
        ));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

/// How the server ended a transaction on COMMIT, read from the
/// CommandComplete tags of the SQL that began with the COMMIT.
///
/// PostgreSQL answers COMMIT on a failed transaction with the tag `ROLLBACK`
/// and no ErrorResponse: the statement succeeds and every write the
/// transaction made is gone. Any first tag but `COMMIT` is that loss.
pub(crate) fn commit_outcome(tags: &[String], operation: &str) -> PgResult<()> {
    match tags.first().map(String::as_str) {
        Some("COMMIT") => Ok(()),
        Some(tag) => Err(PgError::Query(format!(
            "{operation}: the server answered COMMIT with {tag}; \
             the transaction had failed and none of its writes were kept"
        ))),
        None => Err(PgError::Query(format!(
            "{operation}: the server answered COMMIT without a command tag; \
             whether the transaction's writes were kept is unknown"
        ))),
    }
}

impl PgConnection {
    /// Begin a new transaction.
    /// After calling this, all queries run within the transaction
    /// until `commit()` or `rollback()` is called.
    pub async fn begin_transaction(&mut self) -> PgResult<()> {
        self.execute_simple("BEGIN").await
    }

    /// Commit the current transaction.
    /// Makes all changes since `begin_transaction()` permanent.
    ///
    /// Errors when the server answers the COMMIT with `ROLLBACK`: a
    /// statement in the transaction had failed, so none of its changes were
    /// kept. The transaction is over either way.
    pub async fn commit(&mut self) -> PgResult<()> {
        let tags = self.execute_simple_tags("COMMIT").await?;
        commit_outcome(&tags, "COMMIT")
    }

    /// Rollback the current transaction.
    /// Discards all changes since `begin_transaction()`.
    pub async fn rollback(&mut self) -> PgResult<()> {
        self.execute_simple("ROLLBACK").await
    }

    /// Create a named savepoint within the current transaction.
    /// Savepoints allow partial rollback within a transaction.
    /// Use `rollback_to()` to return to this savepoint.
    pub async fn savepoint(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!("SAVEPOINT {}", quote_savepoint_name(name)?))
            .await
    }

    /// Rollback to a previously created savepoint.
    /// Discards all changes since the named savepoint was created,
    /// but keeps the transaction open.
    pub async fn rollback_to(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!(
            "ROLLBACK TO SAVEPOINT {}",
            quote_savepoint_name(name)?
        ))
        .await
    }

    /// Release a savepoint (free resources, if no longer needed).
    pub async fn release_savepoint(&mut self, name: &str) -> PgResult<()> {
        self.execute_simple(&format!(
            "RELEASE SAVEPOINT {}",
            quote_savepoint_name(name)?
        ))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{commit_outcome, quote_savepoint_name};

    fn tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    #[test]
    fn commit_outcome_keeps_a_commit_tag() {
        assert!(commit_outcome(&tags(&["COMMIT"]), "COMMIT").is_ok());
        assert!(commit_outcome(&tags(&["COMMIT", "CLOSE CURSOR ALL", "SET"]), "COMMIT").is_ok());
    }

    #[test]
    fn commit_outcome_reports_a_commit_answered_rollback() {
        let err = commit_outcome(
            &tags(&["ROLLBACK", "CLOSE CURSOR ALL"]),
            "pool release COMMIT",
        )
        .expect_err("a rolled-back COMMIT is an error");
        let text = err.to_string();
        assert!(text.contains("pool release COMMIT"), "{text}");
        assert!(text.contains("ROLLBACK"), "{text}");
    }

    #[test]
    fn commit_outcome_reports_a_commit_without_a_tag() {
        assert!(commit_outcome(&[], "COMMIT").is_err());
    }

    #[test]
    fn quote_savepoint_name_escapes_quotes() {
        assert_eq!(quote_savepoint_name("sp\"1").unwrap(), "\"sp\"\"1\"");
    }

    #[test]
    fn quote_savepoint_name_rejects_empty_or_nul() {
        assert!(quote_savepoint_name("").is_err());
        assert!(quote_savepoint_name("sp\0shadow").is_err());
    }
}
