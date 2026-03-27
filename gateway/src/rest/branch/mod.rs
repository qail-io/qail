//! Branch management handlers and Copy-on-Write helpers for data virtualization.
//!
//! - `apply_branch_overlay` — CoW Read: merge overlay into query results
//! - `redirect_to_overlay` — CoW Write: redirect mutations to overlay table
//! - `branch_create_handler` — POST /api/_branch
//! - `branch_list_handler` — GET /api/_branch
//! - `branch_delete_handler` — DELETE /api/_branch/:name
//! - `branch_merge_handler` — POST /api/_branch/:name/merge

mod handlers;
mod overlay;

use qail_core::branch::BranchContext;

use crate::middleware::ApiError;

pub(crate) use handlers::{
    branch_create_handler, branch_delete_handler, branch_list_handler, branch_merge_handler,
};
pub(crate) use overlay::{apply_branch_overlay, redirect_to_overlay};

pub(crate) fn validate_branch_name(name: &str) -> Result<(), ApiError> {
    if name.eq_ignore_ascii_case("main") {
        return Err(ApiError::bad_request(
            "INVALID_BRANCH_NAME",
            "Branch name 'main' is reserved for the default branch",
        ));
    }
    if !BranchContext::is_valid_name(name) {
        return Err(ApiError::bad_request(
            "INVALID_BRANCH_NAME",
            format!(
                "Invalid branch name '{}'. Use 1-{} ASCII alphanumeric/._- characters",
                name,
                BranchContext::MAX_NAME_LEN
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_branch_name;

    #[test]
    fn validate_branch_name_accepts_valid_name() {
        assert!(validate_branch_name("feature_auth.1").is_ok());
    }

    #[test]
    fn validate_branch_name_rejects_main() {
        assert!(validate_branch_name("main").is_err());
        assert!(validate_branch_name("MAIN").is_err());
    }

    #[test]
    fn validate_branch_name_rejects_invalid_chars() {
        assert!(validate_branch_name("bad name").is_err());
        assert!(validate_branch_name("bad;drop").is_err());
    }
}
