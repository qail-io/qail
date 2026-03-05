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

pub(crate) use handlers::{
    branch_create_handler, branch_delete_handler, branch_list_handler, branch_merge_handler,
};
pub(crate) use overlay::{apply_branch_overlay, redirect_to_overlay};
