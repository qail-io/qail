use std::collections::BTreeSet;

use crate::ast::{Action, ConflictAction, MergeAction, Qail};

use super::model::AccessOperation;

/// Required operations for a full command.
pub fn required_operations_for_command(cmd: &Qail) -> Option<BTreeSet<AccessOperation>> {
    let mut operations = BTreeSet::new();
    match cmd.action {
        Action::Add => {
            operations.insert(AccessOperation::Create);
            if matches!(
                cmd.on_conflict.as_ref().map(|conflict| &conflict.action),
                Some(ConflictAction::DoUpdate { .. })
            ) {
                operations.insert(AccessOperation::Update);
            }
        }
        Action::Merge => {
            if let Some(merge) = &cmd.merge {
                for clause in &merge.clauses {
                    match &clause.action {
                        MergeAction::Update { .. } => {
                            operations.insert(AccessOperation::Update);
                        }
                        MergeAction::Insert { .. } => {
                            operations.insert(AccessOperation::Create);
                        }
                        MergeAction::Delete => {
                            operations.insert(AccessOperation::Delete);
                        }
                        MergeAction::DoNothing => {}
                    }
                }
                if operations.is_empty() {
                    operations.extend([
                        AccessOperation::Create,
                        AccessOperation::Update,
                        AccessOperation::Delete,
                    ]);
                }
            } else {
                operations.extend([
                    AccessOperation::Create,
                    AccessOperation::Update,
                    AccessOperation::Delete,
                ]);
            }
        }
        action => {
            operations.extend(AccessOperation::required_for_action(action)?);
        }
    }
    Some(operations)
}
