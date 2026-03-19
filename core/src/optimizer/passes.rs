use super::{NormalizedMutation, NormalizedSelect};

/// Apply a deterministic, semantics-preserving cleanup pass.
///
/// This is the first rewrite pass used by optimizer tests and as a base
/// for future transformation passes.
pub fn cleanup_select(select: &NormalizedSelect) -> NormalizedSelect {
    select.cleaned()
}

/// Apply deterministic cleanup for supported mutation rewrites.
pub fn cleanup_mutation(mutation: &NormalizedMutation) -> NormalizedMutation {
    mutation.cleaned()
}
