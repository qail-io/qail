//! Property-based testing with proptest.
//!
//! Provides Arbitrary implementations for AST types and property tests
//! for roundtrip invariants.

pub mod arbitrary;

#[cfg(test)]
mod roundtrip;

#[cfg(test)]
mod invariants;
