//! Workflow registry — stores workflow definitions and state transitions.
//!
//! A `WorkflowDefinition` is a named set of state transitions,
//! each with a sequence of steps to execute.

use crate::step::WorkflowStep;

/// A complete workflow definition.
///
/// Contains the workflow name, initial state, and all possible
/// state transitions with their associated steps.
#[derive(Debug, Clone)]
pub struct WorkflowDefinition {
    /// Workflow name (e.g., "booking_recovery", "inventory_sync")
    pub name: String,
    /// The state a new workflow instance starts in
    pub initial_state: String,
    /// All state transitions
    pub transitions: Vec<StateTransition>,
}

/// A single state transition with the steps to execute.
#[derive(Debug, Clone)]
pub struct StateTransition {
    /// Source state
    pub from: String,
    /// Target state
    pub to: String,
    /// Steps to execute during this transition
    pub steps: Vec<WorkflowStep>,
}

impl WorkflowDefinition {
    /// Create a new workflow definition.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            initial_state: String::new(),
            transitions: Vec::new(),
        }
    }

    /// Set the initial state.
    pub fn initial_state(mut self, state: impl Into<String>) -> Self {
        self.initial_state = state.into();
        self
    }

    /// Add a state transition.
    pub fn transition(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        steps: Vec<WorkflowStep>,
    ) -> Self {
        self.transitions.push(StateTransition {
            from: from.into(),
            to: to.into(),
            steps,
        });
        self
    }

    /// Find a transition from the given state.
    /// Returns the first matching transition.
    pub fn find_transition(&self, from: &str) -> Option<&StateTransition> {
        self.transitions.iter().find(|t| t.from == from)
    }

    /// Find all transitions from the given state.
    pub fn transitions_from(&self, from: &str) -> Vec<&StateTransition> {
        self.transitions.iter().filter(|t| t.from == from).collect()
    }

    /// Get all unique state names referenced in this workflow.
    pub fn states(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();

        if !self.initial_state.is_empty() {
            names.push(&self.initial_state);
        }

        for t in &self.transitions {
            if !names.contains(&t.from.as_str()) {
                names.push(&t.from);
            }
            if !names.contains(&t.to.as_str()) {
                names.push(&t.to);
            }
        }

        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_builder() {
        let wf = WorkflowDefinition::new("test_flow")
            .initial_state("created")
            .transition("created", "pending", vec![WorkflowStep::log("Starting")])
            .transition("pending", "fulfilled", vec![WorkflowStep::log("Complete")]);

        assert_eq!(wf.name, "test_flow");
        assert_eq!(wf.initial_state, "created");
        assert_eq!(wf.transitions.len(), 2);
    }

    #[test]
    fn test_find_transition() {
        let wf = WorkflowDefinition::new("test")
            .initial_state("a")
            .transition("a", "b", vec![])
            .transition("b", "c", vec![]);

        assert!(wf.find_transition("a").is_some());
        assert_eq!(wf.find_transition("a").unwrap().to, "b");
        assert!(wf.find_transition("c").is_none());
    }

    #[test]
    fn test_states() {
        let wf = WorkflowDefinition::new("test")
            .initial_state("created")
            .transition("created", "pending", vec![])
            .transition("pending", "done", vec![]);

        let states = wf.states();
        assert!(states.contains(&"created"));
        assert!(states.contains(&"pending"));
        assert!(states.contains(&"done"));
    }
}
