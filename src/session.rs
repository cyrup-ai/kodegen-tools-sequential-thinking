//! Session actor management
//!
//! This module handles the MPSC actor pattern for session management.
//! Each session has an isolated async task that owns its state directly,
//! eliminating lock contention and providing perfect isolation between users.

use crate::types::{
    SessionCommand, SessionResponse, SessionStateSnapshot, ThinkingState,
};

// ============================================================================
// SESSION ACTOR TASK
// ============================================================================

/// Spawn session actor with optional initial state
///
/// The spawned task exclusively owns the `ThinkingState` for this session.
/// No locks needed within the task since only this task accesses the state.
pub fn spawn_session_actor_with_state(
    mut rx: tokio::sync::mpsc::Receiver<SessionCommand>,
    initial_state: ThinkingState,
) {
    tokio::spawn(async move {
        // Task OWNS the state - no locks needed!
        let mut state = initial_state;

        // Process commands until channel closes
        while let Some(cmd) = rx.recv().await {
            match cmd {
                SessionCommand::AddThought {
                    thought,
                    respond_to,
                } => {
                    // Update state (lock-free - we own it!)
                    state.thought_history.push(thought.clone());

                    // Add to branch if applicable
                    if let (Some(_), Some(branch_id)) =
                        (thought.branch_from_thought, &thought.branch_id)
                    {
                        state
                            .branches
                            .entry(branch_id.clone())
                            .or_default()
                            .push(thought.clone());
                    }

                    // Build response
                    let response = SessionResponse {
                        thought_number: thought.thought_number,
                        total_thoughts: thought.total_thoughts,
                        next_thought_needed: thought.next_thought_needed,
                        branches: state.branches.keys().cloned().collect(),
                        thought_history_length: state.thought_history.len(),
                    };

                    // Send response (ignore if receiver dropped)
                    let _ = respond_to.send(response);

                    // Terminate session if thinking is complete
                    if !thought.next_thought_needed {
                        log::debug!(
                            "Session completed (final thought {}), terminating actor",
                            thought.thought_number
                        );
                        break;
                    }
                }

                SessionCommand::GetState { respond_to } => {
                    let snapshot = SessionStateSnapshot {
                        thought_history: state.thought_history.clone(),
                        branches: state.branches.clone(),
                    };
                    let _ = respond_to.send(snapshot);
                }

                SessionCommand::Clear { respond_to } => {
                    state.thought_history.clear();
                    state.branches.clear();
                    let _ = respond_to.send(());
                    log::debug!("Session cleared, terminating actor");
                    break;
                }
            }
        }
        // Channel closed - session terminated, state automatically cleaned up
        log::debug!("Session actor task terminated, state cleaned up");
    });
}

/// Spawn new session actor with empty state
pub fn spawn_session_actor(
    rx: tokio::sync::mpsc::Receiver<SessionCommand>,
) {
    // Delegate to _with_state with default state
    spawn_session_actor_with_state(rx, ThinkingState::default());
}


