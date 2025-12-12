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
                    // NOTE: New thought chains are handled by SequenceManager in tool.rs
                    // When thought_number == 1, a new session actor is spawned with fresh state.
                    // No need to clear state here - this actor only sees one thought chain.

                    // ================================================================
                    // STEP 1: Clone for branching ONLY if needed (before any moves)
                    // ================================================================
                    let branch_clone = match (&thought.branch_from_thought, &thought.branch_id) {
                        (Some(_), Some(branch_id)) => Some((branch_id.clone(), thought.clone())),
                        _ => None,
                    };

                    // ================================================================
                    // STEP 2: Extract Copy fields for response BEFORE moving thought
                    // ================================================================
                    let thought_number = thought.thought_number;
                    let total_thoughts = thought.total_thoughts;
                    let next_thought_needed = thought.next_thought_needed;

                    // ================================================================
                    // STEP 3: Move thought into history (ZERO-COPY for non-branching)
                    // ================================================================
                    state.thought_history.push(thought);

                    // ================================================================
                    // STEP 4: Insert pre-cloned thought into branches if applicable
                    // ================================================================
                    if let Some((branch_id, thought_clone)) = branch_clone {
                        state.branches.entry(branch_id).or_default().push(thought_clone);
                    }

                    // ================================================================
                    // STEP 5: Build response using extracted Copy fields
                    // ================================================================
                    let response = SessionResponse {
                        thought_number,
                        total_thoughts,
                        next_thought_needed,
                        branches: state.branches.keys().cloned().collect(),
                        thought_history_length: state.thought_history.len(),
                    };

                    // ================================================================
                    // STEP 6: Send response
                    // ================================================================
                    let _ = respond_to.send(response);

                    // ================================================================
                    // STEP 7: Check termination using extracted flag
                    // ================================================================
                    if !next_thought_needed {
                        log::debug!(
                            "Thought {} marked as final, terminating session actor",
                            thought_number
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


