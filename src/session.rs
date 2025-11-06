//! Session actor management
//!
//! This module handles the MPSC actor pattern for session management.
//! Each session has an isolated async task that owns its state directly,
//! eliminating lock contention and providing perfect isolation between users.

use crate::types::{
    SessionCommand, SessionResponse, SessionStateSnapshot, ThinkingState, ThoughtData,
};
use std::io::Write;
use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

// ============================================================================
// SESSION ACTOR TASK
// ============================================================================

/// Spawn session actor with optional initial state
///
/// The spawned task exclusively owns the `ThinkingState` for this session.
/// No locks needed within the task since only this task accesses the state.
pub fn spawn_session_actor_with_state(
    mut rx: tokio::sync::mpsc::Receiver<SessionCommand>,
    disable_logging: bool,
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

                    // Log to stderr if enabled
                    if !disable_logging {
                        let formatted = format_thought(&thought);
                        let bufwtr = BufferWriter::stderr(ColorChoice::Auto);
                        let mut buffer = bufwtr.buffer();
                        let _ = write!(&mut buffer, "{formatted}");
                        let _ = bufwtr.print(&buffer);
                    }

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
    disable_logging: bool,
) {
    // Delegate to _with_state with default state
    spawn_session_actor_with_state(rx, disable_logging, ThinkingState::default());
}

// ============================================================================
// FORMATTING
// ============================================================================

/// Format thought for stderr display with ANSI colors
/// Creates a bordered box with colored prefix based on thought type
pub fn format_thought(data: &ThoughtData) -> String {
    let bufwtr = BufferWriter::stderr(ColorChoice::Auto);
    let mut buffer = bufwtr.buffer();

    // Determine the prefix text and color based on thought type
    let (prefix_text, prefix_color, context) = if data.is_revision.unwrap_or(false) {
        let ctx = data
            .revises_thought
            .map(|n| format!(" (revising thought {n})"))
            .unwrap_or_default();
        ("🔄 Revision", Color::Yellow, ctx)
    } else if let Some(branch_from) = data.branch_from_thought {
        let ctx = format!(
            " (from thought {}, ID: {})",
            branch_from,
            data.branch_id.as_deref().unwrap_or("unknown")
        );
        ("🌿 Branch", Color::Green, ctx)
    } else {
        ("💭 Thought", Color::Blue, String::new())
    };

    // Create the header with colored prefix
    let _ = write!(&mut buffer, "\n┌");

    // Calculate border length - we'll build header first to get accurate length
    let header_plain = format!(
        "{prefix_text} {}/{}{context}",
        data.thought_number, data.total_thoughts
    );
    let border_len = header_plain.len().max(data.thought.len()) + 4;
    let border = "─".repeat(border_len);

    let _ = writeln!(&mut buffer, "{border}┐");
    let _ = write!(&mut buffer, "│ ");

    // Write colored prefix
    let _ = buffer.set_color(ColorSpec::new().set_fg(Some(prefix_color)));
    let _ = write!(&mut buffer, "{prefix_text}");
    let _ = buffer.reset();

    // Write rest of header
    let _ = writeln!(
        &mut buffer,
        " {}/{}{context} │",
        data.thought_number, data.total_thoughts
    );
    let _ = writeln!(&mut buffer, "├{border}┤");
    let _ = writeln!(&mut buffer, "│ {} │", data.thought);
    let _ = writeln!(&mut buffer, "└{border}┘");

    String::from_utf8_lossy(buffer.as_slice()).to_string()
}
