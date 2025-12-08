//! Type definitions for sequential thinking
//!
//! This module contains all data structures used throughout the sequential thinking tool,
//! including thought data, session state, commands, and persistence formats.

use kodegen_config::KodegenConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// INTERNAL STATE
// ============================================================================

/// Internal representation of a thought
///
/// Stored in `thought_history` and branches.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ThoughtData {
    pub thought: String,
    pub thought_number: u32,
    pub total_thoughts: u32,
    pub next_thought_needed: bool,
    pub is_revision: Option<bool>,
    pub revises_thought: Option<u32>,
    pub branch_from_thought: Option<u32>,
    pub branch_id: Option<String>,
    pub needs_more_thoughts: Option<bool>,
}

/// Internal state tracking all thoughts for a single session
///
/// Each session actor task owns an instance of this state directly (no locks!)
#[derive(Debug, Default)]
pub struct ThinkingState {
    /// Linear history of all thoughts in this session
    pub thought_history: Vec<ThoughtData>,

    /// Branched thoughts organized by `branch_id`
    pub branches: HashMap<String, Vec<ThoughtData>>,
}

// ============================================================================
// SESSION COMMAND TYPES (MPSC Actor Pattern)
// ============================================================================

/// Commands sent to session actor task via MPSC
pub enum SessionCommand {
    /// Add a new thought to this session's history
    AddThought {
        thought: ThoughtData,
        /// Response channel for returning updated state
        respond_to: tokio::sync::oneshot::Sender<SessionResponse>,
    },

    /// Get current session state (for future features)
    GetState {
        respond_to: tokio::sync::oneshot::Sender<SessionStateSnapshot>,
    },

    /// Clear this session's history (for future features)
    Clear {
        respond_to: tokio::sync::oneshot::Sender<()>,
    },
}

/// Response from session actor
#[derive(Debug, Clone, Serialize)]
pub struct SessionResponse {
    pub thought_number: u32,
    pub total_thoughts: u32,
    pub next_thought_needed: bool,
    pub branches: Vec<String>,
    pub thought_history_length: usize,
}

/// Complete session state snapshot (for debugging or persistence)
#[derive(Debug, Clone, Serialize)]
pub struct SessionStateSnapshot {
    pub thought_history: Vec<ThoughtData>,
    pub branches: HashMap<String, Vec<ThoughtData>>,
}

// ============================================================================
// PERSISTENCE TYPES
// ============================================================================

/// Persistence configuration for orphaned sessions
pub struct PersistenceConfig {
    /// Base directory: $`XDG_CONFIG_HOME/kodegen/sequential_thinking`/
    pub sessions_dir: PathBuf,

    /// Age before disk cleanup (24 hours)
    pub cleanup_after: Duration,
}

impl PersistenceConfig {
    pub fn default() -> Self {
        let sessions_dir = KodegenConfig::state_dir()
            .map(|dir| dir.join("sessions").join(kodegen_config::SEQUENTIAL_THINKING))
            .unwrap_or_else(|_| {
                let mut path = PathBuf::from("sessions");
                path.push(kodegen_config::SEQUENTIAL_THINKING);
                path
            });

        Self {
            sessions_dir,
            cleanup_after: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Commands for persistence background task
pub enum PersistenceCommand {
    /// Persist a session to disk
    Persist {
        session_id: String,
        snapshot: SessionStateSnapshot,
        created_at: std::time::SystemTime,
        last_activity: std::time::SystemTime,
    },

    /// Delete a session from disk
    Delete { session_id: String },
}

/// Session metadata file (persisted as session.json)
#[derive(Debug, Serialize, Deserialize)]
pub struct SessionMetadataFile {
    pub session_id: String,
    pub created_at: std::time::SystemTime,
    pub last_activity: std::time::SystemTime,
    pub total_thoughts: usize,
    pub branch_ids: Vec<String>,
}

/// Individual thought file (persisted as thought{n}.json)
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedThought {
    pub thought_number: u32,
    pub thought_data: ThoughtData,
}

// ============================================================================
// SESSION HANDLE
// ============================================================================

/// Handle to a running session actor
#[derive(Clone)]
pub struct SessionHandle {
    /// Channel to send commands to the session's actor task
    pub tx: tokio::sync::mpsc::Sender<SessionCommand>,
    /// When this session was created (for potential future runtime reporting)
    pub created_at: Instant,
    /// Last time a command was processed (used for cleanup)
    pub last_activity: Arc<RwLock<Instant>>,
}
