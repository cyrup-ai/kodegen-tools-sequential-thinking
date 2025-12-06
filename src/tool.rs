//! Main tool implementation
//!
//! This module contains the SequentialThinkingTool struct and its implementation
//! of the Tool trait from kodegen_mcp_schema.

use crate::persistence::{start_disk_cleanup_task, start_persistence_processor, try_restore_session};
use crate::session::spawn_session_actor;
use crate::types::{
    PersistenceCommand, SessionCommand, SessionHandle, SessionStateSnapshot, ThoughtData,
};
use kodegen_mcp_schema::reasoning::SequentialThinkingArgs;
use kodegen_mcp_schema::McpError;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

// ============================================================================
// TOOL STRUCT (SESSION MANAGER)
// ============================================================================

/// Sequential Thinking tool using MPSC actor pattern for session management
///
/// Each session has an isolated async task that owns its state directly.
/// This eliminates lock contention and provides perfect isolation between users.
#[derive(Clone)]
pub struct SequentialThinkingTool {
    /// Active session handles (only stores channel senders, not state)
    sessions: Arc<RwLock<HashMap<String, SessionHandle>>>,

    /// Fire-and-forget channel for persistence requests
    persistence_sender: tokio::sync::mpsc::UnboundedSender<PersistenceCommand>,
}

impl Default for SequentialThinkingTool {
    fn default() -> Self {
        Self::new()
    }
}

impl SequentialThinkingTool {
    /// Create a new `SequentialThinkingTool` instance
    #[must_use]
    pub fn new() -> Self {
        // Create persistence channel
        let (persistence_sender, persistence_receiver) = tokio::sync::mpsc::unbounded_channel();

        let tool = Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            persistence_sender: persistence_sender.clone(),
        };

        // Start background persistence processor
        start_persistence_processor(persistence_receiver);

        // Start hourly disk cleanup task
        start_disk_cleanup_task(persistence_sender);

        tool
    }

    /// Generate unique session ID using UUID v4
    fn generate_session_id(&self) -> String {
        Uuid::new_v4().to_string()
    }

    /// Get or create a session
    pub async fn get_or_create_session(
        &self,
        session_id: Option<String>,
    ) -> Result<(String, tokio::sync::mpsc::Sender<SessionCommand>), McpError> {
        // Generate session ID if not provided
        let session_id = match session_id {
            Some(id) => id,
            None => self.generate_session_id(),
        };

        // Check if session exists in memory
        {
            let sessions = self.sessions.read().await;
            if let Some(handle) = sessions.get(&session_id) {
                // Update last activity
                *handle.last_activity.write().await = Instant::now();
                return Ok((session_id, handle.tx.clone()));
            }
        }

        // Try to restore from disk before creating new session
        if let Some(restored_handle) =
            try_restore_session(&session_id, &self.persistence_sender).await
        {
            // Add restored session to active sessions
            let tx = restored_handle.tx.clone();
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), restored_handle);
            return Ok((session_id, tx));
        }

        // Create new session if not found in memory or disk
        let (tx, rx) = tokio::sync::mpsc::channel::<SessionCommand>(100);

        // Spawn actor task
        spawn_session_actor(rx);

        // Store handle
        let handle = SessionHandle {
            tx: tx.clone(),
            created_at: Instant::now(),
            last_activity: Arc::new(RwLock::new(Instant::now())),
        };

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_id.clone(), handle);
        }

        Ok((session_id, tx))
    }

    /// Get session state snapshot (for debugging or persistence)
    pub async fn get_session_state(
        &self,
        session_id: &str,
    ) -> Result<SessionStateSnapshot, McpError> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let (respond_to, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::GetState { respond_to };

        handle
            .tx
            .send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        rx.await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Failed to receive state")))
    }

    /// Clear a session's history (for starting fresh with same session ID)
    pub async fn clear_session(&self, session_id: &str) -> Result<(), McpError> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let (respond_to, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::Clear { respond_to };

        handle
            .tx
            .send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        rx.await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Failed to clear session")))
    }

    /// Get session info including creation time and activity
    pub async fn get_session_info(&self, session_id: &str) -> Result<(Instant, Instant), McpError> {
        let sessions = self.sessions.read().await;
        let handle = sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let created_at = handle.created_at;
        let last_activity = *handle.last_activity.read().await;

        Ok((created_at, last_activity))
    }

    /// Clean up inactive sessions
    async fn cleanup_sessions(&self, max_age: Duration) {
        let purge_cutoff = Instant::now()
            .checked_sub(max_age)
            .unwrap_or_else(Instant::now);

        let mut sessions = self.sessions.write().await;
        let mut to_persist = Vec::new();

        sessions.retain(|session_id, handle| {
            // Closed channels: session actor terminated, remove immediately
            if handle.tx.is_closed() {
                log::debug!("Removing closed session: {session_id}");
                return false;
            }

            // Check last activity
            let last_activity = handle
                .last_activity
                .try_read()
                .map_or_else(|_| Instant::now(), |t| *t);

            // Old sessions: persist before removal
            if last_activity < purge_cutoff {
                log::debug!("Session {session_id} expired, will persist before removal");
                to_persist.push((session_id.clone(), handle.clone()));
                return false;
            }

            true
        });

        drop(sessions);

        // Persist sessions outside of lock (fire-and-forget)
        for (session_id, handle) in to_persist {
            // Get session state via GetState command
            let (respond_to, rx) = tokio::sync::oneshot::channel();
            if handle
                .tx
                .send(SessionCommand::GetState { respond_to })
                .await
                .is_ok()
                && let Ok(snapshot) = rx.await
            {
                // Convert Instant to SystemTime for persistence
                let created_at_elapsed = handle.created_at.elapsed();
                let created_at = std::time::SystemTime::now()
                    .checked_sub(created_at_elapsed)
                    .unwrap_or_else(std::time::SystemTime::now);

                let last_activity_instant = *handle.last_activity.read().await;
                let last_activity_elapsed = last_activity_instant.elapsed();
                let last_activity = std::time::SystemTime::now()
                    .checked_sub(last_activity_elapsed)
                    .unwrap_or_else(std::time::SystemTime::now);

                // Send to persistence task (fire-and-forget)
                let _ = self.persistence_sender.send(PersistenceCommand::Persist {
                    session_id: session_id.clone(),
                    snapshot,
                    created_at,
                    last_activity,
                });
            }
        }
    }

    /// Start background cleanup task (call once on manager creation)
    /// Pattern from search_manager.rs:565-573
    pub fn start_cleanup_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
            loop {
                interval.tick().await;
                self.cleanup_sessions(Duration::from_secs(30 * 60)).await;
            }
        });
    }

    /// Shutdown the tool gracefully, persisting all active sessions
    ///
    /// Called during server shutdown to ensure no sessions are lost.
    /// Persists all active sessions to disk before terminating.
    pub async fn shutdown(&self) -> Result<(), McpError> {
        log::info!("Shutting down sequential thinking tool, persisting active sessions");

        // Get snapshot of all active sessions
        let sessions = self.sessions.read().await;
        let session_ids: Vec<String> = sessions.keys().cloned().collect();
        drop(sessions);

        log::debug!("Found {} active sessions to persist", session_ids.len());

        // Persist each session
        for session_id in session_ids {
            // Get session state
            if let Ok(snapshot) = self.get_session_state(&session_id).await {
                // Get session handle for timestamps
                let sessions = self.sessions.read().await;
                if let Some(handle) = sessions.get(&session_id) {
                    // Convert Instant → SystemTime (pattern from cleanup_sessions)
                    let created_at_elapsed = handle.created_at.elapsed();
                    let created_at = std::time::SystemTime::now()
                        .checked_sub(created_at_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    let last_activity_instant = *handle.last_activity.read().await;
                    let last_activity_elapsed = last_activity_instant.elapsed();
                    let last_activity = std::time::SystemTime::now()
                        .checked_sub(last_activity_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    // Send persistence command (fire-and-forget)
                    let _ = self.persistence_sender.send(PersistenceCommand::Persist {
                        session_id: session_id.clone(),
                        snapshot,
                        created_at,
                        last_activity,
                    });

                    log::debug!("Queued session {} for persistence", session_id);
                }
            }
        }

        // Give persistence task time to process all commands
        // (persistence runs in background, this ensures writes complete)
        tokio::time::sleep(Duration::from_millis(500)).await;

        log::info!("Sequential thinking tool shutdown complete");
        Ok(())
    }

    /// Validate and convert args to `ThoughtData`
    /// Auto-adjusts totalThoughts if thoughtNumber exceeds it
    pub fn validate_thought(args: SequentialThinkingArgs) -> ThoughtData {
        // Auto-adjust totalThoughts if needed (ensures consistency)
        let total_thoughts = args.total_thoughts.max(args.thought_number);

        ThoughtData {
            thought: args.thought,
            thought_number: args.thought_number,
            total_thoughts,
            next_thought_needed: args.next_thought_needed,
            is_revision: args.is_revision,
            revises_thought: args.revises_thought,
            branch_from_thought: args.branch_from_thought,
            branch_id: args.branch_id,
            needs_more_thoughts: args.needs_more_thoughts,
        }
    }
}
