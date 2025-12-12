//! Main tool implementation
//!
//! This module contains the SequentialThinkingTool struct and its implementation
//! of the Tool trait from kodegen_mcp_schema.

use crate::persistence::{start_disk_cleanup_task, start_persistence_processor, try_restore_session};
use crate::session::spawn_session_actor;
use crate::types::{
    PersistenceCommand, SessionCommand, SessionHandle, SessionStateSnapshot, ThoughtData,
};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use kodegen_mcp_schema::sequential_thinking::SequentialThinkingArgs;
use kodegen_mcp_schema::McpError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// CONFIGURATION CONSTANTS
// ============================================================================

/// Persistence channel capacity (prevents OOM from unbounded queue growth)
/// 
/// Sized to handle typical shutdown scenarios:
/// - 100 capacity = supports ~100 concurrent persistence operations
/// - At ~100KB per session, max buffer = ~10MB
/// - Provides backpressure when disk I/O is slow
const PERSISTENCE_CHANNEL_CAPACITY: usize = 100;

/// Batch size for persistence operations during shutdown
///
/// Larger batches improve I/O efficiency but increase memory per message.
/// - 50 sessions per batch = ~5MB per batch message
/// - Reduces total messages by 50x (500 sessions → 10 batch messages)
const PERSISTENCE_BATCH_SIZE: usize = 50;

// ============================================================================
// TOOL STRUCT (SESSION MANAGER)
// ============================================================================

/// Sequential Thinking tool using MPSC actor pattern for session management
///
/// Each session has an isolated async task that owns its state directly.
/// This eliminates lock contention and provides perfect isolation between users.
#[derive(Clone)]
pub struct SequentialThinkingTool {
    /// Lock-free concurrent map of active session handles
    /// Key: connection_id (one session per connection)
    /// Value: SessionHandle (contains channel sender + metadata)
    sessions: Arc<DashMap<String, SessionHandle>>,

    /// Fire-and-forget channel for persistence requests
    persistence_sender: tokio::sync::mpsc::Sender<PersistenceCommand>,
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
        let (persistence_sender, persistence_receiver) = 
            tokio::sync::mpsc::channel(PERSISTENCE_CHANNEL_CAPACITY);

        let tool = Self {
            sessions: Arc::new(DashMap::new()),
            persistence_sender: persistence_sender.clone(),
        };

        // Start background persistence processor
        start_persistence_processor(persistence_receiver);

        // Start hourly disk cleanup task
        start_disk_cleanup_task(persistence_sender);

        tool
    }

    /// Get or create a connection session (RACE-FREE)
    ///
    /// Returns (connection_id, channel_sender) for communication with the session actor.
    /// Note: The returned ID is the connection_id, not a full session_id.
    /// Full session_id = "{connection_id}_{sequence_id}" is constructed by the caller
    /// using the sequence_id from SessionResponse.
    ///
    /// ## Race Condition Safety
    ///
    /// This implementation is safe against TOCTOU races because:
    /// 1. Fast path uses DashMap::get() (lock-free, atomic)
    /// 2. Slow path uses DashMap::entry() for atomic check-and-insert
    /// 3. If two threads race during disk restore, only one wins via Entry::Vacant
    /// 4. Loser's restored/created handle is dropped (actor terminates cleanly)
    pub async fn get_or_create_session(
        &self,
        connection_id: &str,
    ) -> Result<(String, tokio::sync::mpsc::Sender<SessionCommand>), McpError> {
        let conn_id = connection_id.to_string();

        // ========================================================================
        // FAST PATH: Check if session already exists (lock-free read)
        // ========================================================================
        if let Some(entry) = self.sessions.get(&conn_id) {
            // Update last activity timestamp
            *entry.value().last_activity.write().await = Instant::now();
            return Ok((conn_id, entry.value().tx.clone()));
        }

        // ========================================================================
        // SLOW PATH: Session not in memory, try restore from disk
        // ========================================================================
        // No lock held during async disk I/O - allows concurrent requests
        let maybe_restored = try_restore_session(&conn_id, &self.persistence_sender).await;

        // ========================================================================
        // ATOMIC INSERT: Use entry() API for race-free check-and-insert
        // ========================================================================
        let handle = match self.sessions.entry(conn_id.clone()) {
            Entry::Occupied(entry) => {
                // Another thread created the session while we were doing disk I/O
                // Use their session handle and discard ours (if any)
                log::debug!(
                    "Session {} already created by another thread, using existing",
                    conn_id
                );

                // Update last activity and return existing handle
                *entry.get().last_activity.write().await = Instant::now();
                entry.get().clone()
            }

            Entry::Vacant(entry) => {
                // We won the race! Create or use restored session
                let handle = if let Some(restored_handle) = maybe_restored {
                    // Use the restored session from disk
                    log::info!("Restored session {} from disk", conn_id);
                    restored_handle
                } else {
                    // Create brand new session
                    log::debug!("Creating new session {}", conn_id);

                    let (tx, rx) = tokio::sync::mpsc::channel::<SessionCommand>(100);

                    // Spawn session actor task
                    spawn_session_actor(rx);

                    SessionHandle {
                        tx,
                        created_at: Instant::now(),
                        last_activity: Arc::new(RwLock::new(Instant::now())),
                    }
                };

                // Insert into map atomically (we hold the Entry::Vacant lock)
                entry.insert(handle.clone());
                handle
            }
        };

        Ok((conn_id, handle.tx.clone()))
    }

    /// Get session state snapshot (for debugging or persistence)
    pub async fn get_session_state(
        &self,
        session_id: &str,
    ) -> Result<SessionStateSnapshot, McpError> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let (respond_to, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::GetState { respond_to };

        handle
            .value()
            .tx
            .send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        rx.await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Failed to receive state")))
    }

    /// Clear a session's history (for starting fresh with same session ID)
    pub async fn clear_session(&self, session_id: &str) -> Result<(), McpError> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let (respond_to, rx) = tokio::sync::oneshot::channel();
        let cmd = SessionCommand::Clear { respond_to };

        handle
            .value()
            .tx
            .send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        rx.await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Failed to clear session")))
    }

    /// Get session info including creation time and activity
    pub async fn get_session_info(&self, session_id: &str) -> Result<(Instant, Instant), McpError> {
        let handle = self
            .sessions
            .get(session_id)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {session_id}")))?;

        let created_at = handle.value().created_at;
        let last_activity = *handle.value().last_activity.read().await;

        Ok((created_at, last_activity))
    }

    /// Clean up inactive sessions
    async fn cleanup_sessions(&self, max_age: Duration) {
        let purge_cutoff = Instant::now()
            .checked_sub(max_age)
            .unwrap_or_else(Instant::now);

        let mut to_persist = Vec::new();

        // Iterate and remove in-place (DashMap supports concurrent modification)
        self.sessions.retain(|session_id, handle| {
            // Closed channels: session actor terminated, remove immediately
            if handle.tx.is_closed() {
                log::debug!("Removing closed session: {}", session_id);
                return false;
            }

            // Check last activity
            let last_activity = handle
                .last_activity
                .try_read()
                .map_or_else(|_| Instant::now(), |t| *t);

            // Old sessions: persist before removal
            if last_activity < purge_cutoff {
                log::debug!("Session {} expired, will persist before removal", session_id);
                to_persist.push((session_id.clone(), handle.clone()));
                return false;
            }

            true // Keep session
        });

        // Persist sessions outside of map iteration (fire-and-forget)
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

                // Use try_send for non-critical cleanup (don't block cleanup task)
                match self.persistence_sender.try_send(PersistenceCommand::Persist {
                    session_id: session_id.clone(),
                    snapshot,
                    created_at,
                    last_activity,
                }) {
                    Ok(_) => {
                        log::debug!("Queued session {} for persistence", session_id);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        log::warn!(
                            "Persistence channel full, skipping persistence for session {}. \
                             Session will be re-queued on next cleanup cycle.",
                            session_id
                        );
                        // Session will be persisted on next cleanup cycle or shutdown
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        log::error!("Persistence channel closed, cannot persist session {}", session_id);
                    }
                }
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
    /// Uses completion channels to wait for actual persistence completion.
    /// Each batch has a 30-second timeout to prevent indefinite blocking.
    /// Pattern from kodegen-server-http/src/managers.rs shutdown()
    pub async fn shutdown(&self) -> Result<(), McpError> {
        log::info!("Shutting down sequential thinking tool, persisting active sessions");

        // Get snapshot of all active sessions
        let session_ids: Vec<String> = self.sessions.iter()
            .map(|entry| entry.key().clone())
            .collect();

        let total_sessions = session_ids.len();
        log::debug!("Found {} active sessions to persist", total_sessions);

        if total_sessions == 0 {
            log::info!("No sessions to persist, shutdown complete");
            return Ok(());
        }

        // Collect completion receivers for all batches
        let mut batch_receivers: Vec<tokio::sync::oneshot::Receiver<Result<usize, String>>> = Vec::new();
        let mut current_batch: Vec<(String, SessionStateSnapshot, std::time::SystemTime, std::time::SystemTime)> = Vec::new();
        let mut sessions_queued = 0usize;

        for session_id in session_ids {
            // Get session state
            if let Ok(snapshot) = self.get_session_state(&session_id).await {
                // Get session handle for timestamps
                if let Some(handle) = self.sessions.get(&session_id) {
                    // Convert Instant to SystemTime
                    let created_at_elapsed = handle.created_at.elapsed();
                    let created_at = std::time::SystemTime::now()
                        .checked_sub(created_at_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    let last_activity_instant = *handle.last_activity.read().await;
                    let last_activity_elapsed = last_activity_instant.elapsed();
                    let last_activity = std::time::SystemTime::now()
                        .checked_sub(last_activity_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    // Add to current batch
                    current_batch.push((session_id.clone(), snapshot, created_at, last_activity));
                    sessions_queued += 1;

                    // Send batch when it reaches target size
                    if current_batch.len() >= PERSISTENCE_BATCH_SIZE {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        
                        if let Err(e) = self.persistence_sender.send(
                            PersistenceCommand::PersistBatch {
                                sessions: std::mem::take(&mut current_batch),
                                completion: Some(tx),
                            }
                        ).await {
                            log::error!("Failed to send persistence batch: {e}");
                            break;
                        }
                        
                        batch_receivers.push(rx);
                    }
                }
            }
        }

        // Send final partial batch if not empty
        if !current_batch.is_empty() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            
            if let Err(e) = self.persistence_sender.send(
                PersistenceCommand::PersistBatch {
                    sessions: current_batch,
                    completion: Some(tx),
                }
            ).await {
                log::error!("Failed to send final persistence batch: {e}");
            } else {
                batch_receivers.push(rx);
            }
        }

        let batch_count = batch_receivers.len();
        log::info!(
            "Queued {} sessions in {} batches, waiting for completion",
            sessions_queued, batch_count
        );

        // Wait for all batches to complete with per-batch timeout
        // 30 seconds is generous for slow disks while preventing indefinite hang
        const BATCH_TIMEOUT: Duration = Duration::from_secs(30);
        
        let mut total_persisted = 0usize;
        let mut batches_succeeded = 0usize;
        let mut batches_failed = 0usize;
        let mut batches_timeout = 0usize;

        for (batch_num, rx) in batch_receivers.into_iter().enumerate() {
            match tokio::time::timeout(BATCH_TIMEOUT, rx).await {
                Ok(Ok(Ok(count))) => {
                    // Batch completed successfully
                    log::debug!("Batch {} completed: {} sessions persisted", batch_num, count);
                    total_persisted += count;
                    batches_succeeded += 1;
                }
                Ok(Ok(Err(e))) => {
                    // Batch reported failure (unlikely but possible)
                    log::error!("Batch {} reported failure: {}", batch_num, e);
                    batches_failed += 1;
                }
                Ok(Err(_)) => {
                    // oneshot channel dropped - persistence task crashed
                    log::error!("Batch {} completion channel dropped (task crashed?)", batch_num);
                    batches_failed += 1;
                }
                Err(_) => {
                    // Timeout - batch still processing but we can't wait forever
                    log::error!(
                        "Batch {} timeout after {:?} (persistence may still complete)",
                        batch_num, BATCH_TIMEOUT
                    );
                    batches_timeout += 1;
                }
            }
        }

        log::info!(
            "Sequential thinking shutdown: {}/{} sessions persisted, \
             batches: {} succeeded, {} failed, {} timeout",
            total_persisted, sessions_queued,
            batches_succeeded, batches_failed, batches_timeout
        );

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
