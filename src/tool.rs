//! Main tool implementation
//!
//! This module contains the SequentialThinkingTool struct and its implementation
//! of the Tool trait from kodegen_mcp_schema.

use crate::persistence::{start_disk_cleanup_task, start_persistence_processor, try_restore_session};
use crate::sequence_manager::SequenceManager;
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
    /// Key: (connection_id, sequence_id) tuple - allows multiple chains per connection
    /// Value: SessionHandle (contains channel sender + metadata)
    sessions: Arc<DashMap<(String, u64), SessionHandle>>,

    /// Manages sequence lifecycle per connection
    /// Tracks which sequence_id is active for each connection_id
    sequence_manager: Arc<SequenceManager>,

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
            sequence_manager: Arc::new(SequenceManager::new()),
            persistence_sender: persistence_sender.clone(),
        };

        // Start background persistence processor
        start_persistence_processor(persistence_receiver);

        // Start hourly disk cleanup task
        start_disk_cleanup_task(persistence_sender);

        tool
    }

    /// Get or create a session for a connection + thought chain (RACE-FREE)
    ///
    /// Uses SequenceManager to resolve the correct sequence_id based on thought_number.
    /// A new sequence is created when:
    /// 1. No current sequence exists for this connection, OR
    /// 2. `thought_number == 1` (explicit new chain start)
    ///
    /// Returns (composite_session_id, channel_sender) for communication with the session actor.
    /// The composite_session_id format is "{connection_id}_{sequence_id}".
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
        thought_number: u32,
    ) -> Result<(String, u32, tokio::sync::mpsc::Sender<SessionCommand>), McpError> {
        // Resolve sequence_id via SequenceManager
        let (sequence_id, is_new_sequence, previous_sequence) = 
            self.sequence_manager.resolve(connection_id, thought_number);

        // If new sequence, cleanup previous session completely
        if is_new_sequence
            && let Some(prev_seq) = previous_sequence
        {
            let old_key = (connection_id.to_string(), prev_seq);
            if let Some((_, old_handle)) = self.sessions.remove(&old_key) {
                log::debug!(
                    "Cleaned up previous session for connection {} (sequence {})",
                    connection_id, prev_seq
                );
                // Drop handle - actor will terminate when channel closes
                drop(old_handle);
            }
        }

        let key = (connection_id.to_string(), sequence_id);
        let composite_id = format!("{}_{}", connection_id, sequence_id);

        // ========================================================================
        // FAST PATH: Check if session already exists (lock-free read)
        // ========================================================================
        if let Some(entry) = self.sessions.get(&key) {
            // Check if actor is still alive (channel not closed)
            if !entry.value().tx.is_closed() {
                // Update last activity timestamp (lock-free atomic store)
                entry.value().touch();
                return Ok((composite_id, sequence_id as u32, entry.value().tx.clone()));
            }
            // Actor died - remove stale entry and fall through to create new session
            log::debug!("Session {} actor died, removing stale handle", composite_id);
            drop(entry); // Release read lock before removing
            self.sessions.remove(&key);
        }

        // ========================================================================
        // SLOW PATH: Session not in memory, try restore from disk
        // ========================================================================
        // No lock held during async disk I/O - allows concurrent requests
        let maybe_restored = try_restore_session(&composite_id, &self.persistence_sender).await;

        // ========================================================================
        // ATOMIC INSERT: Use entry() API for race-free check-and-insert
        // ========================================================================
        let handle = match self.sessions.entry(key.clone()) {
            Entry::Occupied(entry) => {
                // Another thread created the session while we were doing disk I/O
                // Use their session handle and discard ours (if any)
                log::debug!(
                    "Session {} already created by another thread, using existing",
                    composite_id
                );

                // Update last activity and return existing handle (lock-free)
                entry.get().touch();
                entry.get().clone()
            }

            Entry::Vacant(entry) => {
                // We won the race! Create or use restored session
                let handle = if let Some(restored_handle) = maybe_restored {
                    // Use the restored session from disk
                    log::info!("Restored session {} from disk", composite_id);
                    restored_handle
                } else {
                    // Create brand new session
                    log::debug!("Creating new session {}", composite_id);

                    let (tx, rx) = tokio::sync::mpsc::channel::<SessionCommand>(100);

                    // Spawn session actor task
                    spawn_session_actor(rx);

                    SessionHandle::new(tx)
                };

                // Insert into map atomically (we hold the Entry::Vacant lock)
                entry.insert(handle.clone());
                handle
            }
        };

        Ok((composite_id, sequence_id as u32, handle.tx.clone()))
    }

    /// Cleanup a completed sequence for a connection
    ///
    /// Called when `next_thought_needed == false` to cleanup the sequence_manager
    /// and remove the session from active sessions.
    pub fn cleanup_sequence(&self, connection_id: &str) {
        // Get current sequence before cleanup
        if let Some(seq_id) = self.sequence_manager.current(connection_id) {
            let key = (connection_id.to_string(), seq_id);
            if self.sessions.remove(&key).is_some() {
                log::debug!(
                    "Removed completed session for connection {} (sequence {})",
                    connection_id, seq_id
                );
            }
        }
        // Cleanup sequence_manager state
        self.sequence_manager.cleanup(connection_id);
    }

    /// Get session state snapshot (for debugging or persistence)
    pub async fn get_session_state(
        &self,
        key: &(String, u64),
    ) -> Result<SessionStateSnapshot, McpError> {
        let handle = self
            .sessions
            .get(key)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {:?}", key)))?;

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
    pub async fn clear_session(&self, key: &(String, u64)) -> Result<(), McpError> {
        let handle = self
            .sessions
            .get(key)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {:?}", key)))?;

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
    pub fn get_session_info(&self, key: &(String, u64)) -> Result<(Instant, Instant), McpError> {
        let handle = self
            .sessions
            .get(key)
            .ok_or_else(|| McpError::Other(anyhow::anyhow!("Session not found: {:?}", key)))?;

        let created_at = handle.value().created_at;
        let last_activity = handle.value().last_activity();

        Ok((created_at, last_activity))
    }

    /// Clean up inactive sessions with safe two-pass approach
    ///
    /// Pass 1: Identify expired sessions (DON'T remove yet)
    /// Pass 2: Persist each session, only remove on success
    ///
    /// This prevents data loss: sessions remain in-memory until persistence
    /// is confirmed. Failed sessions retry on the next cleanup cycle.
    async fn cleanup_sessions(&self, max_age: Duration) {
        let purge_cutoff = Instant::now()
            .checked_sub(max_age)
            .unwrap_or_else(Instant::now);

        // ========================================================================
        // PASS 1: Identify expired sessions (collect keys only, DON'T remove)
        // ========================================================================
        let expired_entries: Vec<((String, u64), bool)> = self.sessions.iter()
            .filter_map(|entry| {
                let handle = entry.value();
                
                // Dead sessions (channel closed) can be removed without persistence
                if handle.tx.is_closed() {
                    log::debug!("Session {:?} has closed channel, marking for removal", entry.key());
                    return Some((entry.key().clone(), true)); // (key, is_dead)
                }
                
                // Check last activity timestamp (lock-free via atomic)
                let last_activity = handle.last_activity();
                
                if last_activity < purge_cutoff {
                    log::debug!("Session {:?} expired, marking for persistence", entry.key());
                    return Some((entry.key().clone(), false)); // (key, is_dead)
                }
                
                None // Keep this session
            })
            .collect();

        if expired_entries.is_empty() {
            return;
        }

        log::debug!("Found {} expired sessions to process", expired_entries.len());

        // ========================================================================
        // PASS 2: Process each expired session individually
        // ========================================================================
        let mut removed_count = 0usize;
        let mut retry_count = 0usize;

        for (key, is_dead) in expired_entries {
            if is_dead {
                // Dead session - no data to persist, safe to remove immediately
                if self.sessions.remove(&key).is_some() {
                    log::debug!("Removed dead session: {:?}", key);
                    removed_count += 1;
                }
                continue;
            }

            // Live session - must persist before removal
            let persisted = self.try_persist_session(&key).await;
            
            if persisted {
                // Persistence queued successfully - safe to remove
                if self.sessions.remove(&key).is_some() {
                    log::debug!("Removed persisted session: {:?}", key);
                    removed_count += 1;
                }
            } else {
                // Persistence failed - leave in map for retry next cycle
                log::warn!(
                    "Failed to persist session {:?}, will retry next cleanup cycle",
                    key
                );
                retry_count += 1;
            }
        }

        if removed_count > 0 || retry_count > 0 {
            log::info!(
                "Session cleanup: {} removed, {} deferred for retry",
                removed_count, retry_count
            );
        }
    }

    /// Attempt to persist a session to disk
    ///
    /// Returns `true` if persistence was successfully queued (or session is dead).
    /// Returns `false` if persistence failed and session should remain in memory.
    ///
    /// # Safety
    ///
    /// This method is safe to call concurrently - it uses DashMap's lock-free
    /// operations and drops guards before async operations.
    async fn try_persist_session(&self, key: &(String, u64)) -> bool {
        // Get session handle (may have been removed by another thread)
        let Some(handle_ref) = self.sessions.get(key) else {
            return true; // Already removed by another thread, nothing to do
        };

        // Dead sessions have no data to persist
        if handle_ref.tx.is_closed() {
            return true;
        }

        // Clone what we need and drop the DashMap guard BEFORE async operations
        let tx = handle_ref.tx.clone();
        let created_at_instant = handle_ref.created_at;
        let last_activity_instant = handle_ref.last_activity();
        drop(handle_ref); // Release DashMap guard

        // Get session state via actor command
        let (respond_to, rx) = tokio::sync::oneshot::channel();
        if tx.send(SessionCommand::GetState { respond_to }).await.is_err() {
            // Channel closed while we were processing - session is now dead
            return true;
        }

        let Ok(snapshot) = rx.await else {
            // Actor terminated before responding - session is now dead
            return true;
        };

        // Convert Instant to SystemTime for persistence
        let created_at_elapsed = created_at_instant.elapsed();
        let created_at = std::time::SystemTime::now()
            .checked_sub(created_at_elapsed)
            .unwrap_or_else(std::time::SystemTime::now);

        let last_activity_elapsed = last_activity_instant.elapsed();
        let last_activity = std::time::SystemTime::now()
            .checked_sub(last_activity_elapsed)
            .unwrap_or_else(std::time::SystemTime::now);

        // Build composite session ID
        let composite_id = format!("{}_{}", key.0, key.1);

        // Queue persistence command
        match self.persistence_sender.try_send(PersistenceCommand::Persist {
            session_id: composite_id.clone(),
            snapshot,
            created_at,
            last_activity,
        }) {
            Ok(_) => {
                log::debug!("Queued session {} for persistence", composite_id);
                true // Persistence queued - safe to remove from map
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                log::warn!(
                    "Persistence channel full, deferring session {} to next cycle",
                    composite_id
                );
                false // Don't remove - retry next cycle
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                log::error!(
                    "Persistence channel closed, cannot persist session {}",
                    composite_id
                );
                false // Don't remove - channel may recover after restart
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

        // Get snapshot of all active sessions (as tuple keys)
        let session_keys: Vec<(String, u64)> = self.sessions.iter()
            .map(|entry| entry.key().clone())
            .collect();

        let total_sessions = session_keys.len();
        log::debug!("Found {} active sessions to persist", total_sessions);

        if total_sessions == 0 {
            log::info!("No sessions to persist, shutdown complete");
            return Ok(());
        }

        // Collect completion receivers for all batches
        let mut batch_receivers: Vec<tokio::sync::oneshot::Receiver<Result<usize, String>>> = Vec::new();
        let mut current_batch: Vec<(String, SessionStateSnapshot, std::time::SystemTime, std::time::SystemTime)> = Vec::new();
        let mut sessions_queued = 0usize;

        for key in session_keys {
            // Convert tuple key to composite session_id string
            let composite_id = format!("{}_{}", key.0, key.1);
            
            // Get session state
            if let Ok(snapshot) = self.get_session_state(&key).await {
                // Get session handle for timestamps
                if let Some(handle) = self.sessions.get(&key) {
                    // Convert Instant to SystemTime
                    let created_at_elapsed = handle.created_at.elapsed();
                    let created_at = std::time::SystemTime::now()
                        .checked_sub(created_at_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    let last_activity_instant = handle.last_activity();
                    let last_activity_elapsed = last_activity_instant.elapsed();
                    let last_activity = std::time::SystemTime::now()
                        .checked_sub(last_activity_elapsed)
                        .unwrap_or_else(std::time::SystemTime::now);

                    // Add to current batch
                    current_batch.push((composite_id.clone(), snapshot, created_at, last_activity));
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
