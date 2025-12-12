//! Disk persistence for session state
//!
//! This module handles saving and restoring sessions to/from disk,
//! along with cleanup of old sessions.

use crate::session::spawn_session_actor_with_state;
use crate::types::{
    PersistedSessionFile, PersistenceCommand, PersistenceConfig, SessionCommand, SessionHandle,
    SessionStateSnapshot, ThinkingState,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// SECURITY FUNCTIONS
// ============================================================================

/// Sanitize session_id to prevent directory traversal (CWE-22)
fn sanitize_session_id(session_id: &str) -> Result<String, anyhow::Error> {
    // Character whitelist: alphanumeric + dash + underscore only
    if !session_id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        anyhow::bail!(
            "Invalid session_id '{}': must contain only alphanumeric characters, hyphens, and underscores",
            session_id
        );
    }

    if session_id.is_empty() {
        anyhow::bail!("session_id cannot be empty");
    }

    if session_id.len() > 255 {
        anyhow::bail!("session_id too long (max 255 characters, got {})", session_id.len());
    }

    Ok(session_id.to_string())
}

/// Verify path stays within allowed base directory (defense-in-depth)
async fn verify_path_within_base(
    constructed_path: &std::path::Path,
    allowed_base: &std::path::Path,
) -> Result<std::path::PathBuf, anyhow::Error> {
    use anyhow::Context;

    // Ensure base exists
    if !tokio::fs::try_exists(allowed_base).await.unwrap_or(false) {
        tokio::fs::create_dir_all(allowed_base)
            .await
            .context("Failed to create sessions base directory")?;
    }

    // Canonicalize base
    let canonical_base = allowed_base
        .canonicalize()
        .context("Failed to canonicalize base directory")?;

    // Handle non-existent paths by validating parent
    let canonical_path = if tokio::fs::try_exists(constructed_path).await.unwrap_or(false) {
        constructed_path
            .canonicalize()
            .context("Failed to canonicalize path")?
    } else {
        // Path doesn't exist - validate parent + append filename
        if let Some(parent) = constructed_path.parent() {
            let canonical_parent = if tokio::fs::try_exists(parent).await.unwrap_or(false) {
                parent.canonicalize().context("Failed to canonicalize parent")?
            } else {
                // Parent doesn't exist either - just verify it starts with base
                if !parent.starts_with(&canonical_base) {
                    anyhow::bail!(
                        "Directory traversal detected: parent '{}' escapes base '{}'",
                        parent.display(),
                        canonical_base.display()
                    );
                }
                canonical_base.clone()
            };

            if let Some(filename) = constructed_path.file_name() {
                canonical_parent.join(filename)
            } else {
                anyhow::bail!("Path has no filename");
            }
        } else {
            anyhow::bail!("Path has no parent directory");
        }
    };

    // Verify within base
    if !canonical_path.starts_with(&canonical_base) {
        anyhow::bail!(
            "Directory traversal detected: '{}' escapes base '{}'",
            canonical_path.display(),
            canonical_base.display()
        );
    }

    Ok(canonical_path)
}

// ============================================================================
// PERSISTENCE BACKGROUND TASK
// ============================================================================

/// Start background task to handle persistence commands
pub fn start_persistence_processor(
    mut receiver: tokio::sync::mpsc::Receiver<PersistenceCommand>,
) {
    let config = PersistenceConfig::default();

    tokio::spawn(async move {
        // Create base directory once
        if let Err(e) = tokio::fs::create_dir_all(&config.sessions_dir).await {
            log::error!("Failed to create sessions directory: {e}");
        }

        // Process commands until channel closes
        while let Some(cmd) = receiver.recv().await {
            match cmd {
                PersistenceCommand::Persist {
                    session_id,
                    snapshot,
                    created_at,
                    last_activity,
                } => {
                    if let Err(e) = persist_session_to_disk(
                        &config,
                        &session_id,
                        &snapshot,
                        created_at,
                        last_activity,
                    )
                    .await
                    {
                        log::error!("Failed to persist session {session_id}: {e}");
                    }
                }

                PersistenceCommand::PersistBatch { sessions, completion } => {
                    let batch_size = sessions.len();
                    log::info!("Processing batch of {} sessions", batch_size);
                    
                    let mut success_count = 0usize;
                    let mut failure_count = 0usize;
                    
                    for (session_id, snapshot, created_at, last_activity) in sessions {
                        match persist_session_to_disk(
                            &config,
                            &session_id,
                            &snapshot,
                            created_at,
                            last_activity,
                        )
                        .await
                        {
                            Ok(()) => {
                                success_count += 1;
                            }
                            Err(e) => {
                                log::error!("Failed to persist session {session_id} in batch: {e}");
                                failure_count += 1;
                                // Continue processing other sessions in batch
                            }
                        }
                    }
                    
                    log::debug!(
                        "Batch persistence complete: {}/{} succeeded, {} failed",
                        success_count, batch_size, failure_count
                    );
                    
                    // Send completion signal if caller requested it
                    // Use `let _ =` because receiver may have timed out and dropped
                    if let Some(tx) = completion {
                        let _ = tx.send(Ok(success_count));
                    }
                }

                PersistenceCommand::Delete { session_id } => {
                    // SECURITY: Sanitize session_id to prevent directory traversal (CWE-22)
                    let safe_session_id = match sanitize_session_id(&session_id) {
                        Ok(id) => id,
                        Err(e) => {
                            log::error!("Invalid session_id for deletion: {e}");
                            continue;
                        }
                    };

                    let session_dir = config.sessions_dir.join(&safe_session_id);

                    // SECURITY: Verify path is within sessions_dir before deletion
                    let verified_dir = match verify_path_within_base(&session_dir, &config.sessions_dir).await {
                        Ok(path) => path,
                        Err(e) => {
                            log::error!("Path verification failed during deletion: {e}");
                            continue;
                        }
                    };

                    if let Err(e) = tokio::fs::remove_dir_all(&verified_dir).await {
                        log::debug!("Failed to delete session directory {session_id}: {e}");
                    } else {
                        log::info!("Deleted persisted session: {session_id}");
                    }
                }
            }
        }

        log::debug!("Persistence processor terminated");
    });
}

/// Persist a single session to disk with atomic write guarantees
///
/// Uses write-to-temp-then-rename pattern to ensure either:
/// - Old session file remains intact (crash before rename), OR
/// - New session file is complete and valid (crash after rename)
///
/// Never leaves partial/corrupted files on disk.
async fn persist_session_to_disk(
    config: &PersistenceConfig,
    session_id: &str,
    snapshot: &SessionStateSnapshot,
    created_at: std::time::SystemTime,
    last_activity: std::time::SystemTime,
) -> Result<(), anyhow::Error> {
    use anyhow::Context;
    use tokio::io::AsyncWriteExt;

    // SECURITY: Sanitize session_id to prevent directory traversal (CWE-22)
    let safe_session_id = sanitize_session_id(session_id)
        .context("session_id validation failed")?;

    // Create session directory: {sessions_dir}/{safe-session-id}/
    let session_dir = config.sessions_dir.join(&safe_session_id);

    // SECURITY: Verify path is within sessions_dir (defense-in-depth)
    let verified_session_dir = verify_path_within_base(&session_dir, &config.sessions_dir)
        .await
        .context("Path verification failed - potential directory traversal")?;

    tokio::fs::create_dir_all(&verified_session_dir)
        .await
        .context("Failed to create session directory")?;

    // Use verified_session_dir for all subsequent operations
    let session_dir = verified_session_dir;

    // Build unified session file structure
    let session_file = PersistedSessionFile::from_snapshot(
        session_id.to_string(),
        snapshot,
        created_at,
        last_activity,
    );

    // Serialize to pretty JSON (for debugging/manual inspection)
    let json = serde_json::to_string_pretty(&session_file)
        .context("Failed to serialize session data")?;

    // Define final and temporary file paths
    let final_path = session_dir.join("session.json");
    let temp_path = final_path.with_extension("json.tmp");

    // ATOMIC WRITE STEP 1: Write to temporary file
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .context("Failed to create temporary session file")?;

    file.write_all(json.as_bytes())
        .await
        .context("Failed to write session data to temp file")?;

    // ATOMIC WRITE STEP 2: Sync to physical disk
    // Ensures data is durable before we commit the rename
    file.sync_all()
        .await
        .context("Failed to sync session file to disk")?;

    // Drop the file handle before rename (required on some platforms)
    drop(file);

    // ATOMIC WRITE STEP 3: Atomic rename
    // On POSIX systems (macOS, Linux), this is a single atomic operation
    tokio::fs::rename(&temp_path, &final_path)
        .await
        .context("Failed to atomically commit session file")?;

    log::info!(
        "Persisted session {} ({} thoughts, {} branches) atomically to {:?}",
        session_id,
        snapshot.thought_history.len(),
        snapshot.branches.len(),
        final_path
    );

    Ok(())
}

// ============================================================================
// SESSION RESTORATION
// ============================================================================

/// Attempt to restore session from disk
///
/// Reads the atomic session file and reconstructs the session actor.
/// Returns None if session doesn't exist or restoration fails.
///
/// Note: Ignores .tmp files (incomplete writes from crashes)
pub async fn try_restore_session(
    session_id: &str,
    persistence_sender: &tokio::sync::mpsc::Sender<PersistenceCommand>,
) -> Option<SessionHandle> {
    let config = PersistenceConfig::default();

    // SECURITY: Sanitize session_id to prevent directory traversal (CWE-22)
    let safe_session_id = match sanitize_session_id(session_id) {
        Ok(id) => id,
        Err(e) => {
            log::warn!("Invalid session_id for restoration: {e}");
            return None;
        }
    };

    let session_dir = config.sessions_dir.join(&safe_session_id);

    // SECURITY: Verify path is within sessions_dir (defense-in-depth)
    let session_dir = match verify_path_within_base(&session_dir, &config.sessions_dir).await {
        Ok(path) => path,
        Err(e) => {
            log::warn!("Path verification failed during restoration: {e}");
            return None;
        }
    };

    let session_file = session_dir.join("session.json");

    // Check if session file exists
    if !tokio::fs::try_exists(&session_file).await.unwrap_or(false) {
        log::debug!("Session file not found for {session_id}");
        return None;
    }

    log::debug!("Attempting to restore session {session_id} from {:?}", session_file);

    // Read and parse session file atomically
    let contents = tokio::fs::read_to_string(&session_file).await.ok()?;
    let persisted: PersistedSessionFile = match serde_json::from_str(&contents) {
        Ok(p) => p,
        Err(e) => {
            log::error!("Failed to parse session file for {session_id}: {e}");
            // Clean up corrupted file
            let _ = tokio::fs::remove_dir_all(&session_dir).await;
            return None;
        }
    };

    // Validate restored data
    if persisted.thought_history.is_empty() && persisted.branches.is_empty() {
        log::warn!("Restored session {session_id} has no thoughts, ignoring");
        return None;
    }

    log::info!(
        "Restored session {} ({} thoughts, {} branches) from disk",
        session_id,
        persisted.thought_history.len(),
        persisted.branches.len()
    );

    // Convert to snapshot and spawn session actor
    let snapshot = persisted.to_snapshot();
    let (tx, rx) = tokio::sync::mpsc::channel(100);

    // Create restored state
    let restored_state = ThinkingState {
        thought_history: snapshot.thought_history,
        branches: snapshot.branches,
    };

    spawn_session_actor_with_state(rx, restored_state);

    // Calculate original timestamps from persisted metadata
    let created_at_elapsed = persisted.created_at.elapsed().ok()?;
    let created_at = Instant::now()
        .checked_sub(created_at_elapsed)
        .unwrap_or_else(Instant::now);

    let handle = SessionHandle {
        tx: tx.clone(),  // ← Clone tx for verification (needs to be reusable)
        created_at,
        last_activity: Arc::new(RwLock::new(Instant::now())),
    };

    // VERIFY ACTOR IS RESPONSIVE before deleting files
    // This prevents data loss if actor fails immediately after spawn
    let (verify_tx, verify_rx) = tokio::sync::oneshot::channel();
    let verify_cmd = SessionCommand::GetState { respond_to: verify_tx };

    // Use timeout to prevent indefinite blocking
    // 5 seconds is generous - healthy actor responds in milliseconds
    match tokio::time::timeout(Duration::from_secs(5), async {
        // Send verification command to actor
        if handle.tx.send(verify_cmd).await.is_err() {
            // Channel closed - actor already dead
            return None;
        }
        // Wait for actor's response
        verify_rx.await.ok()
    }).await {
        Ok(Some(_snapshot)) => {
            // Actor verified - safe to delete disk backup
            log::info!("Restored session {} verified, deleting disk backup", session_id);
            // Use try_send - if channel is full, files stay on disk (will be cleaned up later)
            let _ = persistence_sender.try_send(PersistenceCommand::Delete {
                session_id: session_id.to_string(),
            });
            Some(handle)
        }
        _ => {
            // Verification failed (timeout, send error, or channel closed)
            // Keep disk files for next restore attempt
            log::error!(
                "Restored session {} failed verification, preserving disk backup for retry",
                session_id
            );
            None
        }
    }
}

// ============================================================================
// CLEANUP TASKS
// ============================================================================

/// Start background task to clean up old disk sessions (runs hourly)
pub fn start_disk_cleanup_task(
    persistence_sender: tokio::sync::mpsc::Sender<PersistenceCommand>,
) {
    tokio::spawn(async move {
        let config = PersistenceConfig::default();
        let mut interval = tokio::time::interval(Duration::from_secs(60 * 60)); // 1 hour

        loop {
            interval.tick().await;

            log::debug!("Running disk cleanup task");

            // Read all session directories
            let Ok(mut entries) = tokio::fs::read_dir(&config.sessions_dir).await else {
                continue;
            };

            while let Ok(Some(entry)) = entries.next_entry().await {
                // Only process directories (session directories)
                let Ok(file_type) = entry.file_type().await else {
                    continue;
                };

                if !file_type.is_dir() {
                    continue;
                }

                let path = entry.path();

                // Read session.json to check age (now uses PersistedSessionFile)
                let session_file = path.join("session.json");
                let Ok(session_json) = tokio::fs::read_to_string(&session_file).await else {
                    continue;
                };

                let Ok(session) = serde_json::from_str::<PersistedSessionFile>(&session_json)
                else {
                    // Corrupted file - schedule for deletion immediately
                    if let Some(session_id) = path.file_name().and_then(|n| n.to_str()) {
                        log::warn!("Found corrupted session file {session_id}, scheduling cleanup");
                        let _ = persistence_sender.try_send(PersistenceCommand::Delete {
                            session_id: session_id.to_string(),
                        });
                    }
                    continue;
                };

                // Check if session is older than cleanup threshold
                let age = session
                    .last_activity
                    .elapsed()
                    .unwrap_or_else(|_| Duration::from_secs(0));

                if age > config.cleanup_after {
                    // Use try_send for non-critical disk cleanup
                    match persistence_sender.try_send(PersistenceCommand::Delete {
                        session_id: session.session_id.clone(),
                    }) {
                        Ok(_) => {
                            log::info!(
                                "Queued old session {} for deletion (age: {:.1} hours)",
                                session.session_id,
                                age.as_secs_f64() / 3600.0
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            log::warn!(
                                "Persistence channel full, deferring deletion of session {}",
                                session.session_id
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            log::error!("Persistence channel closed, cannot delete session {}", session.session_id);
                        }
                    }
                }
            }
        }
    });
}
