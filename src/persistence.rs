//! Disk persistence for session state
//!
//! This module handles saving and restoring sessions to/from disk,
//! along with cleanup of old sessions.

use crate::session::spawn_session_actor_with_state;
use crate::types::{
    PersistedThought, PersistenceCommand, PersistenceConfig, SessionHandle, SessionMetadataFile,
    SessionStateSnapshot, ThinkingState,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ============================================================================
// PERSISTENCE BACKGROUND TASK
// ============================================================================

/// Start background task to handle persistence commands
pub fn start_persistence_processor(
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<PersistenceCommand>,
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

                PersistenceCommand::Delete { session_id } => {
                    let session_dir = config.sessions_dir.join(&session_id);
                    if let Err(e) = tokio::fs::remove_dir_all(&session_dir).await {
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

/// Persist a single session to disk (called by background task)
async fn persist_session_to_disk(
    config: &PersistenceConfig,
    session_id: &str,
    snapshot: &SessionStateSnapshot,
    created_at: std::time::SystemTime,
    last_activity: std::time::SystemTime,
) -> Result<(), anyhow::Error> {
    use anyhow::Context;

    // Create session directory: {sessions_dir}/{session-id}/
    let session_dir = config.sessions_dir.join(session_id);
    tokio::fs::create_dir_all(&session_dir)
        .await
        .context("Failed to create session directory")?;

    // Write session metadata file
    let metadata = SessionMetadataFile {
        session_id: session_id.to_string(),
        created_at,
        last_activity,
        total_thoughts: snapshot.thought_history.len(),
        branch_ids: snapshot.branches.keys().cloned().collect(),
    };
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    tokio::fs::write(session_dir.join("session.json"), metadata_json)
        .await
        .context("Failed to write session.json")?;

    // Write individual thought files: thought1.json, thought2.json, ...
    for (idx, thought) in snapshot.thought_history.iter().enumerate() {
        let persisted = PersistedThought {
            thought_number: thought.thought_number,
            thought_data: thought.clone(),
        };
        let thought_json = serde_json::to_string_pretty(&persisted)?;
        let thought_path = session_dir.join(format!("thought{}.json", idx + 1));
        tokio::fs::write(thought_path, thought_json)
            .await
            .with_context(|| format!("Failed to write thought{}.json", idx + 1))?;
    }

    // Write branch files: branch_{branch_id}_thought{n}.json
    for (branch_id, branch_thoughts) in &snapshot.branches {
        for (idx, thought) in branch_thoughts.iter().enumerate() {
            let persisted = PersistedThought {
                thought_number: thought.thought_number,
                thought_data: thought.clone(),
            };
            let thought_json = serde_json::to_string_pretty(&persisted)?;
            let branch_path =
                session_dir.join(format!("branch_{}_thought{}.json", branch_id, idx + 1));
            tokio::fs::write(branch_path, thought_json)
                .await
                .with_context(|| format!("Failed to write branch file for {branch_id}"))?;
        }
    }

    log::info!(
        "Persisted session {} ({} thoughts) to {:?}",
        session_id,
        snapshot.thought_history.len(),
        session_dir
    );

    Ok(())
}

// ============================================================================
// SESSION RESTORATION
// ============================================================================

/// Attempt to restore session from disk
/// Returns None if session doesn't exist on disk or restoration fails
pub async fn try_restore_session(
    session_id: &str,
    persistence_sender: &tokio::sync::mpsc::UnboundedSender<PersistenceCommand>,
) -> Option<SessionHandle> {
    let config = PersistenceConfig::default();
    let session_dir = config.sessions_dir.join(session_id);

    // Check if session directory exists (async)
    if !tokio::fs::try_exists(&session_dir).await.unwrap_or(false) {
        return None;
    }

    log::debug!("Attempting to restore session {session_id} from disk");

    // Read session metadata
    let metadata_path = session_dir.join("session.json");
    let metadata_json = tokio::fs::read_to_string(metadata_path).await.ok()?;
    let metadata: SessionMetadataFile = serde_json::from_str(&metadata_json).ok()?;

    // Read all thought files in order
    let mut thought_history = Vec::new();
    for idx in 1..=metadata.total_thoughts {
        let thought_path = session_dir.join(format!("thought{idx}.json"));
        if let Ok(thought_json) = tokio::fs::read_to_string(thought_path).await
            && let Ok(persisted) = serde_json::from_str::<PersistedThought>(&thought_json)
        {
            thought_history.push(persisted.thought_data);
        }
    }

    // Read branch files
    let mut branches = HashMap::new();
    for branch_id in &metadata.branch_ids {
        let mut branch_thoughts = Vec::new();
        let mut idx = 1;
        loop {
            let branch_path = session_dir.join(format!("branch_{branch_id}_thought{idx}.json"));
            match tokio::fs::read_to_string(branch_path).await {
                Ok(thought_json) => {
                    if let Ok(persisted) =
                        serde_json::from_str::<PersistedThought>(&thought_json)
                    {
                        branch_thoughts.push(persisted.thought_data);
                        idx += 1;
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        if !branch_thoughts.is_empty() {
            branches.insert(branch_id.clone(), branch_thoughts);
        }
    }

    log::info!(
        "Restored session {} ({} thoughts, {} branches) from disk",
        session_id,
        thought_history.len(),
        branches.len()
    );

    // Create session actor with restored state
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let restored_state = ThinkingState {
        thought_history,
        branches,
    };
    spawn_session_actor_with_state(rx, restored_state);

    // Calculate original timestamps from metadata
    let created_at_elapsed = metadata.created_at.elapsed().ok()?;
    let created_at = Instant::now()
        .checked_sub(created_at_elapsed)
        .unwrap_or_else(Instant::now);

    let handle = SessionHandle {
        tx,
        created_at,
        last_activity: Arc::new(RwLock::new(Instant::now())), // Reset activity time
    };

    // Delete disk files after successful restoration (session is active again)
    let _ = persistence_sender.send(PersistenceCommand::Delete {
        session_id: session_id.to_string(),
    });

    Some(handle)
}

// ============================================================================
// CLEANUP TASKS
// ============================================================================

/// Start background task to clean up old disk sessions (runs hourly)
pub fn start_disk_cleanup_task(
    persistence_sender: tokio::sync::mpsc::UnboundedSender<PersistenceCommand>,
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

                // Read session.json to check age
                let metadata_path = path.join("session.json");
                let Ok(metadata_json) = tokio::fs::read_to_string(metadata_path).await else {
                    continue;
                };

                let Ok(metadata) = serde_json::from_str::<SessionMetadataFile>(&metadata_json)
                else {
                    continue;
                };

                // Check if session is older than cleanup threshold
                let age = metadata
                    .last_activity
                    .elapsed()
                    .unwrap_or_else(|_| Duration::from_secs(0));

                if age > config.cleanup_after {
                    // Send delete command to persistence task
                    log::info!(
                        "Purging old session {} (age: {:.1} hours)",
                        metadata.session_id,
                        age.as_secs_f64() / 3600.0
                    );

                    let _ = persistence_sender.send(PersistenceCommand::Delete {
                        session_id: metadata.session_id,
                    });
                }
            }
        }
    });
}
