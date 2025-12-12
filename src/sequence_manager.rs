//! Sequence lifecycle management
//!
//! Manages the mapping of connection_id → sequence_id for thought chains.
//!
//! ## Hierarchy
//!
//! ```text
//! connection_id (1 per agent session)
//!   └── sequence_id (many thought chains per connection)
//!         └── thought_number (many thoughts per chain: 1, 2, 3...)
//! ```

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Manages sequence lifecycle per connection
///
/// Each connection can have multiple thought sequences over time.
/// This manager tracks which sequence is currently active for each connection
/// and handles generation of new sequence IDs.
#[derive(Debug)]
pub struct SequenceManager {
    /// Current active sequence_id per connection_id
    current_sequence: DashMap<String, u64>,

    /// Counter for generating unique sequence IDs
    next_id: AtomicU64,
}

impl SequenceManager {
    /// Create a new SequenceManager
    #[must_use]
    pub fn new() -> Self {
        Self {
            current_sequence: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Resolve sequence_id for a thought
    ///
    /// A new sequence is created when:
    /// 1. No current sequence exists for this connection, OR
    /// 2. `thought_number == 1` (explicit new chain start)
    ///
    /// Returns `(sequence_id, is_new_sequence, previous_sequence_id)`
    ///
    /// ## Thread Safety
    ///
    /// This method is safe for concurrent access. The hot path (continuation thoughts)
    /// uses lock-free `get()` with no allocation. Only `thought_number == 1` and the
    /// rare edge case of missing sequence perform allocations.
    pub fn resolve(&self, connection_id: &str, thought_number: u32) -> (u64, bool, Option<u64>) {

        // Case 1: thought_number == 1 ALWAYS starts a new sequence
        // This is an explicit "start fresh" signal from the agent
        if thought_number == 1 {
            // Get previous sequence atomically via remove-and-return
            let previous = self.current_sequence.remove(connection_id).map(|(_, v)| v);
            
            // Generate new sequence ID
            let new_seq = self.next_id.fetch_add(1, Ordering::Relaxed);
            
            // Insert new sequence
            self.current_sequence.insert(connection_id.to_string(), new_seq);

            log::debug!(
                "New sequence {} for connection {} (thought_number=1, previous={:?})",
                new_seq,
                connection_id,
                previous
            );

            return (new_seq, true, previous);
        }

        // Case 2: Fast path - check existing sequence WITHOUT allocation
        if let Some(entry) = self.current_sequence.get(connection_id) {
            let seq_id = *entry;
            return (seq_id, false, None);
        }

        // Case 3: Slow path - need to create new sequence (rare edge case)
        // This handles the case where thought_number > 1 arrives but no sequence
        // exists (e.g., after server restart). Only allocate when we need to insert.
        let new_seq = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.current_sequence.insert(connection_id.to_string(), new_seq);

        log::debug!(
            "New sequence {} for connection {} (thought_number={}, no previous)",
            new_seq,
            connection_id,
            thought_number
        );

        (new_seq, true, None)
    }

    /// Get current sequence_id for a connection (if exists)
    #[must_use]
    pub fn current(&self, connection_id: &str) -> Option<u64> {
        self.current_sequence.get(connection_id).map(|r| *r)
    }

    /// Cleanup sequence for a connection (called when chain completes)
    pub fn cleanup(&self, connection_id: &str) {
        if let Some((_, seq_id)) = self.current_sequence.remove(connection_id) {
            log::debug!("Cleaned up sequence {} for connection {}", seq_id, connection_id);
        }
    }
}

impl Default for SequenceManager {
    fn default() -> Self {
        Self::new()
    }
}
