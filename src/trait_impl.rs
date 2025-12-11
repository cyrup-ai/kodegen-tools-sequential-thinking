//! Tool trait implementation
//!
//! This module contains the implementation of the Tool trait for SequentialThinkingTool
//! and the server shutdown hook.

use crate::string_utils::safe_truncate_word_boundary;
use crate::tool::SequentialThinkingTool;
use crate::types::SessionCommand;
use kodegen_mcp_schema::sequential_thinking::{SequentialThinkingArgs, SequentialThinkingOutput, SequentialThinkingPrompts};
use kodegen_mcp_schema::McpError;
use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse};

// ============================================================================
// TOOL IMPLEMENTATION
// ============================================================================

impl Tool for SequentialThinkingTool {
    type Args = SequentialThinkingArgs;
    type Prompts = SequentialThinkingPrompts;

    fn name() -> &'static str {
        kodegen_mcp_schema::sequential_thinking::SEQUENTIAL_THINKING
    }

    fn description() -> &'static str {
        "A detailed tool for dynamic and reflective problem-solving through thoughts.\n\
         This tool helps analyze problems through a flexible thinking process that can adapt and evolve.\n\
         Each thought can build on, question, or revise previous insights as understanding deepens.\n\n\
         When to use this tool:\n\
         - Breaking down complex problems into steps\n\
         - Planning and design with room for revision\n\
         - Analysis that might need course correction\n\
         - Problems where the full scope might not be clear initially\n\
         - Problems that require a multi-step solution\n\
         - Tasks that need to maintain context over multiple steps\n\
         - Situations where irrelevant information needs to be filtered out\n\n\
         Key features:\n\
         - Adjust total_thoughts up or down as you progress\n\
         - Question or revise previous thoughts\n\
         - Add more thoughts even after reaching what seemed like the end\n\
         - Express uncertainty and explore alternative approaches\n\
         - Branch or backtrack (not every thought needs to build linearly)\n\
         - Generate and verify solution hypotheses\n\
         - Repeat the process until satisfied"
    }

    fn read_only() -> bool {
        true // Only tracks internal state, doesn't modify external resources
    }

    async fn execute(&self, args: Self::Args, ctx: ToolExecutionContext) -> Result<ToolResponse<<Self::Args as kodegen_mcp_schema::ToolArgs>::Output>, McpError> {
        // Validate and convert args
        let thought_data = Self::validate_thought(args.clone());

        // Get connection_id from context (same pattern as terminal tool)
        let connection_id = ctx.connection_id().unwrap_or("default");

        // Get or create session using connection_id
        let (session_id, tx) = self.get_or_create_session(connection_id).await?;

        // Create response channel
        let (respond_to, rx) = tokio::sync::oneshot::channel();

        // Send command to session actor
        let cmd = SessionCommand::AddThought {
            thought: thought_data.clone(),
            respond_to,
        };

        tx.send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        // Wait for response
        let response = rx
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor failed to respond")))?;

        // Build display: thought only, truncated at 200 chars, light grey
        let display = {
            let thought = &thought_data.thought;
            let truncated = if thought.chars().count() > 200 {
                let truncate_at = safe_truncate_word_boundary(thought, 200);
                format!("{}...", &thought[..truncate_at])
            } else {
                thought.clone()
            };
            // LIGHT GREY ANSI 247 (RGB 158,158,158 ≈ #9e9e9e)
            format!("\x1b[38;5;247m{}\x1b[0m", truncated)
        };

        // Build typed output
        let output = SequentialThinkingOutput {
            session_id,
            thought_number: response.thought_number,
            total_thoughts: response.total_thoughts,
            thought: thought_data.thought,
            next_thought_needed: response.next_thought_needed,
            is_revision: args.is_revision,
            revises_thought: args.revises_thought,
            branch_id: args.branch_id,
            branch_from_thought: args.branch_from_thought,
            branches: response.branches,
            thought_history_length: response.thought_history_length,
        };

        Ok(ToolResponse::new(display, output))
    }

}

// ============================================================================
// SHUTDOWN HOOK FOR MCP SERVER
// ============================================================================

#[cfg(feature = "server")]
use kodegen_server_http::ShutdownHook;

#[cfg(feature = "server")]
impl ShutdownHook for SequentialThinkingTool {
    fn shutdown(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send + '_>> {
        Box::pin(async move {
            SequentialThinkingTool::shutdown(self).await
                .map_err(|e| anyhow::anyhow!("Failed to shutdown sequential thinking tool: {e}"))
        })
    }
}
