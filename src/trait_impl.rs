//! Tool trait implementation
//!
//! This module contains the implementation of the Tool trait for SequentialThinkingTool
//! and the server shutdown hook.

use crate::tool::SequentialThinkingTool;
use crate::types::SessionCommand;
use kodegen_mcp_schema::reasoning::{SequentialThinkingArgs, SequentialThinkingPromptArgs};
use kodegen_mcp_tool::error::McpError;
use kodegen_mcp_tool::Tool;
use rmcp::model::{PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
use serde_json::{json, Value};

// ============================================================================
// TOOL IMPLEMENTATION
// ============================================================================

impl Tool for SequentialThinkingTool {
    type Args = SequentialThinkingArgs;
    type PromptArgs = SequentialThinkingPromptArgs;

    fn name() -> &'static str {
        "sequential_thinking"
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

    async fn execute(&self, args: Self::Args) -> Result<Value, McpError> {
        // Validate and convert args
        let thought_data = Self::validate_thought(args.clone());

        // Get or create session
        let (session_id, tx) = self.get_or_create_session(args.session_id).await?;

        // Create response channel
        let (respond_to, rx) = tokio::sync::oneshot::channel();

        // Send command to session actor
        let cmd = SessionCommand::AddThought {
            thought: thought_data,
            respond_to,
        };

        tx.send(cmd)
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor terminated")))?;

        // Wait for response
        let response = rx
            .await
            .map_err(|_| McpError::Other(anyhow::anyhow!("Session actor failed to respond")))?;

        // Build JSON response with session ID (snake_case)
        Ok(json!({
            "session_id": session_id,
            "thought_number": response.thought_number,
            "total_thoughts": response.total_thoughts,
            "next_thought_needed": response.next_thought_needed,
            "branches": response.branches,
            "thought_history_length": response.thought_history_length
        }))
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![] // No arguments needed for teaching prompt
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I use the sequential_thinking tool to solve a complex problem?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(
                    "The sequential_thinking tool helps you break down complex problems step by step:\n\n\
                     1. Start with initial estimate:\n\
                     sequential_thinking({\n\
                       \"thought\": \"First, I need to understand the problem scope\",\n\
                       \"thought_number\": 1,\n\
                       \"total_thoughts\": 5,\n\
                       \"next_thought_needed\": true\n\
                     })\n\n\
                     2. Continue building:\n\
                     sequential_thinking({\n\
                       \"thought\": \"Now analyzing the core requirements\",\n\
                       \"thought_number\": 2,\n\
                       \"total_thoughts\": 5,\n\
                       \"next_thought_needed\": true\n\
                     })\n\n\
                     3. Revise if needed:\n\
                     sequential_thinking({\n\
                       \"thought\": \"Wait, I need to reconsider my approach\",\n\
                       \"thought_number\": 3,\n\
                       \"total_thoughts\": 6,\n\
                       \"is_revision\": true,\n\
                       \"revises_thought\": 2,\n\
                       \"next_thought_needed\": true\n\
                     })\n\n\
                     4. Branch to explore alternatives:\n\
                     sequential_thinking({\n\
                       \"thought\": \"Alternative approach using pattern X\",\n\
                       \"thought_number\": 4,\n\
                       \"total_thoughts\": 6,\n\
                       \"branch_from_thought\": 2,\n\
                       \"branch_id\": \"alt-pattern-x\",\n\
                       \"next_thought_needed\": true\n\
                     })\n\n\
                     5. Conclude:\n\
                     sequential_thinking({\n\
                       \"thought\": \"Final solution: implement approach Y\",\n\
                       \"thought_number\": 6,\n\
                       \"total_thoughts\": 6,\n\
                       \"next_thought_needed\": false\n\
                     })\n\n\
                     The tool tracks your entire thinking process, allowing you to:\n\
                     - Adjust total_thoughts dynamically as you learn more\n\
                     - Revise earlier thoughts when you discover new information\n\
                     - Branch to explore multiple solution paths\n\
                     - See your complete thought history across all invocations",
                ),
            },
        ])
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
