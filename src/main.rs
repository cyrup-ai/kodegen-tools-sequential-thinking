// Category HTTP Server: Sequential Thinking
//
// This binary serves the sequential thinking tool over HTTP/HTTPS transport.
// Managed by kodegend daemon, typically running on port kodegen_config::PORT_REASONING (30450).

use anyhow::Result;
use kodegen_config::CATEGORY_SEQUENTIAL_THINKING;
use kodegen_server_http::{ServerBuilder, Managers, RouterSet, register_tool};
use rmcp::handler::server::router::{prompt::PromptRouter, tool::ToolRouter};
use std::sync::Arc;

// Import the reusable shutdown hook from lib
use kodegen_tools_sequential_thinking::{
    SequentialThinkingTool,
    SequentialThinkingShutdownHook,
};

#[tokio::main]
async fn main() -> Result<()> {
    ServerBuilder::new()
        .category(CATEGORY_SEQUENTIAL_THINKING)
        .register_tools(|| async {
            let mut tool_router = ToolRouter::new();
            let mut prompt_router = PromptRouter::new();
            let managers = Managers::new();

            // Create sequential thinking tool
            let tool = SequentialThinkingTool::new();

            // Wrap in Arc and start cleanup task (required for session management)
            let tool_arc = Arc::new(tool.clone());
            tool_arc.clone().start_cleanup_task();

            // Register shutdown hook to persist active sessions on exit
            managers.register(SequentialThinkingShutdownHook(tool_arc)).await;

            // Register the tool (1 tool)
            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                tool,
            );

            Ok(RouterSet::new(tool_router, prompt_router, managers))
        })
        .run()
        .await
}
