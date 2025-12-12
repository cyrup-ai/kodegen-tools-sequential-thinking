//! Sequential thinking tool for MCP

mod types;
mod session;
mod persistence;
mod string_utils;
pub mod tool;
mod trait_impl;

pub use tool::SequentialThinkingTool;
pub use types::{SessionStateSnapshot, ThoughtData};

use anyhow::Result;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

/// Shutdown hook wrapper for Arc<SequentialThinkingTool>
///
/// This newtype wrapper implements the ShutdownHook trait for Arc<SequentialThinkingTool>.
/// It's necessary because:
/// - `start_cleanup_task()` requires `Arc<Self>` for background task spawning
/// - `ShutdownHook` is implemented on `SequentialThinkingTool`, not `Arc<SequentialThinkingTool>`
/// - Rust doesn't forward trait impls through Arc automatically
/// - Orphan rules prevent implementing ShutdownHook directly on Arc<T>
///
/// # Usage
/// ```no_run
/// use std::sync::Arc;
/// use kodegen_tools_sequential_thinking::{SequentialThinkingTool, SequentialThinkingShutdownHook};
/// use kodegen_server_http::Managers;
///
/// # async fn example() {
/// let tool = SequentialThinkingTool::new();
/// let tool_arc = Arc::new(tool);
/// tool_arc.clone().start_cleanup_task();
///
/// let managers = Managers::new();
/// managers.register(SequentialThinkingShutdownHook(tool_arc)).await;
/// # }
/// ```
pub struct SequentialThinkingShutdownHook(pub Arc<SequentialThinkingTool>);

impl kodegen_server_http::ShutdownHook for SequentialThinkingShutdownHook {
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let tool = self.0.clone();
        Box::pin(async move {
            tool.shutdown().await
                .map_err(|e| anyhow::anyhow!("Failed to shutdown sequential thinking tool: {e}"))
        })
    }
}

/// Start the sequential-thinking HTTP server programmatically
///
/// Returns a ServerHandle for graceful shutdown control.
/// This function is non-blocking - the server runs in background tasks.
///
/// # Arguments
/// * `addr` - Socket address to bind to (e.g., "127.0.0.1:30450")
/// * `tls_cert` - Optional path to TLS certificate file
/// * `tls_key` - Optional path to TLS private key file
///
/// # Returns
/// ServerHandle for graceful shutdown, or error if startup fails
pub async fn start_server(
    addr: std::net::SocketAddr,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
) -> anyhow::Result<kodegen_server_http::ServerHandle> {
    // Bind to the address first
    let listener = tokio::net::TcpListener::bind(addr).await
        .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", addr, e))?;

    // Convert separate cert/key into Option<(cert, key)> tuple
    let tls_config = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => None,
    };

    // Delegate to start_server_with_listener
    start_server_with_listener(listener, tls_config).await
}

/// Start sequential-thinking HTTP server using pre-bound listener (TOCTOU-safe)
///
/// This variant is used by kodegend to eliminate TOCTOU race conditions
/// during port cleanup. The listener is already bound to a port.
///
/// # Arguments
/// * `listener` - Pre-bound TcpListener (port already reserved)
/// * `tls_config` - Optional (cert_path, key_path) for HTTPS
///
/// # Returns
/// ServerHandle for graceful shutdown, or error if startup fails
pub async fn start_server_with_listener(
    listener: tokio::net::TcpListener,
    tls_config: Option<(std::path::PathBuf, std::path::PathBuf)>,
) -> anyhow::Result<kodegen_server_http::ServerHandle> {
    use kodegen_server_http::{ServerBuilder, Managers, RouterSet, register_tool};
    use rmcp::handler::server::router::{prompt::PromptRouter, tool::ToolRouter};

    let mut builder = ServerBuilder::new()
        .category(kodegen_config::CATEGORY_SEQUENTIAL_THINKING)
        .register_tools(|| async {
            let mut tool_router = ToolRouter::new();
            let mut prompt_router = PromptRouter::new();
            let managers = Managers::new();

            // Create sequential thinking tool
            let tool = crate::SequentialThinkingTool::new();

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
        .with_listener(listener);

    if let Some((cert, key)) = tls_config {
        builder = builder.with_tls_config(cert, key);
    }

    builder.serve().await
}
