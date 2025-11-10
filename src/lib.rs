//! Sequential thinking tool for MCP

mod types;
mod session;
mod persistence;
pub mod tool;
mod trait_impl;

pub use tool::SequentialThinkingTool;
pub use types::{SessionStateSnapshot, ThoughtData};

use anyhow::Result;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

// Wrapper to implement ShutdownHook for session persistence
struct SequentialThinkingWrapper(Arc<crate::SequentialThinkingTool>);

impl kodegen_server_http::ShutdownHook for SequentialThinkingWrapper {
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let tool = self.0.clone();
        Box::pin(async move {
            tool.shutdown().await
                .map_err(|e| anyhow::anyhow!("Failed to shutdown sequential thinking tool: {e}"))
        })
    }
}

/// Start the sequential-thinking HTTP server programmatically for embedded mode
pub async fn start_server(
    addr: std::net::SocketAddr,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
) -> Result<()> {
    use kodegen_server_http::{Managers, RouterSet, register_tool};
    use kodegen_tools_config::ConfigManager;
    use rmcp::handler::server::router::{prompt::PromptRouter, tool::ToolRouter};

    let _ = env_logger::try_init();

    if rustls::crypto::ring::default_provider().install_default().is_err() {
        log::debug!("rustls crypto provider already installed");
    }

    let config = ConfigManager::new();
    config.init().await?;

    let timestamp = chrono::Utc::now();
    let pid = std::process::id();
    let instance_id = format!("{}-{}", timestamp.format("%Y%m%d-%H%M%S-%9f"), pid);
    let usage_tracker = kodegen_utils::usage_tracker::UsageTracker::new(
        format!("sequential-thinking-{}", instance_id)
    );

    kodegen_mcp_tool::tool_history::init_global_history(instance_id.clone()).await;

    let mut tool_router = ToolRouter::new();
    let mut prompt_router = PromptRouter::new();
    let managers = Managers::new();

    // Create sequential thinking tool
    let tool = crate::SequentialThinkingTool::new();
    
    // Wrap in Arc and start cleanup task (required for session management)
    let tool_arc = Arc::new(tool.clone());
    tool_arc.clone().start_cleanup_task();

    // Register shutdown hook to persist active sessions on exit
    managers.register(SequentialThinkingWrapper(tool_arc)).await;

    // Register the tool (1 tool)
    (tool_router, prompt_router) = register_tool(
        tool_router,
        prompt_router,
        tool,
    );

    let router_set = RouterSet::new(tool_router, prompt_router, managers);

    let session_config = rmcp::transport::streamable_http_server::session::local::SessionConfig {
        channel_capacity: 16,
        keep_alive: Some(std::time::Duration::from_secs(3600)),
    };
    let session_manager = Arc::new(
        rmcp::transport::streamable_http_server::session::local::LocalSessionManager {
            sessions: Default::default(),
            session_config,
        }
    );

    let server = kodegen_server_http::HttpServer::new(
        router_set.tool_router,
        router_set.prompt_router,
        usage_tracker,
        config,
        router_set.managers,
        session_manager,
    );

    let shutdown_timeout = std::time::Duration::from_secs(30);
    let tls_config = tls_cert.zip(tls_key);
    let handle = server.serve_with_tls(addr, tls_config, shutdown_timeout).await?;

    handle.wait_for_completion(shutdown_timeout).await
        .map_err(|e| anyhow::anyhow!("Server shutdown error: {}", e))?;

    Ok(())
}
