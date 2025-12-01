<div align="center">
  <img src="assets/img/banner.png" alt="Kodegen AI Banner" width="100%" />
</div>

# kodegen-tools-sequential-thinking

[![License](https://img.shields.io/badge/license-Apache%202.0%20OR%20MIT-blue.svg)](LICENSE.md)
[![Rust](https://img.shields.io/badge/rust-nightly-orange.svg)](https://www.rust-lang.org/)

A blazing-fast, memory-efficient MCP (Model Context Protocol) tool that provides sequential thinking capabilities for AI agents. Part of the KODEGEN.ᴀɪ ecosystem.

## Features 

- **Sequential Thinking**: Break down complex problems into manageable thought steps
- **Dynamic Adjustment**: Expand or reduce total thoughts as understanding evolves
- **Revisions**: Correct or refine previous thoughts when new insights emerge
- **Branching**: Explore alternative solution paths in parallel
- **Session Persistence**: Automatically save and restore thinking sessions
- **Zero-Copy Architecture**: Lock-free actor model for maximum performance
- **Concurrent Sessions**: Perfect isolation between multiple users/agents

## Installation

### Prerequisites

- Rust nightly toolchain (automatically configured via `rust-toolchain.toml`)
- Cargo

### Building from Source

```bash
# Clone the repository
git clone https://github.com/cyrup-ai/kodegen-tools-sequential-thinking.git
cd kodegen-tools-sequential-thinking

# Build the project
cargo build --release

# Run the MCP server
cargo run --release --bin kodegen-sequential-thinking --features server
```

## Usage

### As an MCP Server

The tool runs as an HTTP/HTTPS server implementing the Model Context Protocol:

```bash
# Start the server (typically managed by kodegend daemon)
cargo run --bin kodegen-sequential-thinking --features server
```

Default port: `30437` (configured by kodegend)

### Tool Parameters

```json
{
  "thought": "Your reasoning step",
  "thought_number": 1,
  "total_thoughts": 5,
  "next_thought_needed": true,
  "session_id": "optional-uuid",
  "is_revision": false,
  "revises_thought": null,
  "branch_from_thought": null,
  "branch_id": null,
  "needs_more_thoughts": false
}
```

### Basic Example

```rust
// Thought 1: Initial analysis
{
  "thought": "I need to calculate 15 * 24 step by step",
  "thought_number": 1,
  "total_thoughts": 3,
  "next_thought_needed": true
}

// Thought 2: Break down the problem
{
  "thought": "Using distribution: 15 * 24 = 15 * (20 + 4)",
  "thought_number": 2,
  "total_thoughts": 3,
  "next_thought_needed": true
}

// Thought 3: Final answer
{
  "thought": "15*20 = 300, 15*4 = 60, so 300 + 60 = 360",
  "thought_number": 3,
  "total_thoughts": 3,
  "next_thought_needed": false
}
```

### Advanced Features

#### Revisions
Correct a previous thought when you realize an error:
```json
{
  "thought": "Wait, I need to revise my earlier assumption",
  "thought_number": 3,
  "total_thoughts": 5,
  "is_revision": true,
  "revises_thought": 2,
  "next_thought_needed": true
}
```

#### Branching
Explore alternative approaches:
```json
{
  "thought": "Let me explore denormalization as an alternative",
  "thought_number": 3,
  "total_thoughts": 5,
  "branch_from_thought": 2,
  "branch_id": "denormalize-approach",
  "next_thought_needed": true
}
```

#### Dynamic Adjustment
Expand your thinking mid-process:
```json
{
  "thought": "Actually, this needs more steps than I thought",
  "thought_number": 3,
  "total_thoughts": 7,  // Increased from 5
  "needs_more_thoughts": true,
  "next_thought_needed": true
}
```

## How It Works

### Architecture

The tool uses an **MPSC actor model** for session management:

- Each session has an isolated async task that owns its state (zero lock contention)
- Sessions communicate via message passing (tokio MPSC channels)
- Automatic persistence to disk when sessions become inactive
- Sessions restore seamlessly when accessed again

### Session Lifecycle

1. **Creation**: New session → UUID generated → actor task spawned
2. **Active**: Thoughts processed lock-free by dedicated actor
3. **Idle**: Inactive for 30+ minutes → persisted to disk → actor terminated
4. **Restoration**: Old session_id used → loaded from disk → actor respawned
5. **Cleanup**: Sessions older than 24 hours automatically deleted

### Persistence

Sessions are stored in: `$XDG_CONFIG_HOME/kodegen-mcp/sequential_thinking/{session-id}/`

Files:
- `session.json` - Metadata (timestamps, thought count, branches)
- `thought{n}.json` - Individual thoughts
- `branch_{id}_thought{n}.json` - Branch thoughts

## Development

### Running Tests

```bash
# Run unit tests
cargo test

# Run the comprehensive demo
cargo run --example sequential_thinking_demo

# Run with verbose output
cargo test -- --nocapture
```

### Code Quality

```bash
# Format code
cargo fmt

# Run linter
cargo clippy --all-features

# Check all targets
cargo check --all-targets --all-features
```

### Environment Variables

- `DISABLE_THOUGHT_LOGGING=true` - Suppress colorful stderr output during testing

### Project Structure

```
src/
├── lib.rs                      # Public API exports
├── sequential_thinking.rs      # Core implementation
└── main.rs                     # HTTP server binary

examples/
├── sequential_thinking_demo.rs # Comprehensive feature demo
└── common/mod.rs              # Test utilities
```

## Performance

- **Lock-free hot path**: Thought processing uses zero locks
- **Concurrent sessions**: Perfect isolation, no contention between users
- **Memory efficient**: Sessions automatically unloaded when inactive
- **Fast restoration**: Sessions restored from disk in <10ms

## Integration

### With Claude Code

This tool is designed to work seamlessly with Claude Code and other MCP-compatible AI agents:

```markdown
Use the sequential_thinking tool to break down complex problems:
1. Start with an initial thought and estimated total
2. Progress through your reasoning step by step
3. Revise or branch when you discover new insights
4. Conclude when you reach a satisfactory solution
```

### With KODEGEN.ᴀɪ Ecosystem

Part of the kodegen-tools family:
- `kodegen_mcp_tool` - Tool trait and error types
- `kodegen_mcp_schema` - Shared schema definitions
- `kodegen_server_http` - HTTP/HTTPS MCP transport

## License

Dual-licensed under:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE.md#apache-license-20))
- MIT License ([LICENSE-MIT](LICENSE.md#mit-license))

You may choose either license for your use.

## Contributing

Contributions are welcome! Please ensure:
- Code is formatted with `cargo fmt`
- All tests pass with `cargo test`
- Clippy shows no warnings with `cargo clippy --all-features`

## Links

- **Homepage**: [kodegen.ai](https://kodegen.ai)
- **Repository**: [github.com/cyrup-ai/kodegen-tools-sequential-thinking](https://github.com/cyrup-ai/kodegen-tools-sequential-thinking)
- **MCP Specification**: [Model Context Protocol](https://modelcontextprotocol.io/)

---

**KODEGEN.ᴀɪ** - Memory-efficient, Blazing-Fast, MCP tools for code generation agents.

Copyright © 2025 David Maple / kodegen.ai
