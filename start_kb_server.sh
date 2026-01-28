#!/bin/bash
# Start KnowledgeBase REST API Server

set -e

echo "🚀 Starting KnowledgeBase REST API Server..."

# Change to KnowledgeBase directory
cd "$(dirname "$0")/KnowledgeBase"

# Check if Cargo is installed
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: Cargo (Rust) is not installed"
    echo "   Please install Rust from https://rustup.rs/"
    exit 1
fi

# Build if needed
if [ ! -f "target/release/kb-api-server" ]; then
    echo "📦 Building server..."
    cargo build --release
fi

# Set default host/port if not set
export KB_API_HOST=${KB_API_HOST:-127.0.0.1}
export KB_API_PORT=${KB_API_PORT:-8080}

echo "🌐 Server will run on http://${KB_API_HOST}:${KB_API_PORT}"
echo "   Press Ctrl+C to stop"
echo ""

# Run the server
cargo run --release --bin kb-api-server

