#!/bin/bash
# Start the RCA Engine API Server

echo "🚀 Starting RCA Engine API Server..."
echo "📡 Server will run on http://localhost:8080"
echo "🌐 UI should connect to: http://localhost:8080"
echo ""
echo "Building server..."
cargo build --bin server --release

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Build successful! Starting server..."
    echo ""
    ./target/release/server
else
    echo ""
    echo "❌ Build failed. Please check errors above."
    exit 1
fi

