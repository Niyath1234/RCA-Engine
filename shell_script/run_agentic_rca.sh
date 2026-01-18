#!/bin/bash

# Run Agentic RCA Reasoning - Shows reasoning steps like Cursor
# Usage: ./run_agentic_rca.sh [problem]

set -e

# Default problem if not provided
PROBLEM="${1:-Why is System A TOS different from System B TOS for loan L001?}"

# Load environment
if [ -f .env ]; then
    source .env
fi

# Check if metadata exists
if [ ! -d "metadata/multi_grain_test" ]; then
    echo "❌ Error: metadata/multi_grain_test not found"
    echo "   Please ensure metadata is set up"
    exit 1
fi

# Build if needed
if [ ! -f "target/release/rca-engine" ]; then
    echo "🔨 Building rca-engine..."
    cargo build --release --bin rca-engine
fi

echo ""
echo "🤖 Running Agentic RCA Reasoning..."
echo "📋 Problem: $PROBLEM"
echo ""

# Run agentic reasoning
./target/release/rca-engine agentic "$PROBLEM" \
    --metadata-dir metadata/multi_grain_test

