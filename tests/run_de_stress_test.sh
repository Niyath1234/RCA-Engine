#!/bin/bash

# Script to run the DE Tools Stress Test
# This test validates that all DE tools work correctly with real-world data quality issues

set -e

echo "🧪 Running Data Engineering Tools Stress Test"
echo "=============================================="
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "❌ Error: Must run from RCA-ENGINE directory"
    exit 1
fi

# Set test environment variables if not set
export OPENAI_API_KEY=${OPENAI_API_KEY:-"test-api-key"}
export OPENAI_MODEL=${OPENAI_MODEL:-"gpt-4o-mini"}
export OPENAI_BASE_URL=${OPENAI_BASE_URL:-"https://api.openai.com/v1"}

echo "📋 Test Configuration:"
echo "   - API Key: ${OPENAI_API_KEY:0:10}..."
echo "   - Model: $OPENAI_MODEL"
echo "   - Base URL: $OPENAI_BASE_URL"
echo ""

# Clean up any previous test artifacts
echo "🧹 Cleaning up previous test artifacts..."
rm -rf test_data_de_stress test_metadata_de_stress 2>/dev/null || true

# Run the test
echo "🚀 Running stress test..."
echo ""

cargo test --test de_tools_stress_test -- --nocapture

TEST_EXIT_CODE=$?

# Clean up test artifacts
echo ""
echo "🧹 Cleaning up test artifacts..."
rm -rf test_data_de_stress test_metadata_de_stress 2>/dev/null || true

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ Stress test passed!"
    echo ""
    echo "Summary:"
    echo "  - All DE tools executed successfully"
    echo "  - Data quality issues were fixed"
    echo "  - Reconciliation completed"
    echo "  - Expected mismatches identified"
    exit 0
else
    echo ""
    echo "❌ Stress test failed!"
    echo "   Check the output above for details"
    exit 1
fi

