#!/bin/bash

# RCA Engine Integration Test Script
# Tests all components are wired correctly and work in sync

set -e  # Exit on error

echo "=========================================="
echo "RCA Engine Integration Test"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

# Function to print test result
test_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASSED${NC}: $2"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗ FAILED${NC}: $2"
        ((TESTS_FAILED++))
    fi
}

# Function to check if server is running
check_server() {
    curl -s http://localhost:8080/api/health > /dev/null 2>&1
}

# Function to wait for server
wait_for_server() {
    echo "Waiting for server to start..."
    for i in {1..30}; do
        if check_server; then
            echo -e "${GREEN}Server is running!${NC}"
            return 0
        fi
        sleep 1
    done
    echo -e "${RED}Server failed to start within 30 seconds${NC}"
    return 1
}

echo "Step 1: Checking Rust compilation..."
echo "-----------------------------------"
if cargo check --quiet 2>&1; then
    test_result 0 "Rust code compiles successfully"
else
    test_result 1 "Rust code compilation failed"
    echo "Compilation errors:"
    cargo check 2>&1 | head -20
    exit 1
fi
echo ""

echo "Step 2: Checking required directories..."
echo "-----------------------------------"
REQUIRED_DIRS=("metadata" "data" "ui")
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ -d "$dir" ]; then
        test_result 0 "Directory exists: $dir"
    else
        test_result 1 "Directory missing: $dir"
    fi
done
echo ""

echo "Step 3: Checking metadata files..."
echo "-----------------------------------"
REQUIRED_METADATA=("metadata/tables.json" "metadata/rules.json" "metadata/entities.json" "metadata/lineage.json")
for file in "${REQUIRED_METADATA[@]}"; do
    if [ -f "$file" ]; then
        test_result 0 "Metadata file exists: $file"
    else
        test_result 1 "Metadata file missing: $file"
    fi
done
echo ""

echo "Step 4: Starting backend server..."
echo "-----------------------------------"
# Kill any existing server on port 8080
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
sleep 1

# Start server in background
echo "Starting server..."
cargo run --bin server > server_test.log 2>&1 &
SERVER_PID=$!

# Wait for server to start
if wait_for_server; then
    test_result 0 "Backend server started successfully"
else
    test_result 1 "Backend server failed to start"
    echo "Server logs:"
    tail -20 server_test.log
    kill $SERVER_PID 2>/dev/null || true
    exit 1
fi
echo ""

echo "Step 5: Testing API endpoints..."
echo "-----------------------------------"

# Test health endpoint
echo "Testing /api/health..."
HEALTH_RESPONSE=$(curl -s http://localhost:8080/api/health)
if echo "$HEALTH_RESPONSE" | grep -q "ok"; then
    test_result 0 "Health endpoint responds correctly"
else
    test_result 1 "Health endpoint failed"
    echo "Response: $HEALTH_RESPONSE"
fi

# Test tables endpoint
echo "Testing /api/tables..."
TABLES_RESPONSE=$(curl -s http://localhost:8080/api/tables)
if echo "$TABLES_RESPONSE" | grep -q "tables\|sources"; then
    test_result 0 "Tables endpoint responds correctly"
    echo "  Found $(echo "$TABLES_RESPONSE" | grep -o '"id"' | wc -l) tables"
else
    test_result 1 "Tables endpoint failed"
    echo "Response: $TABLES_RESPONSE"
fi

# Test rules endpoint
echo "Testing /api/rules..."
RULES_RESPONSE=$(curl -s http://localhost:8080/api/rules)
if echo "$RULES_RESPONSE" | grep -q "rules"; then
    test_result 0 "Rules endpoint responds correctly"
    echo "  Found $(echo "$RULES_RESPONSE" | grep -o '"id"' | wc -l) rules"
else
    test_result 1 "Rules endpoint failed"
    echo "Response: $RULES_RESPONSE"
fi

# Test knowledge base endpoint
echo "Testing /api/knowledge-base..."
KB_RESPONSE=$(curl -s http://localhost:8080/api/knowledge-base)
if echo "$KB_RESPONSE" | grep -q "terms\|tables\|relationships"; then
    test_result 0 "Knowledge base endpoint responds correctly"
else
    test_result 1 "Knowledge base endpoint failed"
    echo "Response: $KB_RESPONSE"
fi

# Test graph endpoint
echo "Testing /api/graph..."
GRAPH_RESPONSE=$(curl -s http://localhost:8080/api/graph)
if echo "$GRAPH_RESPONSE" | grep -q "nodes\|edges"; then
    test_result 0 "Graph endpoint responds correctly"
    NODE_COUNT=$(echo "$GRAPH_RESPONSE" | grep -o '"id"' | wc -l)
    echo "  Found $NODE_COUNT nodes"
else
    test_result 1 "Graph endpoint failed"
    echo "Response: $GRAPH_RESPONSE"
fi
echo ""

echo "Step 6: Testing query execution..."
echo "-----------------------------------"
# Test a simple query
QUERY_TEST='{"query":"SELECT * FROM tables LIMIT 1","mode":"sql"}'
QUERY_RESPONSE=$(curl -s -X POST http://localhost:8080/api/query/execute \
    -H "Content-Type: application/json" \
    -d "$QUERY_TEST")

if echo "$QUERY_RESPONSE" | grep -q "status\|success\|error"; then
    test_result 0 "Query execution endpoint responds"
    echo "  Query response received"
else
    test_result 1 "Query execution endpoint failed"
    echo "Response: $QUERY_RESPONSE"
fi
echo ""

echo "Step 7: Testing intent compilation..."
echo "-----------------------------------"
# Test intent assessment
INTENT_TEST='{"query":"find mismatch in minority_category between system_a and system_b"}'
INTENT_RESPONSE=$(curl -s -X POST http://localhost:8080/api/reasoning/assess \
    -H "Content-Type: application/json" \
    -d "$INTENT_TEST")

if echo "$INTENT_RESPONSE" | grep -q "status\|needs_clarification\|success"; then
    test_result 0 "Intent assessment endpoint responds"
    echo "  Intent assessment completed"
else
    test_result 1 "Intent assessment endpoint failed"
    echo "Response: $INTENT_RESPONSE"
fi
echo ""

echo "Step 8: Testing frontend build..."
echo "-----------------------------------"
if [ -d "ui" ]; then
    cd ui
    if npm list --depth=0 > /dev/null 2>&1; then
        test_result 0 "Frontend dependencies installed"
        
        # Check if frontend can build
        if npm run build > /dev/null 2>&1; then
            test_result 0 "Frontend builds successfully"
        else
            test_result 1 "Frontend build failed"
            echo "Build errors:"
            npm run build 2>&1 | head -20
        fi
    else
        test_result 1 "Frontend dependencies not installed"
        echo "Run: cd ui && npm install"
    fi
    cd ..
else
    test_result 1 "Frontend directory not found"
fi
echo ""

echo "Step 9: Testing component integration..."
echo "-----------------------------------"
# Test that metadata can be loaded
METADATA_TEST=$(curl -s http://localhost:8080/api/tables | jq -r '.tables[0].name // .sources[0].name // empty' 2>/dev/null)
if [ -n "$METADATA_TEST" ]; then
    test_result 0 "Metadata loading works"
    echo "  Sample table: $METADATA_TEST"
else
    test_result 1 "Metadata loading failed"
fi

# Test that graph can be constructed
GRAPH_NODES=$(curl -s http://localhost:8080/api/graph | jq -r '.nodes | length' 2>/dev/null)
if [ -n "$GRAPH_NODES" ] && [ "$GRAPH_NODES" -gt 0 ]; then
    test_result 0 "Graph construction works"
    echo "  Graph has $GRAPH_NODES nodes"
else
    test_result 1 "Graph construction failed"
fi
echo ""

echo "Step 10: Cleanup..."
echo "-----------------------------------"
# Kill the test server
if [ -n "$SERVER_PID" ]; then
    kill $SERVER_PID 2>/dev/null || true
    sleep 1
    echo "Test server stopped"
fi
echo ""

echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "Tests Failed: ${RED}$TESTS_FAILED${NC}"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed! All components are wired correctly.${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Please review the errors above.${NC}"
    exit 1
fi

