#!/usr/bin/env python3
"""
RCA Engine Component Integration Test
Tests that all components are wired correctly and work in sync
"""

import requests
import json
import time
import subprocess
import sys
import os
from pathlib import Path

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

BASE_URL = "http://localhost:8080"
SERVER_PID = None

def print_test(name):
    print(f"\n{Colors.BLUE}Testing: {name}{Colors.END}")

def print_pass(msg):
    print(f"{Colors.GREEN}✓ PASSED{Colors.END}: {msg}")

def print_fail(msg):
    print(f"{Colors.RED}✗ FAILED{Colors.END}: {msg}")

def check_server():
    """Check if server is running"""
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=2)
        return response.status_code == 200
    except:
        return False

def wait_for_server(max_wait=30):
    """Wait for server to start"""
    print("Waiting for server to start...")
    for i in range(max_wait):
        if check_server():
            print_pass("Server is running!")
            return True
        time.sleep(1)
        print(f"  Attempt {i+1}/{max_wait}...")
    print_fail("Server failed to start")
    return False

def start_server():
    """Start the backend server"""
    global SERVER_PID
    print("Starting backend server...")
    
    # Kill any existing server
    try:
        subprocess.run(["lsof", "-ti:8080"], check=False, 
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(["pkill", "-f", "cargo run --bin server"], 
                      check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(1)
    except:
        pass
    
    # Start server
    try:
        process = subprocess.Popen(
            ["cargo", "run", "--bin", "server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=os.getcwd()
        )
        SERVER_PID = process.pid
        return wait_for_server()
    except Exception as e:
        print_fail(f"Failed to start server: {e}")
        return False

def test_health():
    """Test health endpoint"""
    print_test("Health Check")
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "ok":
                print_pass("Health endpoint working")
                return True
        print_fail(f"Health check failed: {response.status_code}")
        return False
    except Exception as e:
        print_fail(f"Health check error: {e}")
        return False

def test_tables():
    """Test tables endpoint"""
    print_test("Tables Endpoint")
    try:
        response = requests.get(f"{BASE_URL}/api/tables", timeout=5)
        if response.status_code == 200:
            data = response.json()
            tables = data.get("tables", data.get("sources", []))
            print_pass(f"Tables endpoint working ({len(tables)} tables)")
            if tables:
                print(f"  Sample table: {tables[0].get('name', 'N/A')}")
            return True
        print_fail(f"Tables endpoint failed: {response.status_code}")
        return False
    except Exception as e:
        print_fail(f"Tables endpoint error: {e}")
        return False

def test_rules():
    """Test rules endpoint"""
    print_test("Rules Endpoint")
    try:
        response = requests.get(f"{BASE_URL}/api/rules", timeout=5)
        if response.status_code == 200:
            data = response.json()
            rules = data.get("rules", [])
            print_pass(f"Rules endpoint working ({len(rules)} rules)")
            return True
        print_fail(f"Rules endpoint failed: {response.status_code}")
        return False
    except Exception as e:
        print_fail(f"Rules endpoint error: {e}")
        return False

def test_knowledge_base():
    """Test knowledge base endpoint"""
    print_test("Knowledge Base Endpoint")
    try:
        response = requests.get(f"{BASE_URL}/api/knowledge-base", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print_pass("Knowledge base endpoint working")
            return True
        print_fail(f"Knowledge base endpoint failed: {response.status_code}")
        return False
    except Exception as e:
        print_fail(f"Knowledge base endpoint error: {e}")
        return False

def test_graph():
    """Test graph endpoint"""
    print_test("Graph Endpoint")
    try:
        response = requests.get(f"{BASE_URL}/api/graph", timeout=5)
        if response.status_code == 200:
            data = response.json()
            nodes = data.get("nodes", [])
            edges = data.get("edges", [])
            print_pass(f"Graph endpoint working ({len(nodes)} nodes, {len(edges)} edges)")
            return True
        print_fail(f"Graph endpoint failed: {response.status_code}")
        return False
    except Exception as e:
        print_fail(f"Graph endpoint error: {e}")
        return False

def test_query_execute():
    """Test query execution"""
    print_test("Query Execution")
    try:
        # Get actual table name from metadata
        tables_response = requests.get(f"{BASE_URL}/api/tables", timeout=5)
        if tables_response.status_code == 200:
            tables_data = tables_response.json()
            tables = tables_data.get("tables", tables_data.get("sources", []))
            if tables:
                table_name = tables[0].get("name", "customer_accounts")
            else:
                table_name = "customer_accounts"  # fallback
        else:
            table_name = "customer_accounts"  # fallback
        
        payload = {
            "query": f"SELECT * FROM {table_name} LIMIT 1",
            "mode": "sql"
        }
        response = requests.post(
            f"{BASE_URL}/api/query/execute",
            json=payload,
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            print_pass("Query execution endpoint working")
            return True
        print_fail(f"Query execution failed: {response.status_code}")
        print(f"  Response: {response.text[:200]}")
        return False
    except Exception as e:
        print_fail(f"Query execution error: {e}")
        return False

def test_intent_assessment():
    """Test intent assessment"""
    print_test("Intent Assessment")
    try:
        payload = {
            "query": "find mismatch in minority_category between system_a and system_b"
        }
        response = requests.post(
            f"{BASE_URL}/api/reasoning/assess",
            json=payload,
            timeout=30  # Increased timeout for LLM calls
        )
        if response.status_code == 200:
            data = response.json()
            print_pass("Intent assessment endpoint working")
            status = data.get("status", "unknown")
            print(f"  Status: {status}")
            return True
        print_fail(f"Intent assessment failed: {response.status_code}")
        print(f"  Response: {response.text[:200]}")
        return False
    except Exception as e:
        print_fail(f"Intent assessment error: {e}")
        return False

def test_metadata_files():
    """Test that metadata files exist"""
    print_test("Metadata Files")
    required_files = [
        "metadata/tables.json",
        "metadata/rules.json",
        "metadata/entities.json",
        "metadata/lineage.json"
    ]
    all_exist = True
    for file_path in required_files:
        if Path(file_path).exists():
            print_pass(f"Found: {file_path}")
        else:
            print_fail(f"Missing: {file_path}")
            all_exist = False
    return all_exist

def test_data_directory():
    """Test that data directory exists"""
    print_test("Data Directory")
    if Path("data").exists():
        files = list(Path("data").glob("*.csv")) + list(Path("data").glob("*.parquet"))
        print_pass(f"Data directory exists ({len(files)} data files)")
        return True
    else:
        print_fail("Data directory missing")
        return False

def test_frontend():
    """Test frontend build"""
    print_test("Frontend Build")
    ui_dir = Path("ui")
    if not ui_dir.exists():
        print_fail("Frontend directory not found")
        return False
    
    # Check if node_modules exists
    if not (ui_dir / "node_modules").exists():
        print(f"{Colors.YELLOW}⚠ WARNING{Colors.END}: Frontend dependencies not installed")
        print("  Run: cd ui && npm install")
        return False
    
    # Try to build
    try:
        result = subprocess.run(
            ["npm", "run", "build"],
            cwd=ui_dir,
            capture_output=True,
            timeout=60
        )
        if result.returncode == 0:
            print_pass("Frontend builds successfully")
            return True
        else:
            print_fail("Frontend build failed")
            print(result.stderr.decode()[:500])
            return False
    except Exception as e:
        print_fail(f"Frontend build error: {e}")
        return False

def cleanup():
    """Cleanup: stop server"""
    global SERVER_PID
    if SERVER_PID:
        try:
            os.kill(SERVER_PID, 15)  # SIGTERM
            time.sleep(1)
            print(f"\n{Colors.YELLOW}Stopped test server{Colors.END}")
        except:
            pass

def main():
    """Run all tests"""
    print(f"\n{Colors.BLUE}{'='*60}")
    print("RCA Engine Component Integration Test")
    print(f"{'='*60}{Colors.END}\n")
    
    results = []
    
    # Check files first
    results.append(("Metadata Files", test_metadata_files()))
    results.append(("Data Directory", test_data_directory()))
    
    # Start server
    if not check_server():
        if not start_server():
            print(f"\n{Colors.RED}Cannot proceed without server{Colors.END}")
            return 1
    else:
        print_pass("Server already running")
    
    # Test endpoints
    results.append(("Health Check", test_health()))
    results.append(("Tables Endpoint", test_tables()))
    results.append(("Rules Endpoint", test_rules()))
    results.append(("Knowledge Base", test_knowledge_base()))
    results.append(("Graph Endpoint", test_graph()))
    results.append(("Query Execution", test_query_execute()))
    results.append(("Intent Assessment", test_intent_assessment()))
    
    # Test frontend (optional)
    results.append(("Frontend Build", test_frontend()))
    
    # Summary
    print(f"\n{Colors.BLUE}{'='*60}")
    print("Test Summary")
    print(f"{'='*60}{Colors.END}\n")
    
    passed = sum(1 for _, result in results if result)
    failed = len(results) - passed
    
    for name, result in results:
        if result:
            print_pass(name)
        else:
            print_fail(name)
    
    print(f"\n{Colors.BLUE}Results:{Colors.END}")
    print(f"  {Colors.GREEN}Passed: {passed}{Colors.END}")
    print(f"  {Colors.RED}Failed: {failed}{Colors.END}")
    
    cleanup()
    
    if failed == 0:
        print(f"\n{Colors.GREEN}All tests passed! All components are wired correctly.{Colors.END}")
        return 0
    else:
        print(f"\n{Colors.RED}Some tests failed. Please review the errors above.{Colors.END}")
        return 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Test interrupted{Colors.END}")
        cleanup()
        sys.exit(1)

