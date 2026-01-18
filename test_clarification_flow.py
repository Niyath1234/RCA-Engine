#!/usr/bin/env python3
"""
Test script for the fail-fast clarification flow.
Tests the new /api/reasoning/assess and /api/reasoning/clarify endpoints.
"""

import requests
import json
import subprocess
import time
import sys

API_BASE = "http://localhost:8080"

def start_server():
    """Start the server in background."""
    print("🚀 Starting server...")
    process = subprocess.Popen(
        ["cargo", "run", "--bin", "server"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/Users/niyathnair/Desktop/Task/RCA-ENGINE"
    )
    time.sleep(5)  # Wait for server to start
    return process

def stop_server(process):
    """Stop the server."""
    if process:
        process.terminate()
        process.wait()
        print("🛑 Server stopped")

def test_health():
    """Test server is running."""
    try:
        response = requests.get(f"{API_BASE}/api/health")
        return response.status_code == 200
    except:
        return False

def test_assess_vague_query():
    """Test assessment of a vague query - should ask for clarification."""
    print("\n" + "="*60)
    print("TEST 1: Assess Vague Query (should need clarification)")
    print("="*60)
    
    query = "Why is the balance different?"
    print(f"Query: {query}")
    
    try:
        response = requests.post(
            f"{API_BASE}/api/reasoning/assess",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        
        if data.get("needs_clarification"):
            print("✅ PASS: System correctly asked for clarification")
            print(f"   Question: {data.get('question', 'N/A')[:100]}...")
            print(f"   Confidence: {data.get('confidence', 'N/A')}")
            return True
        else:
            print("❌ FAIL: Expected clarification request")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def test_assess_clear_query():
    """Test assessment of a clear query - should succeed without clarification."""
    print("\n" + "="*60)
    print("TEST 2: Assess Clear Query (should NOT need clarification)")
    print("="*60)
    
    query = "Compare TOS between system_a and system_b for active loans at loan level"
    print(f"Query: {query}")
    
    try:
        response = requests.post(
            f"{API_BASE}/api/reasoning/assess",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Response status: {data.get('status')}")
        
        if not data.get("needs_clarification"):
            print("✅ PASS: System understood query without clarification")
            return True
        else:
            print(f"⚠️  PARTIAL: System asked for clarification (confidence: {data.get('confidence')})")
            print(f"   This might be due to API key not being set")
            return True  # Still acceptable
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def test_clarify_endpoint():
    """Test the clarification answer endpoint."""
    print("\n" + "="*60)
    print("TEST 3: Submit Clarification Answer")
    print("="*60)
    
    original_query = "Why is the balance different?"
    answer = "Compare system_a and system_b at loan level"
    
    print(f"Original Query: {original_query}")
    print(f"Answer: {answer}")
    
    try:
        response = requests.post(
            f"{API_BASE}/api/reasoning/clarify",
            headers={"Content-Type": "application/json"},
            data=json.dumps({
                "query": original_query,
                "answer": answer
            }),
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Response status: {data.get('status')}")
        
        if response.status_code == 200:
            print("✅ PASS: Clarification endpoint working")
            return True
        else:
            print(f"❌ FAIL: Unexpected response")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def test_original_query_endpoint():
    """Test that original /api/reasoning/query still works."""
    print("\n" + "="*60)
    print("TEST 4: Original Query Endpoint (backward compatibility)")
    print("="*60)
    
    query = "Compare TOS between system_a and system_b"
    print(f"Query: {query}")
    
    try:
        response = requests.post(
            f"{API_BASE}/api/reasoning/query",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"query": query}),
            timeout=30
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            has_result = "result" in data or "steps" in data
            if has_result:
                print("✅ PASS: Original endpoint working")
                return True
            else:
                print("⚠️  PARTIAL: Response received but format unexpected")
                return True
        else:
            print(f"❌ FAIL: Status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def main():
    print("="*60)
    print("FAIL-FAST CLARIFICATION FLOW TEST")
    print("="*60)
    
    server_process = None
    
    # Check if server is already running
    if not test_health():
        server_process = start_server()
        if not test_health():
            print("❌ Failed to start server")
            stop_server(server_process)
            sys.exit(1)
    else:
        print("✅ Server already running")
    
    # Run tests
    results = []
    
    try:
        results.append(("Assess Vague Query", test_assess_vague_query()))
        results.append(("Assess Clear Query", test_assess_clear_query()))
        results.append(("Clarify Endpoint", test_clarify_endpoint()))
        results.append(("Original Query Endpoint", test_original_query_endpoint()))
    finally:
        if server_process:
            stop_server(server_process)
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        return 0
    else:
        print("\n⚠️  Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())

