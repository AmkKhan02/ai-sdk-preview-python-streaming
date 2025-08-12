#!/usr/bin/env python3
"""
Test script for the new analytical query API endpoint.
"""

import requests
import json

def test_analyze_endpoint():
    """Test the /api/analyze-duckdb endpoint"""
    
    # Test data
    test_request = {
        "question": "Describe the data I've uploaded",
        "db_path": "leads_data.duckdb",
        "session_id": "test-session-123"
    }
    
    try:
        # Make request to the endpoint
        response = requests.post(
            "http://localhost:8000/api/analyze-duckdb",
            json=test_request,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            print("Success! Response structure:")
            print(f"- success: {result.get('success')}")
            print(f"- answer: {result.get('answer', 'N/A')[:100]}...")
            print(f"- sql_queries: {len(result.get('sql_queries', []))} queries")
            print(f"- query_results: {len(result.get('query_results', []))} results")
            print(f"- session_id: {result.get('session_id')}")
            print(f"- error: {result.get('error')}")
        else:
            print(f"Error Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("Could not connect to the API server. Make sure it's running on localhost:8000")
    except Exception as e:
        print(f"Error testing endpoint: {e}")

if __name__ == "__main__":
    test_analyze_endpoint()