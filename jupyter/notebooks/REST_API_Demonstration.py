#!/usr/bin/env python3
"""
Test Script for MongoDB REST API
--------------------------------
Simulates a client application requesting data from the Flask Gateway.
"""

import requests
import json
import time

BASE_URL = "http://localhost:5000"

def test_endpoint(endpoint, description):
    print(f"\n--- Testing: {description} ---")
    url = f"{BASE_URL}{endpoint}"
    print(f"GET {url}")
    
    try:
        response = requests.get(url)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            # Pretty print the first item if list, or whole dict if object
            preview = data[0] if isinstance(data, list) and data else data
            print("Response Preview:")
            print(json.dumps(preview, indent=2))
            if isinstance(data, list):
                print(f"... (Total {len(data)} items received)")
        else:
            print("Error Response:", response.text)
            
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to server. Is inspect_mongodb.py running?")

def main():
    # 1. Test Health Check
    test_endpoint("/", "Health Check")

    # 2. Test Users Collection (Dim Table)
    test_endpoint("/api/dim_users?limit=2", "Users Data (Limit 2)")

    # 3. Test Transactions (Fact Table)
    test_endpoint("/api/fact_transactions?limit=1", "Transactions Data (Limit 1)")
    
    # 4. Test Error Handling (Non-existent collection)
    test_endpoint("/api/non_existent_table", "Invalid Collection Handling")

if __name__ == "__main__":
    print("Waiting for server to ensure it's up...")
    time.sleep(1) 
    main()
