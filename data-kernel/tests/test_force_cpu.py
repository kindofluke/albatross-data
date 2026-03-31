#!/usr/bin/env python3
"""
Test CPU execution by temporarily disabling GPU
"""
import os
import sys

# Set DATA_PATH
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

# Check if we can call the CPU execution directly
# Looking at lib.rs, we need to check GPU availability

from data_kernel import arrow_bridge, is_gpu_available, get_gpu_info

print("GPU Status:")
print(f"  Available: {is_gpu_available()}")
gpu_info = get_gpu_info()
if gpu_info:
    print(f"  Name: {gpu_info['name']}")
    print(f"  Backend: {gpu_info['backend']}")

print("\nTesting queries:")

test_queries = [
    ("COUNT", "SELECT COUNT(*) FROM orders"),
    ("SUM", "SELECT SUM(amount) FROM orders"),
    ("Multiple aggs", "SELECT MIN(amount), MAX(amount), AVG(amount) FROM orders"),
]

for name, query in test_queries:
    print(f"\n{name}: {query}")
    try:
        result = arrow_bridge.execute_query(query)
        if result:
            df = result.to_pandas()
            print(f"  Result: {df.to_dict('records')}")
        else:
            print(f"  Empty result")
    except Exception as e:
        print(f"  Error: {e}")
