#!/usr/bin/env python3
"""
Diagnostic test for GPU execution
"""
import os
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

from data_kernel import arrow_bridge

# Test simple queries to understand what works
test_queries = [
    ("Simple SELECT *", "SELECT * FROM orders LIMIT 5"),
    ("Simple COUNT", "SELECT COUNT(*) FROM orders"),
    ("Simple SUM", "SELECT SUM(amount) FROM orders"),
    ("COUNT with WHERE", "SELECT COUNT(*) FROM orders WHERE status = 'shipped'"),
    ("SUM with WHERE", "SELECT SUM(amount) FROM orders WHERE status = 'shipped'"),
    ("GROUP BY", "SELECT status, COUNT(*) FROM orders GROUP BY status"),
    ("Multiple aggs", "SELECT MIN(amount), MAX(amount), AVG(amount) FROM orders"),
]

for name, query in test_queries:
    print(f"\n{'='*60}")
    print(f"{name}: {query}")
    print('='*60)
    try:
        result = arrow_bridge.execute_query(query)
        if result:
            df = result.to_pandas()
            print(f"✓ Success:")
            print(df)
        else:
            print(f"⚠ Empty result")
    except Exception as e:
        print(f"✗ Error: {e}")
