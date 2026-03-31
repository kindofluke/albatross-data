#!/usr/bin/env python3
"""
Debug GROUP BY queries
"""
import os
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

from data_kernel import arrow_bridge

# Test different GROUP BY queries
test_queries = [
    ("GROUP BY status", "SELECT status, COUNT(*) FROM orders GROUP BY status"),
    ("GROUP BY customer_id", "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"),
    ("GROUP BY with SUM", "SELECT status, SUM(amount) FROM orders GROUP BY status"),
    ("Simple GROUP BY", "SELECT status FROM orders GROUP BY status"),
]

for name, query in test_queries:
    print(f"\n{'='*60}")
    print(f"{name}: {query}")
    print('='*60)
    try:
        result = arrow_bridge.execute_query(query)
        if result:
            df = result.to_pandas()
            print(f"✓ Result ({len(df)} rows):")
            print(df)
        else:
            print(f"⚠ Empty result (None returned)")
    except Exception as e:
        print(f"✗ Error: {e}")
