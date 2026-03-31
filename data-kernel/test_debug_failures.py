#!/usr/bin/env python3
"""
Debug script to understand why COUNT(*) and JOINs are failing
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("DEBUGGING FAILED QUERIES")
print("=" * 70)

# Test different COUNT variations
count_tests = [
    "SELECT COUNT(*) as cnt FROM orders",
    "SELECT COUNT(id) as cnt FROM orders",
    "SELECT COUNT(amount) as cnt FROM orders",
    "SELECT count(*) as cnt FROM orders",  # lowercase
]

print("\n[COUNT VARIATIONS]")
for query in count_tests:
    print(f"\nQuery: {query}")
    try:
        result = execute(query)
        print(f"  ✅ Success: {result}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# Test different JOIN variations
join_tests = [
    "SELECT * FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 5",
    "SELECT o.id FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 5",
    "SELECT COUNT(*) FROM orders o join order_items oi ON o.id = oi.order_id",  # lowercase join
]

print("\n\n[JOIN VARIATIONS]")
for query in join_tests:
    print(f"\nQuery: {query}")
    try:
        result = execute(query)
        print(f"  ✅ Success")
        if result and 'result' in result:
            result_list = result['result'].to_list()
            print(f"  Rows: {len(result_list)}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# Test if it's a case sensitivity issue
print("\n\n[CASE SENSITIVITY TEST]")
test_cases = [
    ("Uppercase JOIN", "SELECT * FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 5"),
    ("Lowercase join", "SELECT * FROM orders o join order_items oi ON o.id = oi.order_id LIMIT 5"),
    ("Mixed Join", "SELECT * FROM orders o Join order_items oi ON o.id = oi.order_id LIMIT 5"),
]

for name, query in test_cases:
    print(f"\n{name}: {query}")
    query_upper = query.upper()
    print(f"  Contains 'JOIN': {'JOIN' in query_upper}")
    try:
        result = execute(query)
        print(f"  ✅ Success")
    except Exception as e:
        print(f"  ❌ Error: {e}")
