#!/usr/bin/env python3
"""
Test JOIN with GROUP BY query that's failing
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("JOIN WITH GROUP BY - DEBUGGING")
print("=" * 70)

# First, let's check if the tables have the right schema
print("\n[STEP 1] Verify table schemas")
print("-" * 70)

orders_schema = execute("SELECT * FROM orders LIMIT 1")
print(f"Orders schema: {orders_schema['result'].to_list()[0].keys()}")

items_schema = execute("SELECT * FROM order_items LIMIT 1")
print(f"Order_items schema: {items_schema['result'].to_list()[0].keys()}")

# The failing query
print("\n[STEP 2] Test the failing query")
print("-" * 70)

failing_query = """
SELECT o.customer_id, oi.product_id, sum(oi.quantity)
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.customer_id, oi.product_id
"""

print(f"Query: {failing_query.strip()}")

try:
    result = execute(failing_query)
    result_list = result['result'].to_list()
    print(f"✅ SUCCESS: {len(result_list)} rows")
    if result_list:
        print(f"First row: {result_list[0]}")
except Exception as e:
    print(f"❌ FAILED: {e}")
    print(f"\nError details:")
    print(f"  Error type: {type(e).__name__}")
    print(f"  Error message: {str(e)}")

# Test variations
print("\n[STEP 3] Test simpler variations")
print("-" * 70)

test_queries = [
    {
        "name": "JOIN without GROUP BY",
        "query": "SELECT o.customer_id, oi.product_id FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 10"
    },
    {
        "name": "GROUP BY without JOIN",
        "query": "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id LIMIT 10"
    },
    {
        "name": "JOIN with simple aggregation",
        "query": "SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.id = oi.order_id"
    },
    {
        "name": "JOIN with SUM but no GROUP BY",
        "query": "SELECT SUM(oi.quantity) FROM orders o JOIN order_items oi ON o.id = oi.order_id"
    }
]

for test in test_queries:
    print(f"\n{test['name']}:")
    print(f"  Query: {test['query']}")

    try:
        result = execute(test['query'])
        result_list = result['result'].to_list()
        print(f"  ✅ SUCCESS: {len(result_list)} rows")
        if len(result_list) <= 3:
            for i, row in enumerate(result_list):
                print(f"    Row {i}: {row}")
    except Exception as e:
        print(f"  ❌ FAILED: {e}")

print("\n" + "=" * 70)
print("ANALYSIS")
print("=" * 70)

print("\nQuery routing logic:")
query_upper = failing_query.upper()
has_join = 'JOIN' in query_upper
has_group_by = 'GROUP BY' in query_upper
has_agg = any(kw in query_upper for kw in ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX'])

print(f"  Has JOIN: {has_join}")
print(f"  Has GROUP BY: {has_group_by}")
print(f"  Has aggregation keyword: {has_agg}")

if has_join:
    print(f"\n  → Routes to: execute_join_gpu")
    print(f"  → This path may not support GROUP BY aggregations!")

print("\n" + "=" * 70)
