#!/usr/bin/env python3
"""
Test window functions to understand why they fail
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("WINDOW FUNCTIONS TEST")
print("=" * 70)

# Test various window function queries
window_tests = [
    {
        "name": "ROW_NUMBER() basic",
        "query": "SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num FROM orders LIMIT 10"
    },
    {
        "name": "ROW_NUMBER() with PARTITION BY",
        "query": "SELECT id, customer_id, amount, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) as row_num FROM orders LIMIT 10"
    },
    {
        "name": "RANK()",
        "query": "SELECT id, amount, RANK() OVER (ORDER BY amount DESC) as rank FROM orders LIMIT 10"
    },
    {
        "name": "SUM() OVER (window aggregate)",
        "query": "SELECT id, amount, SUM(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum FROM orders LIMIT 10"
    },
    {
        "name": "LAG/LEAD",
        "query": "SELECT id, amount, LAG(amount, 1) OVER (ORDER BY id) as prev_amount FROM orders LIMIT 10"
    },
    {
        "name": "AVG() OVER with PARTITION",
        "query": "SELECT id, customer_id, amount, AVG(amount) OVER (PARTITION BY customer_id) as avg_by_customer FROM orders LIMIT 10"
    }
]

for i, test in enumerate(window_tests, 1):
    print(f"\n[Test {i}] {test['name']}")
    print(f"  Query: {test['query']}")

    try:
        result = execute(test['query'])

        if result is None or 'result' not in result:
            print(f"  ❌ FAILED: No result returned")
            continue

        result_list = result['result'].to_list()
        print(f"  ✅ SUCCESS: {len(result_list)} rows returned")

        # Show first row
        if result_list:
            print(f"  First row: {result_list[0]}")

    except Exception as e:
        print(f"  ❌ ERROR: {e}")

        # Analyze the error
        error_str = str(e)
        if "error code: -5" in error_str:
            print(f"     → Query execution failed in Rust")
        elif "Binder" in error_str:
            print(f"     → SQL binding/parsing error")
        elif "not supported" in error_str.lower():
            print(f"     → Feature not supported")

print("\n" + "=" * 70)
print("DIAGNOSIS")
print("=" * 70)

# Let's check what the query detection logic thinks about window functions
test_query = "SELECT id, ROW_NUMBER() OVER (ORDER BY amount) FROM orders"
query_upper = test_query.upper()

print(f"\nTest query: {test_query}")
print(f"\nDetection logic analysis:")
print(f"  Contains 'JOIN': {'JOIN' in query_upper}")
print(f"  Contains 'SUM': {'SUM' in query_upper}")
print(f"  Contains 'COUNT': {'COUNT' in query_upper}")
print(f"  Contains 'AVG': {'AVG' in query_upper}")
print(f"  Contains 'MIN': {'MIN' in query_upper}")
print(f"  Contains 'MAX': {'MAX' in query_upper}")
print(f"  Contains 'GROUP BY': {'GROUP BY' in query_upper}")
print(f"  Contains 'OVER': {'OVER' in query_upper}")
print(f"  Contains 'PARTITION': {'PARTITION' in query_upper}")
print(f"  Contains 'ROW_NUMBER': {'ROW_NUMBER' in query_upper}")

print(f"\n→ Current routing: ", end="")
is_join = 'JOIN' in query_upper
is_aggregation = any(kw in query_upper for kw in ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'GROUP BY'])

if is_join:
    print("execute_join_gpu")
elif is_aggregation:
    print("execute_simple_agg_gpu")
else:
    print("execute_table_scan_cpu")

print("\n→ Problem: Window functions should go to CPU table scan path!")
print("   But they might be mis-routed if they contain aggregation keywords.")

print("\n" + "=" * 70)
