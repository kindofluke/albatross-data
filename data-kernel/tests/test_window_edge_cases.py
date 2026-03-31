#!/usr/bin/env python3
"""
Test edge cases where window functions might conflict with aggregation detection
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("WINDOW FUNCTIONS - EDGE CASES")
print("=" * 70)

# These queries contain aggregation keywords but are window functions
edge_cases = [
    {
        "name": "SUM() OVER - contains 'SUM' keyword",
        "query": "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_sum FROM orders LIMIT 10",
        "contains_agg_keyword": True
    },
    {
        "name": "AVG() OVER - contains 'AVG' keyword",
        "query": "SELECT id, amount, AVG(amount) OVER (PARTITION BY customer_id) as avg FROM orders LIMIT 10",
        "contains_agg_keyword": True
    },
    {
        "name": "COUNT() OVER - contains 'COUNT' keyword",
        "query": "SELECT id, COUNT(*) OVER (PARTITION BY customer_id) as count FROM orders LIMIT 10",
        "contains_agg_keyword": True
    },
    {
        "name": "MIN/MAX OVER - contains 'MIN'/'MAX' keywords",
        "query": "SELECT id, amount, MIN(amount) OVER (PARTITION BY customer_id) as min, MAX(amount) OVER (PARTITION BY customer_id) as max FROM orders LIMIT 10",
        "contains_agg_keyword": True
    },
    {
        "name": "Pure aggregation (no OVER) - SUM",
        "query": "SELECT SUM(amount) as total FROM orders",
        "contains_agg_keyword": True
    },
    {
        "name": "Pure aggregation (no OVER) - AVG",
        "query": "SELECT AVG(amount) as avg FROM orders",
        "contains_agg_keyword": True
    },
]

print("\nTesting queries with aggregation keywords...\n")

for i, test in enumerate(edge_cases, 1):
    print(f"[Test {i}] {test['name']}")
    print(f"  Query: {test['query']}")

    # Analyze routing
    query_upper = test['query'].upper()
    has_over = 'OVER' in query_upper
    has_agg = any(kw in query_upper for kw in ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'GROUP BY'])

    print(f"  Detection: OVER={has_over}, AGG_KEYWORD={has_agg}")

    if has_over and has_agg:
        print(f"  ⚠️  CONFLICT: Both OVER and aggregation keywords present")
        print(f"     → Will route to: execute_simple_agg_gpu (WRONG for window functions!)")
    elif has_agg:
        print(f"  → Will route to: execute_simple_agg_gpu (correct)")
    else:
        print(f"  → Will route to: execute_table_scan_cpu (correct)")

    try:
        result = execute(test['query'])

        if result is None or 'result' not in result:
            print(f"  ❌ FAILED: No result returned\n")
            continue

        result_list = result['result'].to_list()
        print(f"  ✅ SUCCESS: {len(result_list)} rows")

        if result_list:
            print(f"  Sample: {result_list[0]}\n")

    except Exception as e:
        print(f"  ❌ ERROR: {e}\n")

print("=" * 70)
print("KEY FINDING")
print("=" * 70)
print("\nThe current query detection logic is:")
print("  if 'JOIN' in query → execute_join_gpu")
print("  elif 'SUM'|'COUNT'|'AVG'|'MIN'|'MAX'|'GROUP BY' in query → execute_simple_agg_gpu")
print("  else → execute_table_scan_cpu")
print("\nPotential issue: Window functions like 'SUM(...) OVER' contain 'SUM'")
print("  → They would be routed to execute_simple_agg_gpu instead of execute_table_scan_cpu")
print("\nBUT: If they're working, DataFusion might be handling them correctly anyway!")
print("=" * 70)
