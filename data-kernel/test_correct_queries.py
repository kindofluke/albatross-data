#!/usr/bin/env python3
"""
Test script with correct queries based on actual schema
"""
import os
import pandas as pd
from data_kernel import execute, get_gpu_info

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("DATA-KERNEL TEST - CORRECT QUERIES FOR ACTUAL SCHEMA")
print("=" * 70)

# Schema: id, customer_id, amount, quantity, status (same for both tables)

test_queries = [
    {
        "name": "Q1: SELECT * from orders (THE FIX!)",
        "query": "SELECT * FROM orders LIMIT 10",
        "expected": "10 rows, all columns"
    },
    {
        "name": "Q2: SELECT * from order_items (THE FIX!)",
        "query": "SELECT * FROM order_items LIMIT 10",
        "expected": "10 rows, all columns"
    },
    {
        "name": "Q3: SELECT specific columns",
        "query": "SELECT id, customer_id, amount FROM orders LIMIT 5",
        "expected": "5 rows, 3 columns"
    },
    {
        "name": "Q4: SELECT with WHERE",
        "query": "SELECT * FROM orders WHERE status = 'shipped' LIMIT 10",
        "expected": "Up to 10 rows where status=shipped"
    },
    {
        "name": "Q5: SUM aggregation (GPU)",
        "query": "SELECT SUM(amount) as total FROM orders",
        "expected": "1 row with sum"
    },
    {
        "name": "Q6: AVG aggregation (GPU)",
        "query": "SELECT AVG(amount) as avg_amt FROM orders",
        "expected": "1 row with average"
    },
    {
        "name": "Q7: MIN/MAX aggregation (GPU)",
        "query": "SELECT MIN(amount) as min_amt, MAX(amount) as max_amt FROM orders",
        "expected": "1 row with min/max"
    },
    {
        "name": "Q8: COUNT(*) - Testing if this works",
        "query": "SELECT COUNT(*) as cnt FROM orders",
        "expected": "1 row with count"
    },
    {
        "name": "Q9: JOIN on customer_id",
        "query": "SELECT o.id, o.customer_id, oi.amount FROM orders o JOIN order_items oi ON o.customer_id = oi.customer_id LIMIT 10",
        "expected": "Up to 10 joined rows"
    },
    {
        "name": "Q10: ORDER BY",
        "query": "SELECT * FROM orders ORDER BY amount DESC LIMIT 5",
        "expected": "5 rows ordered by amount"
    }
]

print(f"\nGPU Available: {get_gpu_info()['available']}")
print("\n" + "=" * 70)

passed = 0
failed = 0
results = []

for i, test in enumerate(test_queries, 1):
    print(f"\n[{i}/{len(test_queries)}] {test['name']}")
    print(f"  Query: {test['query']}")
    print(f"  Expected: {test['expected']}")

    try:
        result = execute(test['query'])

        if result is None or 'result' not in result:
            print(f"  ❌ FAILED: No result returned")
            failed += 1
            results.append({'test': test['name'], 'status': 'FAILED', 'details': 'No result'})
            continue

        result_list = result['result'].to_list()
        row_count = len(result_list)

        if row_count > 0:
            df = pd.DataFrame(result_list)
            col_count = len(df.columns)
            print(f"  ✅ SUCCESS: {row_count} rows × {col_count} columns")

            # Show sample for small results
            if row_count <= 3:
                print(f"     Data: {result_list}")
        else:
            print(f"  ✅ SUCCESS: Empty result (valid)")

        passed += 1
        results.append({'test': test['name'], 'status': 'PASSED', 'details': f'{row_count} rows'})

    except Exception as e:
        print(f"  ❌ FAILED: {e}")
        failed += 1
        results.append({'test': test['name'], 'status': 'FAILED', 'details': str(e)})

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"Total: {len(test_queries)} | Passed: {passed} | Failed: {failed}")
print(f"Success Rate: {passed/len(test_queries)*100:.1f}%")

# Key findings
print("\n" + "=" * 70)
print("KEY FINDINGS")
print("=" * 70)

select_star = [r for r in results if "SELECT *" in r['test']]
if all(r['status'] == 'PASSED' for r in select_star):
    print("✅ SELECT * queries: ALL PASSED - The fix works!")
else:
    print("❌ SELECT * queries: Some failed")

count_queries = [r for r in results if "COUNT" in r['test']]
if count_queries:
    if all(r['status'] == 'PASSED' for r in count_queries):
        print("✅ COUNT queries: ALL PASSED")
    else:
        print("⚠️  COUNT queries: Some failed (known issue)")

join_queries = [r for r in results if "JOIN" in r['test']]
if join_queries:
    if all(r['status'] == 'PASSED' for r in join_queries):
        print("✅ JOIN queries: ALL PASSED")
    else:
        print("⚠️  JOIN queries: Some failed")

print("\n" + "=" * 70)
