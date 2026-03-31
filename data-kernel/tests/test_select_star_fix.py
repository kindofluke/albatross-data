#!/usr/bin/env python3
"""
Test script to verify SELECT * fix for data-kernel
"""
import os
import sys
import pandas as pd
from data_kernel import execute, get_gpu_info, is_gpu_available

# Set DATA_PATH to test data location
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("DATA-KERNEL SELECT * FIX - COMPREHENSIVE TEST SUITE")
print("=" * 70)

# Check GPU availability
print("\n[GPU INFO]")
gpu_info = get_gpu_info()
print(f"  GPU Available: {gpu_info['available']}")
if gpu_info['available']:
    print(f"  Device: {gpu_info['name']}")
    print(f"  Backend: {gpu_info['backend']}")

# Test queries
test_queries = [
    {
        "name": "Q1: SELECT * (The bug we fixed!)",
        "query": "SELECT * FROM orders LIMIT 10",
        "expected_path": "CPU table scan",
        "should_fail_before_fix": True
    },
    {
        "name": "Q2: SELECT * with LIMIT",
        "query": "SELECT * FROM orders LIMIT 100",
        "expected_path": "CPU table scan",
        "should_fail_before_fix": True
    },
    {
        "name": "Q3: SELECT specific columns",
        "query": "SELECT id, customer_id, amount FROM orders LIMIT 20",
        "expected_path": "CPU table scan",
        "should_fail_before_fix": True
    },
    {
        "name": "Q4: COUNT aggregation",
        "query": "SELECT COUNT(*) as cnt FROM orders",
        "expected_path": "GPU aggregation",
        "should_fail_before_fix": False
    },
    {
        "name": "Q5: SUM aggregation",
        "query": "SELECT SUM(amount) as total FROM orders",
        "expected_path": "GPU aggregation",
        "should_fail_before_fix": False
    },
    {
        "name": "Q6: AVG aggregation",
        "query": "SELECT AVG(amount) as avg_amt FROM orders",
        "expected_path": "GPU aggregation",
        "should_fail_before_fix": False
    },
    {
        "name": "Q7: MIN/MAX aggregation",
        "query": "SELECT MIN(amount) as min_amt, MAX(amount) as max_amt FROM orders",
        "expected_path": "GPU aggregation",
        "should_fail_before_fix": False
    },
    {
        "name": "Q8: JOIN query",
        "query": "SELECT o.id, oi.product_id FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 50",
        "expected_path": "CPU (JOIN)",
        "should_fail_before_fix": False
    },
    {
        "name": "Q9: JOIN with COUNT",
        "query": "SELECT COUNT(*) FROM orders o JOIN order_items oi ON o.id = oi.order_id",
        "expected_path": "CPU (JOIN)",
        "should_fail_before_fix": False
    },
    {
        "name": "Q10: SELECT * from other table",
        "query": "SELECT * FROM order_items LIMIT 10",
        "expected_path": "CPU table scan",
        "should_fail_before_fix": True
    }
]

print("\n" + "=" * 70)
print("RUNNING TESTS")
print("=" * 70)

passed = 0
failed = 0
results = []

for i, test in enumerate(test_queries, 1):
    print(f"\n[Test {i}/{len(test_queries)}] {test['name']}")
    print(f"  Query: {test['query']}")
    print(f"  Expected path: {test['expected_path']}")

    try:
        result = execute(test['query'])

        if result is None or 'result' not in result:
            print(f"  ❌ FAILED: No result returned")
            failed += 1
            results.append({
                'test': test['name'],
                'status': 'FAILED',
                'error': 'No result returned',
                'rows': 0
            })
            continue

        # Convert to list and count
        result_list = result['result'].to_list()
        row_count = len(result_list)

        # Try to convert to DataFrame to show structure
        if row_count > 0:
            df = pd.DataFrame(result_list)
            col_count = len(df.columns)
            print(f"  ✅ SUCCESS: {row_count} rows × {col_count} columns")

            # Show first few rows for SELECT * queries
            if "SELECT *" in test['query'] and row_count <= 5:
                print(f"\n  Sample data:")
                for col in df.columns[:5]:  # Show first 5 columns
                    print(f"    {col}: {df[col].tolist()[:3]}")
        else:
            print(f"  ✅ SUCCESS: Empty result (valid)")

        passed += 1
        results.append({
            'test': test['name'],
            'status': 'PASSED',
            'error': None,
            'rows': row_count
        })

    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ FAILED: {error_msg}")

        # Check if this was expected to fail before fix
        if test['should_fail_before_fix'] and "error code: -5" in error_msg:
            print(f"  ⚠️  This would have failed before the fix!")

        failed += 1
        results.append({
            'test': test['name'],
            'status': 'FAILED',
            'error': error_msg,
            'rows': 0
        })

# Summary
print("\n" + "=" * 70)
print("TEST SUMMARY")
print("=" * 70)
print(f"  Total tests: {len(test_queries)}")
print(f"  ✅ Passed: {passed}")
print(f"  ❌ Failed: {failed}")
print(f"  Success rate: {passed/len(test_queries)*100:.1f}%")

# Detailed results table
print("\n" + "=" * 70)
print("DETAILED RESULTS")
print("=" * 70)
results_df = pd.DataFrame(results)
print(results_df.to_markdown(index=False))

# Check critical fix
print("\n" + "=" * 70)
print("CRITICAL FIX VALIDATION")
print("=" * 70)
select_star_tests = [r for r in results if "SELECT *" in r['test']]
if all(r['status'] == 'PASSED' for r in select_star_tests):
    print("  ✅ ALL SELECT * queries PASSED - Fix is working!")
else:
    print("  ❌ Some SELECT * queries failed - Fix may have issues")

sys.exit(0 if failed == 0 else 1)
