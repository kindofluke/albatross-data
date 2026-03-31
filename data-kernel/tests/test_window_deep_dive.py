#!/usr/bin/env python3
"""
Deep dive into what's actually happening with window functions
"""
import os
import sys
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("WINDOW FUNCTIONS - DEEP DIVE")
print("=" * 70)

# Test with verbose output to see routing
test_queries = [
    ("Window function", "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_sum FROM orders LIMIT 5"),
    ("Pure aggregation", "SELECT SUM(amount) as total FROM orders"),
    ("Table scan", "SELECT id, amount FROM orders LIMIT 5"),
]

for name, query in test_queries:
    print(f"\n{'=' * 70}")
    print(f"TEST: {name}")
    print(f"Query: {query}")
    print('=' * 70)

    try:
        result = execute(query)

        if result and 'result' in result:
            result_list = result['result'].to_list()
            print(f"\n✅ Result: {len(result_list)} rows")

            # Show all rows for small results
            if len(result_list) <= 5:
                print("\nFull result:")
                for i, row in enumerate(result_list):
                    print(f"  Row {i}: {row}")
        else:
            print("❌ No result returned")

    except Exception as e:
        print(f"❌ Error: {e}")

# Now test if execute_simple_agg_gpu is actually being called
print("\n" + "=" * 70)
print("CRITICAL TEST: What happens with window functions?")
print("=" * 70)

print("\nAccording to the Rust code, execute_simple_agg_gpu:")
print("  1. Calls datafusion::physical_plan::collect() to execute the query")
print("  2. Takes the first batch")
print("  3. Extracts column 0")
print("  4. Sends it to GPU for SUM aggregation")
print("  5. Returns a SINGLE ROW with a SINGLE COLUMN (the sum)")

print("\nBut window functions return MULTIPLE ROWS and MULTIPLE COLUMNS!")

print("\nMy hypothesis:")
print("  DataFusion executes window functions during the collect() call (line 177-178)")
print("  But then execute_simple_agg_gpu tries to extract column 0 and sum it (line 182-186)")
print("  This should FAIL or return wrong results for window functions...")

print("\nLet me verify by checking the actual result structure:")

query = "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_sum FROM orders LIMIT 5"
result = execute(query)

if result and 'result' in result:
    result_list = result['result'].to_list()
    print(f"\n  Rows returned: {len(result_list)}")
    print(f"  Columns in first row: {list(result_list[0].keys())}")
    print(f"  Values in first row: {result_list[0]}")

    if len(result_list) == 5 and len(result_list[0].keys()) == 3:
        print("\n  ✅ We got 5 rows with 3 columns (id, amount, running_sum)")
        print("  → This means it's NOT going through execute_simple_agg_gpu!")
        print("  → It must be going through execute_table_scan_cpu!")
    elif len(result_list) == 1 and len(result_list[0].keys()) == 1:
        print("\n  ❌ We got 1 row with 1 column")
        print("  → This means it went through execute_simple_agg_gpu (wrong!)")

print("\n" + "=" * 70)
