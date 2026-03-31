#!/usr/bin/env python3
"""
Check the actual data returned by window functions
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("WINDOW FUNCTION - ACTUAL DATA RETURNED")
print("=" * 70)

query = "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_sum FROM orders LIMIT 5"

print(f"\nQuery: {query}")
print("\n" + "-" * 70)

result_df = execute(query)

print(f"DataFrame shape: {result_df.shape}")
print(f"DataFrame columns: {result_df.columns.tolist()}")
print(f"Number of rows: {len(result_df)}")

print("\n" + "-" * 70)
print("RAW DATAFRAME:")
print(result_df)

print("\n" + "-" * 70)
print("EXTRACTED RESULT LIST:")
result_list = result_df['result'].to_list()
print(f"Length: {len(result_list)}")

for i, row in enumerate(result_list):
    print(f"\nRow {i}: {row}")
    print(f"  Type: {type(row)}")
    if isinstance(row, dict):
        print(f"  Keys: {row.keys()}")
        print(f"  Values: {row.values()}")

print("\n" + "=" * 70)
print("ANALYSIS")
print("=" * 70)

if len(result_list) == 5:
    first_row = result_list[0]
    if isinstance(first_row, dict) and len(first_row) == 3:
        print("\n✅ Window function returned 5 rows with 3 columns each")
        print(f"   Columns: {list(first_row.keys())}")
        print("\n→ This is CORRECT window function behavior!")
        print("→ It means the query is being executed properly")
    elif isinstance(first_row, dict) and len(first_row) == 1:
        print("\n❌ Window function returned 5 rows with only 1 column each")
        print("→ This would indicate incorrect routing to execute_simple_agg_gpu")
elif len(result_list) == 1:
    first_row = result_list[0]
    if isinstance(first_row, dict) and 'sum' in str(first_row.keys()).lower():
        print("\n❌ Got single row with sum - routed to execute_simple_agg_gpu")
        print("→ Window function was incorrectly treated as simple aggregation")

print("\n" + "=" * 70)
print("COMPARE WITH PURE AGGREGATION")
print("=" * 70)

agg_query = "SELECT SUM(amount) as total FROM orders"
print(f"\nQuery: {agg_query}")

agg_result = execute(agg_query)
agg_list = agg_result['result'].to_list()

print(f"Rows: {len(agg_list)}")
print(f"First row: {agg_list[0]}")
print(f"Columns: {list(agg_list[0].keys()) if isinstance(agg_list[0], dict) else 'N/A'}")

print("\n" + "=" * 70)
