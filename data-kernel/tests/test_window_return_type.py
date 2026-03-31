#!/usr/bin/env python3
"""
Check what execute() actually returns for window functions
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("CHECKING execute() RETURN TYPE")
print("=" * 70)

queries = [
    ("Window function", "SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_sum FROM orders LIMIT 5"),
    ("Pure aggregation", "SELECT SUM(amount) as total FROM orders"),
    ("Table scan", "SELECT id, amount FROM orders LIMIT 5"),
]

for name, query in queries:
    print(f"\n{name}:")
    print(f"  Query: {query}")

    result = execute(query)

    print(f"  Type: {type(result)}")
    print(f"  Type name: {type(result).__name__}")

    # Check if it's a dict or DataFrame
    if isinstance(result, dict):
        print(f"  Keys: {result.keys()}")
        if 'result' in result:
            inner_result = result['result']
            print(f"  result['result'] type: {type(inner_result)}")
            if hasattr(inner_result, 'to_list'):
                result_list = inner_result.to_list()
                print(f"  Rows: {len(result_list)}")
                if result_list:
                    print(f"  First row: {result_list[0]}")
    else:
        # Assume it's a DataFrame or similar
        print(f"  Shape: {result.shape if hasattr(result, 'shape') else 'N/A'}")
        print(f"  Columns: {result.columns.tolist() if hasattr(result, 'columns') else 'N/A'}")
        if hasattr(result, 'head'):
            print(f"  First row:\n{result.head(1)}")

print("\n" + "=" * 70)
