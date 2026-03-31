#!/usr/bin/env python3
"""
Test string column handling
"""
import os
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

from data_kernel import arrow_bridge
import duckdb

# Verify the data has string values
conn = duckdb.connect(':memory:')
conn.execute("CREATE TABLE orders AS SELECT * FROM 'orders.parquet'")
print("DuckDB verification:")
print(conn.execute("SELECT status, COUNT(*) FROM orders GROUP BY status").fetchdf())
print("\n" + "="*60)

# Test different queries with string columns
test_queries = [
    ("SELECT string column", "SELECT status FROM orders LIMIT 5"),
    ("COUNT distinct strings", "SELECT COUNT(DISTINCT status) FROM orders"),
    ("GROUP BY string", "SELECT status, COUNT(*) FROM orders GROUP BY status"),
    ("WHERE on string", "SELECT COUNT(*) FROM orders WHERE status = 'shipped'"),
]

for name, query in test_queries:
    print(f"\n{name}: {query}")
    try:
        result = arrow_bridge.execute_query(query)
        if result:
            df = result.to_pandas()
            print(f"  ✓ Result ({len(df)} rows):")
            print(df.head())
        else:
            print(f"  ⚠ Empty result")
    except Exception as e:
        print(f"  ✗ Error: {e}")
