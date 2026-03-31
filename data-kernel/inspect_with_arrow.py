#!/usr/bin/env python3
"""
Inspect test data using PyArrow
"""
import pyarrow.parquet as pq
import pandas as pd

print("=" * 70)
print("INSPECTING TEST DATA WITH PYARROW")
print("=" * 70)

# Check orders table
print("\n[ORDERS TABLE]")
table = pq.read_table('/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet')
print(f"Row count: {table.num_rows}")
print(f"Columns: {table.column_names}")
print(f"Schema:\n{table.schema}")
df = table.to_pandas()
print(f"\nFirst 3 rows:")
print(df.head(3))
print(f"\nData types:")
print(df.dtypes)

# Check order_items table
print("\n\n[ORDER_ITEMS TABLE]")
table = pq.read_table('/Users/luke.shulman/Projects/albatross-data/data-embed/data/order_items.parquet')
print(f"Row count: {table.num_rows}")
print(f"Columns: {table.column_names}")
print(f"Schema:\n{table.schema}")
df = table.to_pandas()
print(f"\nFirst 3 rows:")
print(df.head(3))

# Check for common ID values for join testing
print("\n\n[JOIN COMPATIBILITY CHECK]")
orders_df = pq.read_table('/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet').to_pandas()
items_df = pq.read_table('/Users/luke.shulman/Projects/albatross-data/data-embed/data/order_items.parquet').to_pandas()
print(f"Orders IDs: {orders_df['id'].unique()[:10]}")
print(f"Order Items order_ids: {items_df['order_id'].unique()[:10]}")
common_ids = set(orders_df['id']) & set(items_df['order_id'])
print(f"Common IDs for JOIN: {len(common_ids)} (sample: {list(common_ids)[:5]})")
