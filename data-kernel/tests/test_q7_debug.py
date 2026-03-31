#!/usr/bin/env python3
"""
Debug Q7 value mismatch
"""
import os
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

import duckdb
from data_kernel import arrow_bridge

# Run Q7 on both engines
query = "SELECT o.customer_id, oi.product_id, sum(oi.quantity) FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id, oi.product_id"

print("DuckDB Result:")
conn = duckdb.connect(':memory:')
duckdb_result = conn.execute(f"SELECT o.customer_id, oi.product_id, sum(oi.quantity) FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id GROUP BY o.customer_id, oi.product_id").fetchdf()
print(f"  Shape: {duckdb_result.shape}")
print(f"  First 10 rows:")
print(duckdb_result.head(10))
print(f"\n  Sum check: {duckdb_result['sum(oi.quantity)'].sum()}")

print("\n" + "="*80)
print("\nGPU Result:")
gpu_result_table = arrow_bridge.execute_query(query)
gpu_result = gpu_result_table.to_pandas()
print(f"  Shape: {gpu_result.shape}")
print(f"  First 10 rows:")
print(gpu_result.head(10))
sum_col = [c for c in gpu_result.columns if 'sum' in c.lower()][0]
print(f"\n  Sum check: {gpu_result[sum_col].sum()}")

# Compare sorted results
print("\n" + "="*80)
print("\nDetailed Comparison:")
duckdb_sorted = duckdb_result.sort_values(by=['customer_id', 'product_id']).reset_index(drop=True)
gpu_sorted = gpu_result.sort_values(by=['customer_id', 'product_id']).reset_index(drop=True)

print(f"DuckDB sorted first 10:")
print(duckdb_sorted.head(10))
print(f"\nGPU sorted first 10:")
print(gpu_sorted.head(10))

# Check if values match
duckdb_sum_col = 'sum(oi.quantity)'
if duckdb_sum_col in duckdb_sorted.columns and sum_col in gpu_sorted.columns:
    matches = (duckdb_sorted[duckdb_sum_col] == gpu_sorted[sum_col]).all()
    print(f"\nValues match: {matches}")
    if not matches:
        diff = duckdb_sorted[['customer_id', 'product_id', duckdb_sum_col]].merge(
            gpu_sorted[['customer_id', 'product_id', sum_col]],
            on=['customer_id', 'product_id'],
            how='outer',
            suffixes=('_duckdb', '_gpu')
        )
        diff['mismatch'] = diff[f'{duckdb_sum_col}_duckdb'] != diff[f'{sum_col}_gpu']
        mismatches = diff[diff['mismatch']]
        print(f"\nMismatches ({len(mismatches)} rows):")
        print(mismatches.head(20))
