#!/usr/bin/env python3
"""
Compare DuckDB vs GPU execution for SQL queries
"""
import os
import sys
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

# Set DATA_PATH for testing
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-kernel'

from data_kernel import arrow_bridge

def create_test_data():
    """Create test parquet files for orders and order_items"""
    print("=" * 80)
    print("Creating Test Data")
    print("=" * 80)

    # Create orders dataset
    orders_data = {
        'id': list(range(1, 10001)),
        'customer_id': [i % 100 for i in range(1, 10001)],
        'status': ['shipped' if i % 3 == 0 else 'pending' if i % 3 == 1 else 'cancelled' for i in range(1, 10001)],
        'amount': [50.0 + (i % 500) * 1.5 for i in range(1, 10001)]
    }
    orders_df = pd.DataFrame(orders_data)
    orders_path = 'orders.parquet'
    orders_df.to_parquet(orders_path, index=False)
    print(f"✓ Created {orders_path} with {len(orders_df)} rows")

    # Create order_items dataset
    order_items_data = {
        'id': list(range(1, 25001)),
        'order_id': [1 + (i % 10000) for i in range(25000)],
        'product_id': [i % 50 for i in range(25000)],
        'quantity': [1 + (i % 10) for i in range(25000)],
        'price': [10.0 + (i % 100) * 2.0 for i in range(25000)]
    }
    order_items_df = pd.DataFrame(order_items_data)
    order_items_path = 'order_items.parquet'
    order_items_df.to_parquet(order_items_path, index=False)
    print(f"✓ Created {order_items_path} with {len(order_items_df)} rows")

    return orders_path, order_items_path

def run_duckdb_query(query, orders_path, order_items_path):
    """Execute a query using DuckDB"""
    # Replace table names with file paths for DuckDB
    duckdb_query = query.replace("'orders.parquet'", f"'{orders_path}'")
    duckdb_query = duckdb_query.replace("'order_items.parquet'", f"'{order_items_path}'")

    start_time = time.time()
    try:
        conn = duckdb.connect(':memory:')
        result = conn.execute(duckdb_query).fetchdf()
        elapsed = time.time() - start_time
        conn.close()
        return result, elapsed, None
    except Exception as e:
        elapsed = time.time() - start_time
        return None, elapsed, str(e)

def run_gpu_query(query, orders_path, order_items_path):
    """Execute a query using data-kernel GPU"""
    # Replace file paths with table names for data-kernel
    gpu_query = query.replace(f"'{orders_path}'", "orders")
    gpu_query = gpu_query.replace(f"'{order_items_path}'", "order_items")
    gpu_query = gpu_query.replace("'orders.parquet'", "orders")
    gpu_query = gpu_query.replace("'order_items.parquet'", "order_items")

    start_time = time.time()
    try:
        result_table = arrow_bridge.execute_query(gpu_query)
        if result_table:
            result = result_table.to_pandas()
            elapsed = time.time() - start_time
            return result, elapsed, None
        else:
            elapsed = time.time() - start_time
            return None, elapsed, "Empty result"
    except Exception as e:
        elapsed = time.time() - start_time
        return None, elapsed, str(e)

def compare_results(duckdb_result, gpu_result):
    """Compare two dataframes"""
    if duckdb_result is None or gpu_result is None:
        return False, "One or both queries failed"

    try:
        # Compare shapes first
        if duckdb_result.shape != gpu_result.shape:
            return False, f"Shape mismatch: DuckDB {duckdb_result.shape} vs GPU {gpu_result.shape}"

        # Normalize column names for comparison
        # DuckDB uses count_star(), DataFusion uses count(*)
        # DuckDB uses avg(amount), DataFusion uses avg(table.amount)
        def normalize_col_name(col):
            # Remove table prefixes, convert count_star() to count(*)
            col_lower = col.lower()
            col_lower = col_lower.replace('count_star()', 'count(*)')
            # Remove table prefixes like "orders." or "o."
            import re
            col_lower = re.sub(r'\b\w+\.', '', col_lower)
            # Normalize window function names (rank() over... vs rank() partition by...)
            # Extract just the function name for window functions
            if 'over' in col_lower or 'partition by' in col_lower:
                # Extract function name (e.g., "rank()" from "rank() over (partition...)")
                match = re.match(r'^(\w+\(\))', col_lower)
                if match:
                    col_lower = match.group(1)
            return col_lower

        duckdb_normalized = {normalize_col_name(c): c for c in duckdb_result.columns}
        gpu_normalized = {normalize_col_name(c): c for c in gpu_result.columns}

        if set(duckdb_normalized.keys()) != set(gpu_normalized.keys()):
            return False, f"Column mismatch: DuckDB {list(duckdb_normalized.keys())} vs GPU {list(gpu_normalized.keys())}"

        # Create column mapping
        col_mapping = {}
        for norm_col in duckdb_normalized.keys():
            col_mapping[duckdb_normalized[norm_col]] = gpu_normalized[norm_col]

        # Sort both dataframes by all columns for order-independent comparison
        duckdb_sorted = duckdb_result.sort_values(by=list(duckdb_result.columns)).reset_index(drop=True)
        gpu_sorted = gpu_result.sort_values(by=list(gpu_result.columns)).reset_index(drop=True)

        # Compare values column by column
        for duckdb_col, gpu_col in col_mapping.items():
            duckdb_vals = duckdb_sorted[duckdb_col]
            gpu_vals = gpu_sorted[gpu_col]

            if pd.api.types.is_numeric_dtype(duckdb_vals) and pd.api.types.is_numeric_dtype(gpu_vals):
                # Convert both to float for numeric comparison (handles int vs float differences)
                duckdb_float = duckdb_vals.astype(float)
                gpu_float = gpu_vals.astype(float)

                # Use approximate comparison with tolerance
                if not pd.Series(duckdb_float).round(6).equals(pd.Series(gpu_float).round(6)):
                    # Check element-wise with tolerance
                    import numpy as np
                    if not np.allclose(duckdb_float, gpu_float, rtol=1e-5, atol=1e-8, equal_nan=True):
                        return False, f"Value mismatch in column {duckdb_col}"
            else:
                # String comparison
                if not duckdb_vals.equals(gpu_vals):
                    return False, f"Value mismatch in column {duckdb_col}"

        return True, "Results match"
    except Exception as e:
        return False, f"Comparison error: {str(e)}"

def run_tests():
    """Run all test queries"""
    # Create test data
    orders_path, order_items_path = create_test_data()

    # Note: Q7 appears to be cut off in the original, so completing it
    queries = [
        {
            "name": "Q1: Filtered Stats",
            "duckdb_sql": "SELECT count(*), avg(amount) FROM 'orders.parquet' WHERE status = 'shipped'",
            "data_kernel_sql": "SELECT count(*), avg(amount) FROM orders WHERE status = 'shipped'"
        },
        {
            "name": "Q2: Whole Table Stats",
            "duckdb_sql": "SELECT min(amount), max(amount), avg(amount) FROM 'orders.parquet'",
            "data_kernel_sql": "SELECT min(amount), max(amount), avg(amount) FROM orders"
        },
        {
            "name": "Q3: Group By Status",
            "duckdb_sql": "SELECT status, count(*) FROM 'orders.parquet' GROUP BY status",
            "data_kernel_sql": "SELECT status, count(*) FROM orders GROUP BY status"
        },
        {
            "name": "Q4: Group By Customer",
            "duckdb_sql": "SELECT customer_id, sum(amount) FROM 'orders.parquet' GROUP BY customer_id",
            "data_kernel_sql": "SELECT customer_id, sum(amount) FROM orders GROUP BY customer_id"
        },
        {
            "name": "Q5: Window Function",
            "duckdb_sql": "SELECT customer_id, amount, rank() OVER (PARTITION BY customer_id ORDER BY amount DESC) FROM 'orders.parquet'",
            "data_kernel_sql": "SELECT customer_id, amount, rank() OVER (PARTITION BY customer_id ORDER BY amount DESC) FROM orders"
        },
        {
            "name": "Q6: Join with Stats",
            "duckdb_sql": "SELECT count(*), avg(o.amount) FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id",
            "data_kernel_sql": "SELECT count(*), avg(o.amount) FROM orders o JOIN order_items oi ON o.id = oi.order_id"
        },
        {
            "name": "Q7: Join with Group By Customer/Product",
            "duckdb_sql": "SELECT o.customer_id, oi.product_id, sum(oi.quantity) FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id GROUP BY o.customer_id, oi.product_id",
            "data_kernel_sql": "SELECT o.customer_id, oi.product_id, sum(oi.quantity) FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id, oi.product_id"
        }
    ]

    print("\n" + "=" * 80)
    print("Running Query Comparison Tests")
    print("=" * 80)

    results_summary = []

    for i, query_pair in enumerate(queries, 1):
        print(f"\n{'-' * 80}")
        print(f"Test {i}: {query_pair['name']}")
        print(f"{'-' * 80}")

        # Run DuckDB query
        print(f"\nDuckDB SQL: {query_pair['duckdb_sql']}")
        duckdb_result, duckdb_time, duckdb_error = run_duckdb_query(
            query_pair['duckdb_sql'], orders_path, order_items_path
        )

        if duckdb_error:
            print(f"  ✗ DuckDB Error: {duckdb_error}")
        else:
            print(f"  ✓ DuckDB Success: {duckdb_time:.4f}s")
            print(f"    Result shape: {duckdb_result.shape}")
            if len(duckdb_result) <= 10:
                print(f"    Result preview:\n{duckdb_result}")

        # Run GPU query
        print(f"\nGPU SQL: {query_pair['data_kernel_sql']}")
        gpu_result, gpu_time, gpu_error = run_gpu_query(
            query_pair['data_kernel_sql'], orders_path, order_items_path
        )

        if gpu_error:
            print(f"  ✗ GPU Error: {gpu_error}")
        else:
            print(f"  ✓ GPU Success: {gpu_time:.4f}s")
            print(f"    Result shape: {gpu_result.shape}")
            if len(gpu_result) <= 10:
                print(f"    Result preview:\n{gpu_result}")

        # Compare results
        if not duckdb_error and not gpu_error:
            match, message = compare_results(duckdb_result, gpu_result)
            if match:
                print(f"\n  ✓ Results Match!")
                speedup = duckdb_time / gpu_time if gpu_time > 0 else float('inf')
                print(f"  Performance: DuckDB {duckdb_time:.4f}s vs GPU {gpu_time:.4f}s (Speedup: {speedup:.2f}x)")
                results_summary.append({
                    'query': query_pair['name'],
                    'status': 'PASS',
                    'duckdb_time': duckdb_time,
                    'gpu_time': gpu_time,
                    'speedup': speedup
                })
            else:
                print(f"\n  ✗ Results Don't Match: {message}")
                results_summary.append({
                    'query': query_pair['name'],
                    'status': 'FAIL',
                    'duckdb_time': duckdb_time,
                    'gpu_time': gpu_time,
                    'speedup': 0,
                    'error': message
                })
        else:
            results_summary.append({
                'query': query_pair['name'],
                'status': 'ERROR',
                'duckdb_time': duckdb_time if not duckdb_error else 0,
                'gpu_time': gpu_time if not gpu_error else 0,
                'speedup': 0,
                'error': duckdb_error or gpu_error
            })

    # Print summary
    print("\n" + "=" * 80)
    print("Test Summary")
    print("=" * 80)

    for result in results_summary:
        status_symbol = "✓" if result['status'] == 'PASS' else "✗"
        print(f"\n{status_symbol} {result['query']}: {result['status']}")
        if result['status'] == 'PASS':
            print(f"  DuckDB: {result['duckdb_time']:.4f}s | GPU: {result['gpu_time']:.4f}s | Speedup: {result['speedup']:.2f}x")
        elif 'error' in result:
            print(f"  Error: {result['error']}")

    passed = sum(1 for r in results_summary if r['status'] == 'PASS')
    total = len(results_summary)
    print(f"\n{'=' * 80}")
    print(f"Overall: {passed}/{total} queries passed")
    print(f"{'=' * 80}")

if __name__ == '__main__':
    run_tests()
