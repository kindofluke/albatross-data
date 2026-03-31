#!/usr/bin/env python3
"""
Inspect test data to understand structure
"""
import duckdb

print("=" * 70)
print("INSPECTING TEST DATA")
print("=" * 70)

# Check orders table
print("\n[ORDERS TABLE]")
result = duckdb.sql("SELECT * FROM '/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet' LIMIT 3").df()
print(result)
print(f"\nRow count: {duckdb.sql('SELECT COUNT(*) FROM \"/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet\"').df()['count_star()'][0]}")
print(f"Schema: {result.dtypes}")

# Check order_items table
print("\n\n[ORDER_ITEMS TABLE]")
result = duckdb.sql("SELECT * FROM '/Users/luke.shulman/Projects/albatross-data/data-embed/data/order_items.parquet' LIMIT 3").df()
print(result)
print(f"\nRow count: {duckdb.sql('SELECT COUNT(*) FROM \"/Users/luke.shulman/Projects/albatross-data/data-embed/data/order_items.parquet\"').df()['count_star()'][0]}")
print(f"Schema: {result.dtypes}")

# Test COUNT with DuckDB
print("\n\n[TESTING COUNT WITH DUCKDB]")
print("SELECT COUNT(*) FROM orders:")
result = duckdb.sql("SELECT COUNT(*) as cnt FROM '/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet'").df()
print(result)
print(f"Result type: {type(result['cnt'][0])}")

# Test JOIN with DuckDB
print("\n\n[TESTING JOIN WITH DUCKDB]")
print("SELECT * FROM orders o JOIN order_items oi ON o.id = oi.order_id LIMIT 3:")
result = duckdb.sql("""
    SELECT * FROM '/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet' o
    JOIN '/Users/luke.shulman/Projects/albatross-data/data-embed/data/order_items.parquet' oi
    ON o.id = oi.order_id LIMIT 3
""").df()
print(result)
print(f"\nJoin result columns: {result.columns.tolist()}")
