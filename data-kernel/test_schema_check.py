#!/usr/bin/env python3
"""
Check actual schema of test data
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("SCHEMA VERIFICATION")
print("=" * 70)

# Get actual schema
print("\n[ORDERS TABLE]")
result = execute("SELECT * FROM orders LIMIT 1")
row = result['result'].to_list()[0]
print(f"Columns: {list(row.keys())}")
print(f"Sample data: {row}")

print("\n[ORDER_ITEMS TABLE]")
result = execute("SELECT * FROM order_items LIMIT 1")
row = result['result'].to_list()[0]
print(f"Columns: {list(row.keys())}")
print(f"Sample data: {row}")

print("\n" + "=" * 70)
print("ISSUE IDENTIFIED")
print("=" * 70)
print("\nYour query uses:")
print("  - oi.order_id (DOESN'T EXIST)")
print("  - oi.product_id (DOESN'T EXIST)")

print("\nActual schema has:")
print("  - Both tables: id, customer_id, amount, quantity, status")
print("  - NO order_id or product_id columns!")

print("\n" + "=" * 70)
print("CORRECTED QUERY TEST")
print("=" * 70)

# Test with correct column names
corrected_query = """
SELECT o.customer_id, oi.id, sum(oi.quantity)
FROM orders o
JOIN order_items oi ON o.id = oi.id
GROUP BY o.customer_id, oi.id
LIMIT 10
"""

print(f"\nCorrected query: {corrected_query.strip()}")

try:
    result = execute(corrected_query)
    result_list = result['result'].to_list()
    print(f"✅ SUCCESS: {len(result_list)} rows")
    for i, row in enumerate(result_list[:3]):
        print(f"  Row {i}: {row}")
except Exception as e:
    print(f"❌ FAILED: {e}")

# Try simpler JOIN that should work
print("\n" + "=" * 70)
print("SIMPLE JOIN TEST (from earlier tests)")
print("=" * 70)

simple_join = "SELECT o.id, o.customer_id, oi.amount FROM orders o JOIN order_items oi ON o.customer_id = oi.customer_id LIMIT 10"
print(f"\nQuery: {simple_join}")

try:
    result = execute(simple_join)
    result_list = result['result'].to_list()
    print(f"✅ SUCCESS: {len(result_list)} rows")
    print(f"First row: {result_list[0]}")
except Exception as e:
    print(f"❌ FAILED: {e}")

print("\n" + "=" * 70)
