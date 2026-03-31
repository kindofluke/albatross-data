#!/usr/bin/env python3
"""
Test the original failing query with improved error messages
"""
import os
from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("ORIGINAL FAILING QUERY - WITH IMPROVED ERROR MESSAGE")
print("=" * 70)

query = """
SELECT o.customer_id, oi.product_id, sum(oi.quantity)
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
GROUP BY o.customer_id, oi.product_id
"""

print(f"\nQuery: {query.strip()}")
print("\n" + "-" * 70)

try:
    result = execute(query)
    result_list = result['result'].to_list()
    print(f"✅ SUCCESS: {len(result_list)} rows")
except Exception as e:
    print(f"❌ Error: {e}")
    print("\n" + "-" * 70)
    print("ANALYSIS:")
    print("-" * 70)
    error_msg = str(e)

    if "oi.product_id" in error_msg or "oi.order_id" in error_msg:
        print("\n✅ Error message now clearly shows which columns don't exist!")
        print("\nThe error should tell you:")
        print("  - Which field is missing (oi.product_id or oi.order_id)")
        print("  - What the valid fields are")
        print("\nThis makes it much easier to debug!")

print("\n" + "=" * 70)
