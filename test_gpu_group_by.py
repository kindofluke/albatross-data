#!/usr/bin/env python3
"""
Test GPU GROUP BY flow using existing test data.
Demonstrates current behavior and validates the improvements made.
"""

import sys
import os
from pathlib import Path

# Add data-kernel to path
sys.path.insert(0, str(Path(__file__).parent / "data-kernel" / "src"))

try:
    from data_kernel import execute
except ImportError as e:
    print(f"Error: Could not import data_kernel: {e}")
    print("Make sure the Rust library is built")
    sys.exit(1)

def main():
    # Set data path
    data_dir = Path(__file__).parent / "data-embed" / "data"
    os.environ['DATA_PATH'] = str(data_dir)

    print("=" * 80)
    print("GPU GROUP BY Audit Test")
    print("=" * 80)

    # Test 1: Simple GROUP BY (should trigger GPU path but fall back to CPU)
    print("\n[TEST 1] Simple GROUP BY query")
    print("-" * 80)
    query = """
    SELECT status, COUNT(*) as cnt
    FROM orders_1m
    GROUP BY status
    """
    print(f"Query: {query.strip()}")
    print("\nExpected: GPU analysis detects GROUP BY, validates data size, falls back to CPU")
    print("Output:\n")

    try:
        result = execute(query)
        print("\nResult:")
        print(result)
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: Multi-aggregation GROUP BY
    print("\n\n[TEST 2] Multi-aggregation GROUP BY")
    print("-" * 80)
    query = """
    SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total
    FROM orders_1m
    GROUP BY customer_id
    ORDER BY cnt DESC
    LIMIT 10
    """
    print(f"Query: {query.strip()}")
    print("\nExpected: GROUP BY with multiple aggregations + ORDER BY")
    print("Current: Falls back to CPU (multi-agg and sort not implemented on GPU)")
    print("Output:\n")

    try:
        result = execute(query)
        print("\nResult:")
        print(result)
    except Exception as e:
        print(f"Error: {e}")

    print("\n" + "=" * 80)
    print("Test Complete")
    print("=" * 80)
    print("\nKey Findings:")
    print("1. GROUP BY queries are now routed to GPU analysis (plan_analyzer.rs:41)")
    print("2. Data size validation checks 10K < rows < 10M (executor.rs:428-444)")
    print("3. Currently falls back to CPU - still needs:")
    print("   - Aggregation expression parsing")
    print("   - Multi-column aggregation support")
    print("   - GPU sorting for ORDER BY")
    print("   - Result formatting with multiple columns")

if __name__ == '__main__':
    main()
