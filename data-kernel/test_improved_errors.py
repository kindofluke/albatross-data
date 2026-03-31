#!/usr/bin/env python3
"""
Test improved error messages
"""
import os
import sys

# Force reload to get the newly built version
if 'data_kernel' in sys.modules:
    del sys.modules['data_kernel']
if 'data_kernel.execute' in sys.modules:
    del sys.modules['data_kernel.execute']
if 'data_kernel.arrow_bridge' in sys.modules:
    del sys.modules['data_kernel.arrow_bridge']

from data_kernel import execute

os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

print("=" * 70)
print("TESTING IMPROVED ERROR MESSAGES")
print("=" * 70)

# Test queries that should fail with specific errors
test_cases = [
    {
        "name": "Non-existent column",
        "query": "SELECT nonexistent_column FROM orders",
        "expected_error": "column"
    },
    {
        "name": "Non-existent table",
        "query": "SELECT * FROM fake_table",
        "expected_error": "table"
    },
    {
        "name": "JOIN with non-existent column",
        "query": "SELECT o.customer_id, oi.product_id FROM orders o JOIN order_items oi ON o.id = oi.order_id",
        "expected_error": "column"
    },
    {
        "name": "Invalid SQL syntax",
        "query": "SELECT FROM orders",
        "expected_error": "parse"
    },
    {
        "name": "Type mismatch",
        "query": "SELECT id + 'abc' FROM orders",
        "expected_error": None  # Any descriptive error
    }
]

print("\n")
for i, test in enumerate(test_cases, 1):
    print(f"[Test {i}] {test['name']}")
    print(f"  Query: {test['query']}")

    try:
        result = execute(test['query'])
        print(f"  ❌ UNEXPECTED: Query succeeded (should have failed)")
    except RuntimeError as e:
        error_msg = str(e)
        print(f"  ✅ Error caught: {error_msg}")

        # Check if error message is descriptive
        if test['expected_error']:
            if test['expected_error'].lower() in error_msg.lower():
                print(f"     → Contains expected keyword '{test['expected_error']}'")
            else:
                print(f"     → Warning: doesn't contain expected keyword '{test['expected_error']}'")

        # Check if it's more than just error code -5
        if "error code:" in error_msg.lower() and "Query execution failed:" not in error_msg:
            print(f"     ⚠️  Still using old error format")
        else:
            print(f"     ✓ Using improved error format")
    except Exception as e:
        print(f"  ❌ Unexpected error type: {type(e).__name__}: {e}")

    print()

print("=" * 70)
print("SUMMARY")
print("=" * 70)
print("\nIf error messages now show 'SQL Error (code -5): Query execution failed: ...'")
print("with detailed DataFusion error messages, the improvement is working!")
print("\nOld format: 'Failed to execute query in Rust (error code: -5)'")
print("New format: 'SQL Error (code -5): Query execution failed: [detailed error]'")
print("=" * 70)
