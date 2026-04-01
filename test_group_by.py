#!/usr/bin/env python3
"""
Test script to demonstrate GPU GROUP BY behavior with the sample query:
SELECT product_id, COUNT(id) as cnt, SUM(quantity) as qty
FROM order_items
GROUP BY product_id
ORDER BY cnt
"""

import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
import os
import sys

# Add the data-kernel to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'data-kernel', 'src'))

try:
    from data_kernel import query_parquet
except ImportError:
    print("Error: Could not import data_kernel")
    print("Make sure the Rust library is built and accessible")
    sys.exit(1)

def create_test_data():
    """Create a test parquet file with order_items data"""
    # Create sample data
    data = {
        'id': list(range(1, 100001)),  # 100k rows to trigger GPU path
        'product_id': [i % 100 for i in range(1, 100001)],  # 100 unique products
        'quantity': [(i % 10) + 1 for i in range(1, 100001)],  # quantities 1-10
    }

    table = pa.table(data)

    # Write to temporary parquet file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
    pq.write_table(table, temp_file.name)
    temp_file.close()

    return temp_file.name

def main():
    print("=" * 80)
    print("GPU GROUP BY Test")
    print("=" * 80)

    # Create test data
    print("\n1. Creating test data...")
    parquet_file = create_test_data()
    print(f"   Created: {parquet_file}")
    print(f"   Rows: 100,000")
    print(f"   Groups: 100 unique product_ids")

    # Define the query
    query = """
    SELECT product_id, COUNT(id) as cnt, SUM(quantity) as qty
    FROM order_items
    GROUP BY product_id
    ORDER BY cnt
    """

    print(f"\n2. Running query:")
    print(f"   {query.strip()}")

    try:
        # Execute query with verbose output
        print("\n3. Execution output:\n")
        result = query_parquet(
            parquet_files=[parquet_file],
            table_names=['order_items'],
            query=query,
            verbose=True
        )

        print("\n4. Results:")
        print(result)

        # Get first few and last few rows to see ordering
        lines = result.strip().split('\n')
        if len(lines) > 20:
            print("\n   First 10 rows:")
            for line in lines[:12]:  # Include header
                print(f"   {line}")
            print("   ...")
            print("   Last 10 rows:")
            for line in lines[-10:]:
                print(f"   {line}")

    except Exception as e:
        print(f"\nError executing query: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up
        print(f"\n5. Cleaning up...")
        try:
            os.unlink(parquet_file)
            print(f"   Removed: {parquet_file}")
        except:
            pass

    print("\n" + "=" * 80)

if __name__ == '__main__':
    main()
