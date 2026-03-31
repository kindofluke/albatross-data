#!/usr/bin/env python3
"""
Integration test for data-kernel FFI bridge
"""
import os
import sys
import subprocess

# Set DATA_PATH for testing
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

from data_kernel import arrow_bridge
from data_kernel.kernel import DataKernel

def test_arrow_bridge():
    """Test the Arrow FFI bridge directly"""
    print("=" * 60)
    print("Testing Arrow Bridge FFI")
    print("=" * 60)
    
    test_queries = [
        ('Simple COUNT', 'SELECT COUNT(*) as count FROM orders'),
        ('Data', 'SELECT * FROM orders limit 10'),
]
    
    for name, query in test_queries:
        print(f"\n{name}: {query}")
        try:
            result = arrow_bridge.execute_query(query)
            if result:
                print(f"  ✓ Success: {result.to_pylist()}")
            else:
                print(f"  ⚠ Empty result")
        except Exception as e:
            print(f"  ✗ Error: {e}")

def test_kernel():
    """Test the DataKernel"""
    print("\n" + "=" * 60)
    print("Testing DataKernel")
    print("=" * 60)
    
    kernel = DataKernel()
    print(f"\n✓ Kernel Info:")
    print(f"  Implementation: {kernel.implementation} v{kernel.implementation_version}")
    print(f"  Language: {kernel.language}")
    print(f"  MIME type: {kernel.language_info['mimetype']}")
    
    # Note: Full kernel execution requires Jupyter infrastructure
    # This just verifies the kernel can be instantiated
    print(f"\n✓ Kernel instantiated successfully")

def test_gpu_execution():
    """Test the GPU execution path"""
    print("\n" + "=" * 60)
    print("Testing GPU Execution")
    print("=" * 60)

    # Path to the compiled executor
    executor_path = "/Users/luke.shulman/Projects/albatross-data/data-embed/executor/target/release/executor"
    data_path = "/Users/luke.shulman/Projects/albatross-data/data-embed/data/orders.parquet"

    query = "SELECT SUM(l_extendedprice) FROM lineitem"

    print(f"\nExecuting query with --gpu: {query}")

    try:
        result = subprocess.run(
            [
                executor_path,
                "--gpu",
                "-f",
                data_path,
                "-q",
                query,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        print(f"  ✓ Success: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Error: {e}")
        print(f"  ✗ Stderr: {e.stderr}")

if __name__ == '__main__':
    test_arrow_bridge()
    test_kernel()
    test_gpu_execution()
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
