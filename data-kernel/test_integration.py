#!/usr/bin/env python3
"""
Integration test for data-kernel FFI bridge
"""
import os
import sys

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
        ('Multiple tables', 'SELECT COUNT(*) FROM orders_10m'),
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

if __name__ == '__main__':
    test_arrow_bridge()
    test_kernel()
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
