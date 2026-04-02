#!/usr/bin/env python3
"""Test script for list_tables() function"""
import os
import json

# Set DATA_PATH to the directory with test parquet files
os.environ['DATA_PATH'] = '/Users/luke.shulman/Projects/albatross-data/data-embed/data'

from data_kernel import list_tables

print("Testing list_tables() function...")
print("=" * 60)

try:
    metadata = list_tables()

    if metadata:
        print(f"✓ SUCCESS! Got metadata for {len(metadata['tables'])} table(s)\n")

        for table in metadata['tables']:
            print(f"Table: {table['name']}")
            print(f"  File: {table['file_path']}")
            print(f"  Rows: {table['num_rows']:,}")
            print(f"  Size: {table['file_size_bytes']:,} bytes")
            print(f"  Columns ({len(table['columns'])}):")
            for col in table['columns']:
                nullable = "NULL" if col['nullable'] else "NOT NULL"
                print(f"    - {col['name']}: {col['data_type']} ({nullable})")
            print()

        print("\nFull JSON output:")
        print(json.dumps(metadata, indent=2))
    else:
        print("✗ ERROR: No metadata returned")
except Exception as e:
    print(f"✗ ERROR: {e}")
    import traceback
    traceback.print_exc()
