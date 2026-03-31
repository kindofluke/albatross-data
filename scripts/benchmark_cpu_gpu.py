#!/usr/bin/env python3
"""
Benchmark: DuckDB CPU vs data_kernel GPU on orders/order_items datasets
Compares CPU execution (DuckDB) with GPU execution (data_kernel with Metal/Vulkan)
"""

import duckdb
import time
import os
import sys
from pathlib import Path
import platform

# Try to import data_kernel for GPU execution
try:
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data-kernel', 'src'))
    from data_kernel import execute as gpu_execute, is_gpu_available, get_gpu_info
    HAS_DATA_KERNEL = True
except ImportError:
    HAS_DATA_KERNEL = False
    print("Warning: data_kernel not available, GPU benchmarks will be skipped")

# Paths
DATA_DIR = os.environ.get('DATA_DIR', './data-embed/data')
OUTPUT = "BENCHMARK_RESULTS.md"

# Colors
GREEN = '\033[0;32m'
BLUE = '\033[0;34m'
YELLOW = '\033[1;33m'
RED = '\033[0;31m'
NC = '\033[0m'

def print_color(msg, color):
    print(f"{color}{msg}{NC}")

def check_gpu():
    """Check if GPU is available via data_kernel"""
    if not HAS_DATA_KERNEL:
        return False
    try:
        return is_gpu_available()
    except Exception as e:
        print(f"Error checking GPU: {e}")
        return False

def get_file_size(filepath):
    """Get human-readable file size"""
    size = os.path.getsize(filepath)
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"

def run_query(con, query, warmup=False):
    """Run query and return execution time in milliseconds"""
    try:
        start = time.time()
        result = con.execute(query).fetchall()
        end = time.time()
        elapsed_ms = (end - start) * 1000
        return elapsed_ms, result
    except Exception as e:
        print(f"  Error: {e}")
        return None, None

def run_cpu_query(con, query_num, table, query):
    """Run query on CPU"""
    print_color(f"Q{query_num} CPU: {table}", BLUE)

    elapsed_ms, result = run_query(con, query)

    if elapsed_ms is None:
        return "ERROR"

    print(f"  CPU: {elapsed_ms:.2f}ms")
    return f"{elapsed_ms:.2f}"

def run_gpu_query(query_num, table, query, has_gpu):
    """Run query on GPU using data_kernel (if available)"""
    if not has_gpu or not HAS_DATA_KERNEL:
        return "N/A"

    print_color(f"Q{query_num} GPU: {table}", BLUE)

    # Use data_kernel for GPU execution
    try:
        start = time.time()
        result = gpu_execute(query)
        if result is not None:
            # Access the result to ensure full execution
            _ = result['result'].to_list()
        end = time.time()
        elapsed_ms = (end - start) * 1000

        print(f"  GPU: {elapsed_ms:.2f}ms")
        return f"{elapsed_ms:.2f}"
    except Exception as e:
        print(f"  GPU execution error: {e}")
        return "ERROR"

def run_benchmark(con, query_num, table, rows, query, has_gpu, output_file):
    """Run benchmark for both CPU and GPU"""
    cpu_time_str = run_cpu_query(con, query_num, table, query)
    gpu_time_str = run_gpu_query(query_num, table, query, has_gpu)

    # Calculate speedup
    if gpu_time_str == "N/A" or gpu_time_str == "ERROR":
        speedup = "N/A"
        notes = "GPU not available"
    elif cpu_time_str == "ERROR":
        speedup = "N/A"
        notes = "CPU query failed"
    else:
        cpu_time = float(cpu_time_str)
        gpu_time = float(gpu_time_str)
        speedup_val = cpu_time / gpu_time
        speedup = f"{speedup_val:.2f}"
        notes = "GPU faster" if speedup_val > 1 else "CPU faster"

    output_file.write(f"| Q{query_num} | {table} | {rows} | {cpu_time_str} | {gpu_time_str} | {speedup}x | {notes} |\n")
    print()

def main():
    print("=== DuckDB CPU vs data_kernel GPU Benchmark ===")
    print()

    # Set DATA_PATH environment variable for data_kernel
    os.environ['DATA_PATH'] = os.path.abspath(DATA_DIR)

    # Check for GPU
    has_gpu = check_gpu()
    gpu_info_dict = None
    if has_gpu and HAS_DATA_KERNEL:
        try:
            gpu_info_dict = get_gpu_info()
            if gpu_info_dict:
                print_color(f"✓ GPU detected: {gpu_info_dict['name']} ({gpu_info_dict['backend']})", GREEN)
        except Exception as e:
            print_color(f"⚠ Error getting GPU info: {e}", YELLOW)
            has_gpu = False
    else:
        print_color("⚠ No GPU detected - will run CPU benchmarks only", YELLOW)

    # Check data files
    orders_file = Path(DATA_DIR) / "orders_5m.parquet"
    items_file = Path(DATA_DIR) / "order_items_5m.parquet"

    if not orders_file.exists():
        print_color(f"✗ Data file not found: {orders_file}", RED)
        sys.exit(1)

    if not items_file.exists():
        print_color(f"✗ Data file not found: {items_file}", RED)
        sys.exit(1)

    orders_size = get_file_size(orders_file)
    items_size = get_file_size(items_file)

    print_color("✓ Data files found:", GREEN)
    print(f"  orders_5m.parquet: {orders_size}")
    print(f"  order_items_5m.parquet: {items_size}")
    print()

    # Connect to DuckDB
    con = duckdb.connect()

    # Create views
    con.execute(f"CREATE OR REPLACE VIEW orders AS SELECT * FROM '{orders_file}'")
    con.execute(f"CREATE OR REPLACE VIEW order_items AS SELECT * FROM '{items_file}'")

    # Get CPU info
    try:
        if platform.system() == "Darwin":
            cpu_info = platform.processor()
        else:
            cpu_info = "Unknown"
    except:
        cpu_info = "Unknown"

    # Initialize results file
    gpu_info_str = 'N/A (CPU-only machine)'
    if has_gpu and gpu_info_dict:
        gpu_info_str = f"{gpu_info_dict['name']} ({gpu_info_dict['backend']} backend, {gpu_info_dict['device_type']})"

    with open(OUTPUT, 'w') as f:
        f.write("# Benchmark Results: DuckDB CPU vs data_kernel GPU\n\n")
        f.write(f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("**Hardware**:\n")
        f.write(f"- **CPU**: {cpu_info}\n")
        f.write(f"- **GPU**: {gpu_info_str}\n")
        f.write(f"- **DuckDB**: {duckdb.__version__}\n")
        f.write(f"- **data_kernel**: {'Available' if HAS_DATA_KERNEL else 'Not installed'}\n\n")
        f.write("## Datasets\n")
        f.write(f"- **orders_5m.parquet**: 5M rows ({orders_size})\n")
        f.write(f"- **order_items_5m.parquet**: ~27.5M rows ({items_size})\n\n")
        f.write("## Queries\n\n")
        f.write("### Orders Table\n")
        f.write("1. **Q1 - Aggregations**: `SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders`\n")
        f.write("2. **Q2 - GROUP BY**: `SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status ORDER BY cnt DESC`\n")
        f.write("3. **Q3 - Filter**: `SELECT * FROM orders WHERE amount > 500 AND quantity > 5 LIMIT 1000`\n")
        f.write("4. **Q4 - Complex**: `SELECT status, AVG(amount), SUM(quantity) FROM orders WHERE amount > 100 GROUP BY status`\n\n")
        f.write("### Order Items Table\n")
        f.write("5. **Q5 - Aggregations**: `SELECT COUNT(*), SUM(price * quantity), AVG(price) FROM order_items`\n")
        f.write("6. **Q6 - Top Products**: `SELECT product_id, COUNT(*) as cnt, SUM(quantity) as qty FROM order_items GROUP BY product_id ORDER BY cnt DESC LIMIT 20`\n")
        f.write("7. **Q7 - Filter**: `SELECT * FROM order_items WHERE price > 100 AND quantity > 5 LIMIT 1000`\n")
        f.write("8. **Q8 - Revenue**: `SELECT product_id, SUM(price * quantity) as revenue FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 20`\n\n")
        f.write("### JOIN Queries\n")
        f.write("9. **Q9 - JOIN with GROUP BY**: `SELECT o.customer_id, COUNT(*) as order_count, SUM(oi.price * oi.quantity) as total_revenue FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id ORDER BY total_revenue DESC LIMIT 20`\n")
        f.write("10. **Q10 - JOIN with Aggregations**: `SELECT o.status, COUNT(DISTINCT o.id) as order_count, SUM(oi.quantity) as total_items, AVG(oi.price) as avg_price FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status`\n\n")
        f.write("### Window Function Queries\n")
        f.write("11. **Q11 - Window Rank**: `SELECT customer_id, id, amount, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM orders`\n")
        f.write("12. **Q12 - Window Row Number**: `SELECT product_id, order_id, price, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC) as row_num FROM order_items`\n\n")
        f.write("## Results\n\n")
        f.write("| Query | Dataset | Rows | CPU Time (ms) | GPU Time (ms) | Speedup | Notes |\n")
        f.write("|-------|---------|------|---------------|---------------|---------|-------|\n")

    print_color("=== Running Benchmarks ===", GREEN)
    print()

    with open(OUTPUT, 'a') as f:
        # Orders queries
        run_benchmark(con, 1, "orders", "5M",
            "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
            has_gpu, f)

        run_benchmark(con, 2, "orders", "5M",
            "SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status ORDER BY cnt DESC",
            has_gpu, f)

        run_benchmark(con, 3, "orders", "5M",
            "SELECT * FROM orders WHERE amount > 500 AND quantity > 5 LIMIT 1000",
            has_gpu, f)

        run_benchmark(con, 4, "orders", "5M",
            "SELECT status, AVG(amount), SUM(quantity) FROM orders WHERE amount > 100 GROUP BY status",
            has_gpu, f)

        # Order items queries
        run_benchmark(con, 5, "order_items", "27.5M",
            "SELECT COUNT(*), SUM(price * quantity), AVG(price) FROM order_items",
            has_gpu, f)

        run_benchmark(con, 6, "order_items", "27.5M",
            "SELECT product_id, COUNT(*) as cnt, SUM(quantity) as qty FROM order_items GROUP BY product_id ORDER BY cnt DESC LIMIT 20",
            has_gpu, f)

        run_benchmark(con, 7, "order_items", "27.5M",
            "SELECT * FROM order_items WHERE price > 100 AND quantity > 5 LIMIT 1000",
            has_gpu, f)

        run_benchmark(con, 8, "order_items", "27.5M",
            "SELECT product_id, SUM(price * quantity) as revenue FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 20",
            has_gpu, f)

        # JOIN queries
        run_benchmark(con, 9, "orders+items", "5M+27.5M",
            "SELECT o.customer_id, COUNT(*) as order_count, SUM(oi.price * oi.quantity) as total_revenue FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id ORDER BY total_revenue DESC LIMIT 20",
            has_gpu, f)

        run_benchmark(con, 10, "orders+items", "5M+27.5M",
            "SELECT o.status, COUNT(DISTINCT o.id) as order_count, SUM(oi.quantity) as total_items, AVG(oi.price) as avg_price FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status",
            has_gpu, f)

        # Window function queries
        run_benchmark(con, 11, "orders", "5M",
            "SELECT customer_id, id, amount, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM orders",
            has_gpu, f)

        run_benchmark(con, 12, "order_items", "27.5M",
            "SELECT product_id, order_id, price, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC) as row_num FROM order_items",
            has_gpu, f)

        # Add summary
        f.write("\n## Summary\n\n")
        f.write("### Performance Analysis\n")
        if has_gpu:
            f.write("- GPU (via data_kernel with WGPU) vs CPU (DuckDB) speedup varies by query type\n")
            f.write("- Aggregation-heavy queries may benefit more from GPU acceleration\n")
            f.write("- Small result sets (LIMIT queries) may have GPU transfer overhead\n")
            f.write("- The GPU backend is using " + (gpu_info_dict['backend'] if gpu_info_dict else "unknown") + " for compute\n")
        else:
            f.write("- CPU-only benchmarks completed\n")
            f.write("- Install data_kernel Python package to enable GPU comparison\n")

        f.write("\n### Next Steps\n")
        if not has_gpu:
            f.write("1. Install data_kernel package: `cd data-kernel && pip install -e .`\n")
            f.write("2. Ensure GPU drivers are installed (Metal for macOS, Vulkan for Linux/Windows)\n")
            f.write("3. Re-run to get GPU comparison\n")
        else:
            f.write("1. Analyze which query patterns benefit most from GPU acceleration\n")
            f.write("2. Test with larger datasets (10M+ rows) to see GPU benefits scale\n")
            f.write("3. Profile GPU utilization with Metal/Vulkan tools\n")

    con.close()

    print()
    print_color("✓ Benchmark complete!", GREEN)
    print(f"Results written to: {OUTPUT}")
    print()

    # Display results
    with open(OUTPUT, 'r') as f:
        print(f.read())

if __name__ == "__main__":
    main()
