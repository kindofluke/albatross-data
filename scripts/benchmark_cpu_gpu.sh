#!/bin/bash
# Benchmark: DuckDB CPU vs Sirius GPU on orders/order_items datasets
# Can run on CPU-only machine (skips GPU) or GPU machine (runs both)

set -e

# Paths
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"  # Override with path to Sirius-enabled DuckDB
DATA_DIR="${DATA_DIR:-./data}"
OUTPUT="BENCHMARK_RESULTS.md"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== DuckDB CPU vs Sirius GPU Benchmark ==="
echo ""

# Check for GPU
HAS_GPU=false
if command -v nvidia-smi &> /dev/null; then
    if nvidia-smi &> /dev/null; then
        HAS_GPU=true
        GPU_INFO=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader | head -1)
        echo -e "${GREEN}✓ GPU detected: $GPU_INFO${NC}"
    fi
fi

if [ "$HAS_GPU" = false ]; then
    echo -e "${YELLOW}⚠ No GPU detected - will run CPU benchmarks only${NC}"
fi

# Check for DuckDB
if ! command -v $DUCKDB_BIN &> /dev/null; then
    echo -e "${RED}✗ DuckDB not found at: $DUCKDB_BIN${NC}"
    echo "Set DUCKDB_BIN environment variable to DuckDB binary path"
    echo "Example: DUCKDB_BIN=/path/to/sirius/build/release/duckdb $0"
    exit 1
fi

# Check data files
if [ ! -f "$DATA_DIR/orders.parquet" ]; then
    echo -e "${RED}✗ Data file not found: $DATA_DIR/orders.parquet${NC}"
    echo "Run: ./generate_10m_orders.sh"
    exit 1
fi

if [ ! -f "$DATA_DIR/order_items.parquet" ]; then
    echo -e "${RED}✗ Data file not found: $DATA_DIR/order_items.parquet${NC}"
    echo "Run: ./generate_10m_orders.sh"
    exit 1
fi

# Get file sizes
ORDERS_SIZE=$(du -h "$DATA_DIR/orders.parquet" | cut -f1)
ITEMS_SIZE=$(du -h "$DATA_DIR/order_items.parquet" | cut -f1)

echo -e "${GREEN}✓ Data files found:${NC}"
echo "  orders.parquet: $ORDERS_SIZE"
echo "  order_items.parquet: $ITEMS_SIZE"
echo ""

# Initialize results file
cat > "$OUTPUT" <<EOF
# Benchmark Results: DuckDB CPU vs Sirius GPU

**Date**: $(date)
**Hardware**:
- **CPU**: $(lscpu | grep "Model name" | cut -d: -f2 | xargs)
- **GPU**: ${GPU_INFO:-N/A (CPU-only machine)}
- **DuckDB**: $($DUCKDB_BIN --version 2>/dev/null || echo "unknown")

## Datasets
- **orders.parquet**: 10M rows ($ORDERS_SIZE)
- **order_items.parquet**: ~55M rows ($ITEMS_SIZE)

## Queries

### Orders Table
1. **Q1 - Aggregations**: \`SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders\`
2. **Q2 - GROUP BY**: \`SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status ORDER BY cnt DESC\`
3. **Q3 - Filter**: \`SELECT * FROM orders WHERE amount > 500 AND quantity > 5 LIMIT 1000\`
4. **Q4 - Complex**: \`SELECT status, AVG(amount), SUM(quantity) FROM orders WHERE amount > 100 GROUP BY status\`

### Order Items Table
5. **Q5 - Aggregations**: \`SELECT COUNT(*), SUM(price * quantity), AVG(price) FROM order_items\`
6. **Q6 - Top Products**: \`SELECT product_id, COUNT(*) as cnt, SUM(quantity) as qty FROM order_items GROUP BY product_id ORDER BY cnt DESC LIMIT 20\`
7. **Q7 - Filter**: \`SELECT * FROM order_items WHERE price > 100 AND quantity > 5 LIMIT 1000\`
8. **Q8 - Revenue**: \`SELECT product_id, SUM(price * quantity) as revenue FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 20\`

## Results

| Query | Dataset | Rows | CPU Time (ms) | GPU Time (ms) | Speedup | Notes |
|-------|---------|------|---------------|---------------|---------|-------|
EOF

# Function to run CPU query
run_cpu_query() {
    local query_num=$1
    local table=$2
    local query=$3
    
    echo -e "${BLUE}Q$query_num CPU: $table${NC}"
    
    # Create temp SQL file
    cat > /tmp/bench_cpu.sql <<EOSQL
CREATE OR REPLACE VIEW orders AS SELECT * FROM '$DATA_DIR/orders.parquet';
CREATE OR REPLACE VIEW order_items AS SELECT * FROM '$DATA_DIR/order_items.parquet';
.timer on
$query;
EOSQL
    
    # Run and capture timing
    output=$($DUCKDB_BIN < /tmp/bench_cpu.sql 2>&1)
    
    # Extract time (DuckDB outputs "Run Time: real X.XXX s")
    time_ms=$(echo "$output" | grep -i "Run Time" | grep -oE '[0-9]+\.[0-9]+' | awk '{print $1 * 1000}')
    
    if [ -z "$time_ms" ]; then
        time_ms="ERROR"
    fi
    
    echo "  CPU: ${time_ms}ms"
    echo "$time_ms"
}

# Function to run GPU query
run_gpu_query() {
    local query_num=$1
    local table=$2
    local query=$3
    
    if [ "$HAS_GPU" = false ]; then
        echo "N/A"
        return
    fi
    
    echo -e "${BLUE}Q$query_num GPU: $table${NC}"
    
    # Create temp SQL file with gpu_execution call
    cat > /tmp/bench_gpu.sql <<EOSQL
CREATE OR REPLACE VIEW orders AS SELECT * FROM '$DATA_DIR/orders.parquet';
CREATE OR REPLACE VIEW order_items AS SELECT * FROM '$DATA_DIR/order_items.parquet';
.timer on
CALL gpu_execution('$query');
EOSQL
    
    # Run and capture timing
    output=$($DUCKDB_BIN < /tmp/bench_gpu.sql 2>&1)
    
    # Extract time
    time_ms=$(echo "$output" | grep -i "Run Time" | grep -oE '[0-9]+\.[0-9]+' | awk '{print $1 * 1000}')
    
    if [ -z "$time_ms" ]; then
        time_ms="ERROR"
    fi
    
    echo "  GPU: ${time_ms}ms"
    echo "$time_ms"
}

# Function to run benchmark
run_benchmark() {
    local query_num=$1
    local table=$2
    local rows=$3
    local query=$4
    
    cpu_time=$(run_cpu_query "$query_num" "$table" "$query")
    gpu_time=$(run_gpu_query "$query_num" "$table" "$query")
    
    # Calculate speedup
    if [ "$gpu_time" = "N/A" ] || [ "$gpu_time" = "ERROR" ]; then
        speedup="N/A"
        notes="GPU not available"
    elif [ "$cpu_time" = "ERROR" ]; then
        speedup="N/A"
        notes="CPU query failed"
    else
        speedup=$(echo "scale=2; $cpu_time / $gpu_time" | bc)
        if (( $(echo "$speedup > 1" | bc -l) )); then
            notes="GPU faster"
        else
            notes="CPU faster"
        fi
    fi
    
    echo "| Q$query_num | $table | $rows | $cpu_time | $gpu_time | ${speedup}x | $notes |" >> "$OUTPUT"
    echo ""
}

# Run benchmarks
echo -e "${GREEN}=== Running Benchmarks ===${NC}"
echo ""

# Orders queries
run_benchmark 1 "orders" "10M" \
    "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders"

run_benchmark 2 "orders" "10M" \
    "SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status ORDER BY cnt DESC"

run_benchmark 3 "orders" "10M" \
    "SELECT * FROM orders WHERE amount > 500 AND quantity > 5 LIMIT 1000"

run_benchmark 4 "orders" "10M" \
    "SELECT status, AVG(amount), SUM(quantity) FROM orders WHERE amount > 100 GROUP BY status"

# Order items queries
run_benchmark 5 "order_items" "55M" \
    "SELECT COUNT(*), SUM(price * quantity), AVG(price) FROM order_items"

run_benchmark 6 "order_items" "55M" \
    "SELECT product_id, COUNT(*) as cnt, SUM(quantity) as qty FROM order_items GROUP BY product_id ORDER BY cnt DESC LIMIT 20"

run_benchmark 7 "order_items" "55M" \
    "SELECT * FROM order_items WHERE price > 100 AND quantity > 5 LIMIT 1000"

run_benchmark 8 "order_items" "55M" \
    "SELECT product_id, SUM(price * quantity) as revenue FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 20"

# Add summary
cat >> "$OUTPUT" <<EOF

## Summary

### Performance Analysis
$(if [ "$HAS_GPU" = true ]; then
    echo "- GPU vs CPU speedup varies by query type"
    echo "- Aggregation-heavy queries may benefit more from GPU"
    echo "- Small result sets (LIMIT queries) may have GPU overhead"
else
    echo "- CPU-only benchmarks completed"
    echo "- Run on GPU machine to compare GPU performance"
fi)

### Next Steps
$(if [ "$HAS_GPU" = false ]; then
    echo "1. Transfer this script and data to GPU machine"
    echo "2. Set DUCKDB_BIN to Sirius-enabled DuckDB path"
    echo "3. Re-run to get GPU comparison"
else
    echo "1. Analyze which query patterns benefit from GPU"
    echo "2. Profile with nvidia-smi and nsys for detailed metrics"
    echo "3. Test with larger datasets (100M+ rows)"
fi)

EOF

echo ""
echo -e "${GREEN}✓ Benchmark complete!${NC}"
echo "Results written to: $OUTPUT"
echo ""
cat "$OUTPUT"
