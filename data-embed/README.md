# DataFusion to Substrait Pipeline

GPU-accelerated query pipeline frontend: converts SQL queries to Substrait plans for GPU execution.

## Architecture

```
SQL Query → DataFusion (Parse/Optimize) → Substrait Plan (.pb) → [Sirius GPU Execution]
```

## Components

- **datafusion-cli**: CLI tool to convert SQL to Substrait protobuf
- **generate-test-data**: Creates sample Parquet test data

## Quick Start

### 1. Generate Test Data

```bash
cargo run --release --bin generate-test-data
```

Creates `data/orders.parquet` with 10,000 rows:
- `id`: Int64
- `customer_id`: Int64
- `amount`: Float64
- `quantity`: Int32
- `status`: String (pending/shipped/delivered/cancelled)

### 2. Convert SQL to Substrait

```bash
cargo run --release --bin datafusion-cli -- \
  --query "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders" \
  --parquet data/orders.parquet \
  --output output/query.pb \
  --verbose
```

**Arguments:**
- `-q, --query`: SQL query string
- `-p, --parquet`: Path to Parquet file
- `-t, --table`: Table name (default: "orders")
- `-o, --output`: Output path for Substrait plan (.pb file)
- `-v, --verbose`: Show logical plan and debug info

## Example Queries

### Simple Aggregation
```bash
cargo run --release --bin datafusion-cli -- \
  -q "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders" \
  -p data/orders.parquet \
  -o output/query1.pb
```

### GROUP BY
```bash
cargo run --release --bin datafusion-cli -- \
  -q "SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status" \
  -p data/orders.parquet \
  -o output/query2.pb
```

### WHERE + LIMIT
```bash
cargo run --release --bin datafusion-cli -- \
  -q "SELECT * FROM orders WHERE amount > 500 LIMIT 10" \
  -p data/orders.parquet \
  -o output/query3.pb
```

### Complex: GROUP BY + ORDER BY + LIMIT
```bash
cargo run --release --bin datafusion-cli -- \
  -q "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id ORDER BY total DESC LIMIT 10" \
  -p data/orders.parquet \
  -o output/query4.pb
```

## Supported SQL Features

✅ **Working:**
- SELECT with projections
- WHERE filters
- GROUP BY aggregations
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- ORDER BY
- LIMIT
- Column aliases

❌ **Not yet tested:**
- JOINs (multi-table queries)
- Subqueries
- Window functions
- HAVING clauses

## Output Format

The CLI generates Substrait protobuf files (`.pb`) containing the serialized query plan. These files can be:
- Passed to Sirius for GPU execution
- Inspected with protobuf tools
- Sent across FFI boundaries
- Used by other Substrait-compatible engines

## Next Steps

1. **Sirius Integration**: Pass `.pb` files to Sirius GPU execution engine
2. **FFI Bridge**: Implement Rust ↔ C++ bridge for zero-copy data transfer
3. **Benchmarking**: Compare performance vs CPU-only execution
4. **Scale Testing**: Increase dataset size to 1M+ rows

## Dependencies

- `datafusion` 43.0: SQL parsing and optimization
- `datafusion-substrait` 43.0: Substrait serialization
- `arrow` 53.3: Columnar data format
- `parquet` 53.3: Parquet file I/O
- `tokio`: Async runtime
- `clap`: CLI argument parsing
