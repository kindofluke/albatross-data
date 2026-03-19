# SQL Executor

Simple, fast SQL query executor for Parquet files using Apache DataFusion.

## Features

- **Pure Rust** - No external dependencies, no C++ compilation
- **Fast** - Columnar execution via DataFusion
- **Standard SQL** - Full SQL support including JOINs, aggregations, window functions
- **Parquet Native** - Direct execution on Parquet files
- **Simple** - ~200 lines of code

## Usage

### Basic Query

```bash
cargo run -- -q "SELECT * FROM orders LIMIT 10" -f data/orders.parquet
```

### Aggregation

```bash
cargo run -- \
  -q "SELECT status, COUNT(*) as count, SUM(amount) as total FROM orders GROUP BY status" \
  -f data/orders.parquet
```

### Filter

```bash
cargo run -- \
  -q "SELECT * FROM orders WHERE amount > 500" \
  -f data/orders.parquet
```

### Multiple Tables (JOIN)

```bash
cargo run -- \
  -q "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id" \
  -f data/orders.parquet \
  -f data/customers.parquet \
  -t orders \
  -t customers
```

### Verbose Output

```bash
cargo run -- \
  -q "SELECT * FROM orders LIMIT 5" \
  -f data/orders.parquet \
  -v
```

### Explain Query Plan

```bash
cargo run -- \
  -q "SELECT * FROM orders WHERE amount > 500" \
  -f data/orders.parquet \
  --explain-only
```

## Command Line Options

```
Options:
  -q, --query <QUERY>        SQL query to execute
  -f, --file <FILES>         Parquet file paths (can be specified multiple times)
  -t, --table <TABLES>       Table names corresponding to files (defaults to file stems)
  -v, --verbose              Enable verbose output
      --explain-only         Only show the logical plan; do not execute
  -h, --help                 Print help
```

## Examples

See the `examples/` directory for more query examples.

## Performance

DataFusion provides excellent performance for analytical queries:
- Vectorized execution
- Predicate pushdown
- Projection pushdown
- Parallel execution
- Memory-efficient streaming

For most workloads, this is faster than traditional databases.

## Architecture

```
SQL Query → DataFusion → Parquet Files → Results
```

That's it! No complex layers, no FFI, just fast SQL execution.

## Building

```bash
cargo build --release
```

## Testing

```bash
# Generate test data
cd ../generate-test-data
cargo run

# Run queries
cd ../executor
cargo run -- -q "SELECT COUNT(*) FROM orders" -f ../data/orders.parquet
```

## License

Same as parent project.
