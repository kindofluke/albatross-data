# GPU-Accelerated Query Executor

End-to-end CLI tool for executing SQL queries on Parquet files with CPU or GPU acceleration.

## Quick Start

### 1. Build the Executor

```bash
cd data-embed
cargo build --release --bin executor
```

### 2. Run a Query (CPU Mode)

```bash
./data-embed/target/release/executor \
  --file data-embed/data/orders_10m.parquet \
  --query "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders_10m" \
  --mode cpu \
  --duckdb lib/sirius/build/release/duckdb
```

### 3. Run Benchmarks

```bash
./scripts/benchmark.sh
cat BENCHMARK_RESULTS.md
```

## Features

### ✅ Implemented
- **Multi-file support**: Query multiple Parquet files with JOINs
- **CPU execution**: Standard DuckDB query processing
- **GPU execution framework**: Sirius integration (config blocked)
- **Timing metrics**: Parse, execution, and total time tracking
- **GPU monitoring**: nvidia-smi integration for utilization and memory
- **Batch data generation**: Create datasets from 10K to 10M+ rows
- **Benchmark suite**: Automated CPU/GPU performance comparison

### ⚠️ Partially Working
- **GPU execution**: Framework ready, blocked by Sirius config format
- **GPU metrics**: Monitoring works, but no GPU execution to measure yet

### 📋 Not Implemented
- **Substrait execution**: Extension unavailable for DuckDB v1.4.4
- **Result validation**: Compare CPU vs GPU outputs
- **Multi-table queries**: CLI supports it, needs testing

## Architecture

```
SQL Query
    ↓
[Executor CLI]
    ↓
[DuckDB + Sirius] ← Parquet Files
    ↓
Results + Metrics
```

### Components

1. **Executor** (`data-embed/executor/`)
   - CLI interface for query execution
   - Supports CPU and GPU modes
   - Captures timing and GPU metrics

2. **Data Generator** (`data-embed/generate-test-data/`)
   - Creates test Parquet files
   - Configurable row count and output path
   - Batched writes for large datasets

3. **Benchmark Suite** (`scripts/benchmark.sh`)
   - Automated performance testing
   - Multiple dataset sizes (10K, 1M, 10M rows)
   - Multiple query patterns

## Usage

### Basic Query

```bash
./data-embed/target/release/executor \
  --file data-embed/data/orders.parquet \
  --query "SELECT * FROM orders LIMIT 10" \
  --mode cpu \
  --duckdb lib/sirius/build/release/duckdb
```

### Multi-File Query (JOIN)

```bash
./data-embed/target/release/executor \
  --file data-embed/data/orders.parquet \
  --file data-embed/data/customers.parquet \
  --table orders \
  --table customers \
  --query "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id" \
  --mode cpu \
  --duckdb lib/sirius/build/release/duckdb
```

### Compare CPU vs GPU (when GPU config is fixed)

```bash
./data-embed/target/release/executor \
  --file data-embed/data/orders_10m.parquet \
  --query "SELECT status, COUNT(*), SUM(amount) FROM orders_10m GROUP BY status" \
  --mode both \
  --duckdb lib/sirius/build/release/duckdb \
  --verbose
```

### Generate Test Data

```bash
# 1 million rows
cd data-embed
cargo run --release --bin generate-test-data -- \
  --rows 1000000 \
  --output data/orders_1m.parquet

# 10 million rows
cargo run --release --bin generate-test-data -- \
  --rows 10000000 \
  --output data/orders_10m.parquet
```

## CLI Options

### Executor

```
--query, -q <SQL>          SQL query to execute (required)
--file, -f <PATH>          Parquet file path (can specify multiple)
--table, -t <NAME>         Table name (defaults to file stem)
--mode, -m <MODE>          Execution mode: cpu, gpu, or both (default: cpu)
--duckdb <PATH>            Path to DuckDB binary
--sirius-config <PATH>     Path to Sirius config file (for GPU mode)
--verbose, -v              Enable verbose output
```

### Data Generator

```
--rows, -r <NUM>           Number of rows to generate (default: 10000)
--output, -o <PATH>        Output Parquet file path
--batch-size, -b <NUM>     Rows per batch (default: 1000000)
```

## Performance Results

### CPU Baseline (DuckDB v1.4.4)

| Dataset | Rows | Query Type | Time (ms) | Throughput |
|---------|------|------------|-----------|------------|
| orders  | 10K  | Aggregation| 3,199     | 3.1K rows/s |
| orders  | 1M   | Aggregation| 3,313     | 302K rows/s |
| orders  | 10M  | Aggregation| 3,983     | 2.5M rows/s |
| orders  | 10M  | GROUP BY   | 4,094     | 2.4M rows/s |

**Observations**:
- DuckDB is highly optimized for analytical queries
- Execution time scales sub-linearly with data size
- Most time spent in data loading, not computation

### GPU Performance

**Status**: Blocked by Sirius configuration requirements

**Issue**: `gpu_execution()` requires `SIRIUS_CONFIG_FILE` environment variable pointing to a valid config file. The config file format is not documented, and attempts to create one result in parse errors.

**Error**: `INTERNAL Error: Attempted to dereference shared_ptr that is NULL!`

**Next Steps**:
1. Find Sirius config file documentation or examples
2. Contact Sirius maintainers for config format
3. Test with minimal config to identify required fields

## File Structure

```
albatross-data/
├── data-embed/
│   ├── executor/                    # Main executor CLI
│   │   ├── src/
│   │   │   ├── main.rs             # CLI entry point
│   │   │   ├── executor.rs         # Execution logic
│   │   │   └── metrics.rs          # GPU monitoring
│   │   └── Cargo.toml
│   ├── generate-test-data/          # Data generator
│   │   └── src/main.rs
│   ├── datafusion-cli/              # DataFusion → Substrait (Phase 2A)
│   │   └── src/
│   │       ├── main.rs
│   │       └── manifest.rs
│   └── data/                        # Test datasets
│       ├── orders.parquet           # 10K rows (221KB)
│       ├── orders_1m.parquet        # 1M rows (18MB)
│       └── orders_10m.parquet       # 10M rows (179MB)
├── lib/sirius/                      # Sirius GPU extension
│   └── build/release/
│       └── duckdb                   # DuckDB with Sirius built-in
├── scripts/
│   └── benchmark.sh                 # Automated benchmarking
├── BENCHMARK_RESULTS.md             # Performance results
└── EXECUTOR_README.md               # This file
```

## Known Issues

### 1. GPU Execution Blocked
**Problem**: Sirius `gpu_execution()` requires undocumented config file format

**Workaround**: Use CPU mode for now

**Impact**: Cannot test GPU acceleration or measure speedup

### 2. Substrait Extension Unavailable
**Problem**: DuckDB v1.4.4 has no substrait extension in community repository

**Workaround**: Execute SQL strings directly instead of Substrait plans

**Impact**: Cannot use Substrait IR for cross-platform query optimization

### 3. Execution Time Dominated by Overhead
**Problem**: Most time spent in DuckDB startup and data loading, not query execution

**Observation**: Even 10M rows execute in ~4 seconds, suggesting DuckDB is extremely efficient

**Impact**: GPU benefits may only appear with 100M+ row datasets or complex queries

## Troubleshooting

### "File not found" Error
```bash
# Use absolute paths or run from project root
cd /LargeData/albatross-data
./data-embed/target/release/executor --file data-embed/data/orders.parquet ...
```

### "DuckDB binary not found"
```bash
# Verify Sirius is built
ls -lh lib/sirius/build/release/duckdb

# Specify full path
--duckdb /LargeData/albatross-data/lib/sirius/build/release/duckdb
```

### GPU Execution Fails
```bash
# Currently expected - config format unknown
# Use CPU mode instead:
--mode cpu
```

## Next Steps

### Immediate
1. **Resolve Sirius config format**: Find documentation or examples
2. **Test GPU execution**: Once config is fixed, run full benchmarks
3. **Generate larger datasets**: 100M rows to see GPU benefits

### Short-term
4. **Multi-table testing**: Verify JOIN queries work correctly
5. **Result validation**: Compare CPU vs GPU outputs for correctness
6. **Profile with nsys**: Detailed GPU kernel analysis

### Long-term
7. **Substrait integration**: Build extension for DuckDB v1.4.4
8. **Docker packaging**: Reproducible environment with all dependencies
9. **CI/CD pipeline**: Automated testing and benchmarking
10. **Documentation**: User guide and API reference

## Contributing

### Adding New Queries

Edit `scripts/benchmark.sh` and add:

```bash
run_benchmark \
    "data/orders_10m.parquet" \
    "orders_10m" \
    "10M" \
    "4" \
    "SELECT customer_id, AVG(amount) FROM orders_10m GROUP BY customer_id HAVING AVG(amount) > 500"
```

### Adding New Datasets

```bash
cargo run --release --bin generate-test-data -- \
  --rows 100000000 \
  --output data/orders_100m.parquet
```

### Testing GPU Execution

Once config format is known:

1. Create valid `lib/sirius/sirius_config.cfg`
2. Run: `--mode gpu --sirius-config lib/sirius/sirius_config.cfg`
3. Monitor: `watch -n 0.1 nvidia-smi`

## References

- **DuckDB**: https://duckdb.org/
- **Sirius**: https://github.com/duckdb/duckdb (extension)
- **DataFusion**: https://datafusion.apache.org/
- **Substrait**: https://substrait.io/
- **NVIDIA T4**: https://www.nvidia.com/en-us/data-center/tesla-t4/
