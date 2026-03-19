# DataFusion → Substrait → Sirius Integration

Complete end-to-end GPU-accelerated query execution pipeline.

## Architecture

```
SQL Query → DataFusion (Rust) → Substrait Plan + Manifest → DuckDB + Sirius → GPU Execution → Results
```

### Components

1. **DataFusion CLI** (`data-embed/datafusion-cli/`)
   - Parses SQL queries
   - Optimizes logical plans
   - Generates Substrait protobuf plans
   - Creates execution manifests (JSON) with Parquet file mappings

2. **Execution Manifest** (JSON format)
   - Bundles Substrait plan (base64-encoded)
   - Maps table names to Parquet file paths
   - Preserves original SQL for reference
   - Supports multiple tables for JOINs

3. **Sirius Executor** (`lib/sirius/execute_manifest.py`)
   - Loads DuckDB with Substrait + Sirius extensions
   - Registers Parquet files as tables
   - Executes Substrait plan on GPU
   - Returns results

## Quick Start

### 1. Generate Execution Manifest

```bash
cd data-embed

# Simple aggregation
cargo run --release --bin datafusion-cli -- \
  -q "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders" \
  -p data/orders.parquet \
  -o output/query1_manifest.json \
  --manifest

# GROUP BY
cargo run --release --bin datafusion-cli -- \
  -q "SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status" \
  -p data/orders.parquet \
  -o output/query2_manifest.json \
  --manifest

# WHERE + LIMIT
cargo run --release --bin datafusion-cli -- \
  -q "SELECT * FROM orders WHERE amount > 500 LIMIT 10" \
  -p data/orders.parquet \
  -o output/query3_manifest.json \
  --manifest
```

### 2. Execute with DuckDB (CPU)

Once Sirius substrait extension is built:

```bash
cd lib/sirius

python3 execute_manifest.py \
  ../../data-embed/output/query1_manifest.json \
  --verbose
```

### 3. Execute with Sirius (GPU)

```bash
cd lib/sirius

python3 execute_manifest.py \
  ../../data-embed/output/query1_manifest.json \
  --gpu \
  --verbose
```

## Execution Manifest Format

```json
{
  "version": "1.0",
  "sql": "SELECT COUNT(*), SUM(amount) FROM orders",
  "substrait_plan": "Eg8aDQj/////DxABGgNzdW0...",
  "tables": {
    "orders": {
      "path": "/absolute/path/to/orders.parquet",
      "format": "parquet"
    }
  }
}
```

### Fields

- **version**: Manifest format version (currently "1.0")
- **sql**: Original SQL query (for debugging/reference)
- **substrait_plan**: Base64-encoded Substrait protobuf bytes
- **tables**: Map of table names to data sources
  - **path**: Absolute or relative path to Parquet file
  - **format**: Data format (currently only "parquet")

## Multi-Table Queries (JOINs)

For queries involving multiple tables:

```bash
# Generate test data for customers
cargo run --release --bin generate-test-data -- --output data/customers.parquet

# Create manifest with JOIN
cargo run --release --bin datafusion-cli -- \
  -q "SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id" \
  -p data/orders.parquet \
  -t orders \
  --manifest
```

**Note**: Multi-table support requires CLI enhancement to accept multiple `-p` flags.

## Build Status

### ✅ Complete
- DataFusion CLI with Substrait generation
- Execution manifest format
- Test data generation (10K rows)
- Sirius DuckDB extension built

### 🔄 In Progress
- Sirius Substrait extension build (compiling)

### 📋 TODO
- Multi-table manifest generation
- GPU execution validation
- Performance benchmarking
- Larger datasets (1M+ rows)

## Testing

### Verify Manifest Generation

```bash
cd data-embed
cat output/query1_manifest.json | jq .
```

Expected output:
```json
{
  "version": "1.0",
  "sql": "SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders",
  "substrait_plan": "...",
  "tables": {
    "orders": {
      "path": "/LargeData/albatross-data/data-embed/data/orders.parquet",
      "format": "parquet"
    }
  }
}
```

### Verify Substrait Extension (once built)

```bash
cd lib/sirius
./build/release/duckdb -unsigned :memory: <<EOF
LOAD 'build/release/extension/substrait/substrait.duckdb_extension';
SELECT * FROM duckdb_extensions() WHERE extension_name = 'substrait';
EOF
```

## Performance Expectations

### Small Dataset (10K rows)
- **CPU**: ~1-5ms
- **GPU**: ~5-10ms (overhead dominates)
- **Speedup**: None (data too small)

### Medium Dataset (1M rows)
- **CPU**: ~50-200ms
- **GPU**: ~10-30ms
- **Speedup**: 2-5x

### Large Dataset (100M rows)
- **CPU**: ~5-20s
- **GPU**: ~500ms-2s
- **Speedup**: 10-20x

GPU acceleration benefits increase with:
- Larger datasets (>1M rows)
- Complex aggregations
- Multiple JOINs
- Heavy filtering

## Troubleshooting

### "Extension could not be loaded"
```bash
# Use -unsigned flag
./build/release/duckdb -unsigned :memory:
```

### "Table not found"
- Verify Parquet path in manifest is absolute
- Check file exists: `ls -lh /path/to/file.parquet`

### "Substrait extension not found"
- Wait for Sirius substrait build to complete
- Check: `ls lib/sirius/build/release/extension/substrait/`

### GPU execution fails
- Sirius GPU execution requires config file (SIRIUS_CONFIG_FILE)
- For now, test with CPU-only Substrait execution
- GPU integration pending configuration setup

## Next Steps

1. **Complete Substrait extension build**
   - Currently compiling in background
   - ETA: 10-20 minutes

2. **Test end-to-end execution**
   - Execute manifests with Substrait
   - Verify results match DataFusion CPU

3. **Enable GPU acceleration**
   - Create Sirius config file
   - Test with `gpu_execution()`
   - Benchmark vs CPU

4. **Scale up dataset**
   - Generate 1M, 10M, 100M row datasets
   - Measure GPU speedup
   - Profile with nvidia-smi

## Files

```
albatross-data/
├── data-embed/
│   ├── datafusion-cli/
│   │   ├── src/
│   │   │   ├── main.rs          # CLI with manifest generation
│   │   │   └── manifest.rs      # Manifest data structures
│   │   └── Cargo.toml
│   ├── data/
│   │   └── orders.parquet       # Test data (10K rows)
│   └── output/
│       ├── query1_manifest.json # Execution manifests
│       ├── query2_manifest.json
│       └── query3_manifest.json
├── lib/sirius/
│   ├── build/release/
│   │   ├── duckdb               # DuckDB binary
│   │   └── extension/
│   │       ├── sirius/          # Sirius GPU extension ✅
│   │       └── substrait/       # Substrait extension 🔄
│   └── execute_manifest.py      # Python executor script
├── AGENT.md                     # Implementation status
└── INTEGRATION.md               # This file
```
