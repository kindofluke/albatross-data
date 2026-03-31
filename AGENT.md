# AGENT.md - Developer Implementation Guide

> **Quick Start**: New to the project? Read [README.md](./README.md) first for setup instructions, then return here for implementation details.

## Project Status: Phase 2A Complete ✅

**Goal**: GPU-accelerated composable data pipeline bypassing traditional databases.

**Current State**: DataFusion frontend working. Converts SQL → Substrait protobuf plans. FFI integration complete between Rust and Python.

## What's Been Built

### 1. Unified Executor (`data-embed/executor/`)
- **Purpose**: Single CLI for SQL execution, plan inspection, and Substrait generation
- **Input**: SQL query string + Parquet file path(s)
- **Modes**:
  - **Execute**: Run query and return results (default)
  - **Logical Plan**: `--explain-only` - Show DataFusion logical plan
  - **Physical Plan**: `--physical-plan` - Show DataFusion physical execution plan
  - **Substrait Binary**: `--substrait` - Generate Substrait protobuf (`.pb` file)
  - **Substrait Text**: `--substrait-text` - Generate human-readable Substrait plan
- **Status**: ✅ Working

**Key Code**: `data-embed/executor/src/`
- `executor.rs`: Core execution logic with multiple output modes
- `main.rs`: CLI argument parsing and mode dispatch
- Uses `datafusion` for SQL parsing, optimization, and execution
- Uses `datafusion-substrait::logical_plan::producer::to_substrait_plan()` for serialization
- Supports multiple Parquet files with custom table names

**Example Usage**:
```bash
# Execute query and show results
cargo run -p executor -- -f data/orders.parquet -q "SELECT COUNT(*) FROM orders"

# Show physical execution plan
cargo run -p executor -- -f data/orders.parquet -q "SELECT COUNT(*) FROM orders" --physical-plan

# Generate Substrait plan (text format)
cargo run -p executor -- -f data/orders.parquet -q "SELECT COUNT(*) FROM orders" --substrait-text

# Generate Substrait plan (binary format)
cargo run -p executor -- -f data/orders.parquet -q "SELECT COUNT(*) FROM orders" --substrait -o plan.pb
```

### 2. DataFusion CLI (`data-embed/datafusion-cli/`)
- **Purpose**: Legacy SQL → Substrait converter with manifest generation
- **Status**: ✅ Working (superseded by executor for most use cases)
- **Unique Feature**: Generates execution manifests (JSON with Substrait + file paths)

### 3. Test Data Generator (`data-embed/generate-test-data/`)
- **Purpose**: Create sample Parquet datasets for testing
- **Output**: `data/orders.parquet` (10,000 rows)
- **Schema**:
  - `id`: Int64
  - `customer_id`: Int64
  - `amount`: Float64
  - `quantity`: Int32
  - `status`: String (pending/shipped/delivered/cancelled)
- **Status**: ✅ Working

## Architecture Overview

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────────┐
│   SQL       │────▶│  DataFusion  │────▶│  Substrait   │────▶│   Sirius    │
│   Query     │     │  (Rust)      │     │  Protobuf    │     │  (C++/CUDA) │
└─────────────┘     └──────────────┘     └──────────────┘     └─────────────┘
                    Parse & Optimize     Language-agnostic    GPU Execution
                                         IR (.pb file)
```

### Component Status
- ✅ **SQL Input**: User provides queries
- ✅ **DataFusion**: Parsing, optimization, schema validation
- ✅ **Substrait**: Serialization to protobuf
- - **Sirius**: in progress to be implement  (requires CUDA hardware)
- ❌ **FFI Bridge**: Not yet implemented (Rust ↔ C++)
- ❌ **Results**: Not yet returned from GPU

## Technical Decisions Made

### 1. Language Choice: Rust
- **Why**: DataFusion is native Rust, excellent FFI support, zero-cost abstractions
- **Tradeoff**: Need FFI bridge to C++ Sirius (vs pure Python which would be simpler but slower)

### 2. Data Format: Parquet
- **Why**: Columnar format, zero-copy GPU transfers, industry standard
- **Tradeoff**: No support for row-oriented formats (CSV/JSON)

### 3. IR: Substrait
- **Why**: Language-agnostic, protobuf-based, designed for this exact use case
- **Tradeoff**: Not all DataFusion features supported (but sufficient for our scope)

### 4. Scope: Single-table aggregations first
- **Why**: Simplest path to end-to-end validation
- **Next**: Multi-table JOINs, subqueries, window functions

## Critical Constraint: Mac GPU Limitation

**Your Mac has Apple M1 Pro with Metal 3, NOT NVIDIA CUDA.**

This means:
- ✅ DataFusion frontend works perfectly on Mac
- ❌ Sirius GPU execution requires NVIDIA GPU (Linux + CUDA)
- 📋 **Strategy**: Build and test DataFusion locally, deploy Sirius on cloud (AWS G4dn/G5)

## What Works Right Now

### Rust SQL Execution

```bash
cd data-embed

# Generate test data (run once)
cargo run --release --bin generate-test-data

# Execute query and see results
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT COUNT(*), SUM(amount) FROM orders WHERE status = 'shipped'"

# Inspect physical execution plan
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT COUNT(*) FROM orders" \
  --physical-plan

# Generate Substrait plan (human-readable)
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT COUNT(*) FROM orders" \
  --substrait-text

# Generate Substrait plan (binary for GPU)
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT COUNT(*) FROM orders" \
  --substrait \
  -o output/my_query.pb
```

### Python Kernel with FFI

```bash
cd data-kernel

# Set up environment (first time only - uses uv for fast dependency resolution)
uv sync
source .venv/bin/activate

# Set data path
export DATA_PATH=../data-embed/data

# Test FFI integration
python test_integration.py

# Use in Python directly
python -c "
from data_kernel import arrow_bridge
result = arrow_bridge.execute_query('SELECT COUNT(*) FROM orders')
print(f'Count: {result[0]}')
"

# Start Jupyter with the data kernel
jupyter notebook
```

**Supported SQL**:
- SELECT with projections
- WHERE filters (comparison operators)
- GROUP BY
- Aggregations: COUNT, SUM, AVG, MIN, MAX
- ORDER BY
- LIMIT
- Column aliases

## Recent Fixes: Query Execution (March 2026)

### Problem: Error -5 Failures and Incorrect Results

All queries with WHERE clauses, GROUP BY, COUNT, and multi-aggregations were failing with error code -5 or returning garbage values.

**Root Cause Analysis**:

1. **Broken GPU Path** (`executor/src/executor.rs:116-220`):
   - `execute_to_arrow_gpu()` only supported SUM aggregations
   - Called `wgpu_engine::run_sum_aggregation()` for ALL aggregations
   - Always returned single "sum" column regardless of actual query
   - Failed on WHERE clauses, GROUP BY, COUNT/MIN/MAX/AVG
   - Line 179: `.unwrap()` panicked on empty batches from COUNT queries

2. **Batch Concatenation Bug** (`executor/src/executor.rs:432`):
   - `batches.into_iter().next()` only took first batch
   - GROUP BY queries often return multiple batches - rest were discarded
   - Resulted in empty or partial results for string-based GROUP BY

**Fixes Applied**:

1. **Disabled GPU Path** (`executor/src/lib.rs:44-54`):
   ```rust
   // Temporarily route all queries through CPU path
   if false && gpu_available {
       execute_query_gpu(...)  // Disabled until properly implemented
   } else {
       execute_query_cpu(...)  // Uses DataFusion correctly
   }
   ```

2. **Fixed Batch Concatenation** (`executor/src/executor.rs:432-459`):
   ```rust
   // Before: Only first batch
   if let Some(batch) = batches.into_iter().next() { ... }

   // After: Concatenate all batches
   let combined_batch = if batches.len() == 1 {
       batches.into_iter().next().unwrap()
   } else {
       concat_batches(&schema, &batches)?
   };
   ```

### Test Results: All Queries Pass ✅

Comparison test (`data-kernel/test_gpu_comparison.py`) results:

| Query Type | Status | DuckDB | data-kernel | Speedup |
|------------|--------|--------|-------------|---------|
| Q1: COUNT/AVG with WHERE | ✅ PASS | 0.019s | 0.102s | 0.19x |
| Q2: MIN/MAX/AVG | ✅ PASS | 0.009s | 0.003s | **2.94x** |
| Q3: GROUP BY String | ✅ PASS | 0.009s | 0.009s | 1.00x |
| Q4: GROUP BY Integer | ✅ PASS | 0.010s | 0.003s | **2.76x** |
| Q5: Window Function | ✅ PASS | 0.013s | 0.008s | **1.66x** |
| Q6: JOIN with Stats | ✅ PASS | 0.010s | 0.006s | **1.67x** |
| Q7: JOIN + GROUP BY | ✅ PASS | 0.010s | 0.005s | **1.87x** |

**Overall: 7/7 queries passed**

### Query Execution Architecture

```
arrow_bridge.execute_query(sql)
    ↓
lib.rs: execute_query_to_arrow()
    ↓
    ├─→ [GPU DISABLED] execute_query_gpu() ─→ execute_to_arrow_gpu()
    │                                            ├─ execute_simple_agg_gpu() [BROKEN]
    │                                            ├─ execute_join_gpu() [CPU fallback]
    │                                            └─ execute_table_scan_cpu()
    │
    └─→ [CURRENT] execute_query_cpu() ─→ execute_to_arrow()
                                           └─ Uses DataFusion (fully functional)
```

**Current State**: All queries use CPU path through DataFusion, which is fully functional and correct.

### Re-enabling GPU: TODO List

To properly implement GPU acceleration, `execute_to_arrow_gpu()` needs:

1. **WHERE Clause Support**:
   - Currently panics on filtered queries
   - Need GPU kernel for predicate evaluation

2. **GROUP BY Support**:
   - Requires GPU hash-based grouping
   - Both string and numeric keys

3. **All Aggregation Functions**:
   - Currently only SUM works (partially)
   - Need: COUNT, MIN, MAX, AVG
   - Multiple aggregations in single query

4. **Proper Error Handling**:
   - Remove `.unwrap()` calls that panic
   - Graceful fallback to CPU on GPU errors

5. **Multi-Batch Results**:
   - Already fixed in CPU path
   - Apply same fix to GPU path when re-enabled

**Files to Modify**:
- `data-embed/executor/src/executor.rs` - GPU execution functions
- `data-embed/executor/src/wgpu_engine.rs` - GPU kernels
- `data-embed/executor/src/lib.rs` - Re-enable GPU routing when ready

**Testing**: Use `data-kernel/test_gpu_comparison.py` to verify GPU results match DuckDB.

### Diagnostic Test Files

Created in `data-kernel/` for debugging query execution:

- `test_gpu_comparison.py` - Full comparison suite (7 queries, DuckDB vs data-kernel)
- `test_simple_gpu.py` - Quick diagnostic for basic query types
- `test_string_columns.py` - Tests string column handling and GROUP BY
- `test_groupby_debug.py` - Detailed GROUP BY debugging
- `test_q7_debug.py` - JOIN + GROUP BY value comparison

**Usage**:
```bash
cd data-kernel
export DATA_PATH=/Users/luke.shulman/Projects/albatross-data/data-kernel
python test_gpu_comparison.py  # Run full test suite
```

All tests generate their own parquet test data automatically.

## Next Implementation Steps

### Phase 3: Sirius Integration (Requires CUDA Hardware)

#### Step 1: Environment Setup
- [ ] Provision Linux machine with NVIDIA GPU (AWS G4dn, G5, or local)
- [ ] Install CUDA Toolkit (version compatible with Sirius)
- [ ] Install cuDF, RMM (RAPIDS libraries)
- [ ] Build Sirius from `lib/sirius/`

**Reference**: `lib/sirius/CLAUDE.md` has build instructions

#### Step 2: FFI Bridge (Rust → C++)
- [ ] Choose FFI approach: `cxx` crate (type-safe) or `bindgen` (auto-generated)
- [ ] Create Rust wrapper for Sirius execution function
- [ ] Pass Substrait bytes from Rust to C++
- [ ] Handle errors across FFI boundary

**Key Challenge**: Memory safety across language boundaries

#### Step 3: Data Loading
- [ ] Sirius reads Parquet file paths from Substrait plan
- [ ] Uses libcudf to load data directly to GPU VRAM
- [ ] Verify GPUDirect is working (zero-copy transfers)

#### Step 4: Execution
- [ ] Sirius executes Substrait plan on GPU
- [ ] Maps Substrait operators to libcudf kernels
- [ ] Handles VRAM allocation via RMM

#### Step 5: Results Return (C++ → Rust)
- [ ] Sirius returns Arrow RecordBatch from GPU
- [ ] Use Arrow C Data Interface for zero-copy transfer
- [ ] Convert to Rust `arrow::RecordBatch`
- [ ] Display or write results

**Key Challenge**: Zero-copy data transfer without serialization overhead

### Phase 4: Validation & Benchmarking
- [ ] Run test queries end-to-end
- [ ] Verify results match CPU execution
- [ ] Measure latency: SQL → Substrait → GPU → Results
- [ ] Compare vs PostgreSQL, DuckDB, CPU-only DataFusion
- [ ] Scale to larger datasets (1M, 10M, 100M rows)

## Code Structure

```
albatross-data/
├── IMPLEMENTATION.md          # Original architecture plan
├── AGENT.md                   # This file - implementation guide
├── data-embed/                # DataFusion frontend (Rust)
│   ├── Cargo.toml            # Workspace definition
│   ├── executor/             # ⭐ Unified SQL executor
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs       # CLI with multiple modes
│   │       └── executor.rs   # Core execution logic
│   ├── datafusion-cli/       # Legacy SQL → Substrait CLI
│   │   ├── Cargo.toml
│   │   └── src/main.rs
│   ├── generate-test-data/   # Parquet generator
│   │   ├── Cargo.toml
│   │   └── src/main.rs
│   ├── data/                 # Test datasets
│   │   └── orders.parquet    # 10K rows
│   ├── output/               # Generated Substrait plans
│   └── README.md             # Usage documentation
├── lib/sirius/               # Sirius GPU engine (C++/CUDA)
│   ├── CLAUDE.md             # Build instructions
│   └── ...                   # Full source code
└── frontend/                 # (Empty - future web UI)
```

## Dependencies

### Rust (data-embed/)
- `datafusion` 43.0 - SQL parsing, optimization
- `datafusion-substrait` 43.0 - Substrait serialization
- `arrow` 53.3 - Columnar data format
- `parquet` 53.3 - Parquet I/O
- `tokio` 1.41 - Async runtime
- `clap` 4.5 - CLI parsing
- `prost` 0.13 - Protobuf encoding

**Installation**: `cargo build --release` (handled by rustup/cargo)

### Python (data-kernel/)
- `ipykernel` - Jupyter kernel protocol
- `pandas` - DataFrame manipulation
- `pyarrow` - Arrow format bindings
- `jupyter-mimetypes` - MIME type support

**Installation**: `uv sync` (fast, reliable dependency resolution)

> **Important**: This project uses `uv` instead of traditional pip/virtualenv. Run `uv sync` to install dependencies and set up the virtual environment automatically.

### C++ (lib/sirius/)
- CUDA Toolkit - GPU programming
- cuDF - GPU DataFrame library
- RMM - RAPIDS Memory Manager
- DuckDB - (Sirius is a DuckDB extension)

**Installation**: See `lib/sirius/CLAUDE.md` for build instructions

## Known Limitations

### DataFusion → Substrait
- Not all DataFusion features serialize to Substrait
- Some complex expressions may fail
- Window functions support unclear

### Sirius
- Only supports subset of SQL operations
- VRAM limitations for large datasets
- Requires NVIDIA GPU (no AMD, no Apple Metal)

### Current Scope
- Single-table queries only (no JOINs tested yet)
- Small dataset (10K rows)
- No error recovery or retry logic
- No result validation against ground truth

## Testing Strategy

### Unit Tests (Not Yet Implemented)
- [ ] Test Substrait serialization for various SQL patterns
- [ ] Test Parquet generation with different schemas
- [ ] Test CLI argument parsing edge cases

### Integration Tests (Not Yet Implemented)
- [ ] End-to-end: SQL → Substrait → Sirius → Results
- [ ] Verify results match CPU execution
- [ ] Test error handling (invalid SQL, missing files, etc.)

### Performance Tests (Not Yet Implemented)
- [ ] Measure query latency at different dataset sizes
- [ ] Compare GPU vs CPU execution time
- [ ] Identify bottlenecks (parsing, serialization, GPU transfer, execution)

## Debugging Tips

### DataFusion Issues
- Use `--verbose` flag to see logical plan
- Check DataFusion logs for optimization steps
- Verify Parquet schema matches SQL query expectations

### Substrait Issues
- Inspect `.pb` files with `protoc --decode`
- Check Substrait version compatibility between DataFusion and Sirius
- Look for unsupported operators in plan

### Sirius Issues (Future)
- Check CUDA/cuDF installation
- Verify GPU memory availability
- Look at Sirius logs for execution errors
- Use `nvidia-smi` to monitor GPU utilization

## Questions for Next Session

1. **GPU Access**: When/how will you get access to NVIDIA GPU hardware?
   - Local Linux machine?
   - AWS/GCP/Azure cloud instance?
   - Remote server?

2. **Sirius Build**: Have you built Sirius before?
   - Need help with build process?
   - Dependencies installed?

3. **FFI Approach**: Preference for FFI bridge?
   - `cxx` (type-safe, more boilerplate)
   - `bindgen` (auto-generated, less safe)

4. **Scope**: After Sirius integration, what's next?
   - JOINs and multi-table queries?
   - Larger datasets (1M+ rows)?
   - Web UI (AG-UI integration)?
   - Benchmarking vs other systems?

## Resources

- **DataFusion Docs**: https://docs.rs/datafusion/
- **Substrait Spec**: https://substrait.io/
- **Sirius Build Guide**: `lib/sirius/CLAUDE.md`
- **Arrow C Data Interface**: https://arrow.apache.org/docs/format/CDataInterface.html
- **RAPIDS cuDF**: https://docs.rapids.ai/api/cudf/stable/

## Summary

**Phase 2A Complete**:
- ✅ DataFusion frontend working
- ✅ SQL queries convert to Substrait protobuf plans
- ✅ FFI bridge functional (Rust ↔ Python)
- ✅ Python Jupyter kernel operational
- ✅ Arrow data transfers working

**Next Milestone**: Sirius integration on CUDA hardware. Pass `.pb` files to GPU, execute, return results.

**Blocker**: Need NVIDIA GPU access (Mac has Metal, not CUDA).

**Ready to Continue**: Frontend and FFI bridge complete. Next session should focus on Sirius GPU engine deployment and integration.

## Getting Help

1. **Setup Issues**: See [README.md](./README.md) troubleshooting section
2. **Build Errors**: Check that `uv sync` completed successfully for Python, `cargo build --release` for Rust
3. **FFI Issues**: See [INTEGRATION_COMPLETE.md](./INTEGRATION_COMPLETE.md) for detailed FFI bridge documentation
4. **Architecture Questions**: See the diagrams in this file and README.md

## Developer Onboarding Checklist

Use this checklist to verify your development environment is set up correctly:

### Prerequisites
- [ ] Rust installed (check: `rustc --version`)
- [ ] Python 3.10+ installed (check: `python --version`)
- [ ] uv installed (check: `uv --version`)
- [ ] Git repository cloned

### First Build
- [ ] `cd data-embed && cargo build --release` completes successfully
- [ ] Test data generated: `cargo run --release --bin generate-test-data`
- [ ] Sample query works: `cargo run -p executor -- -f data/orders.parquet -q "SELECT COUNT(*) FROM orders"`
- [ ] `cd ../data-kernel && uv sync` completes successfully
- [ ] `source .venv/bin/activate` activates virtual environment
- [ ] Full build works: `cd .. && make build`

### Verify Components
- [ ] Rust executor runs queries: `data-embed/target/release/data-run --help`
- [ ] Python FFI bridge works: `cd data-kernel && python test_integration.py`
- [ ] Environment variable set: `export DATA_PATH=$(pwd)/data-embed/data`

### Ready to Develop
- [ ] Can run Rust tests: `cd data-embed && cargo test`
- [ ] Can modify and rebuild: edit code, run `make build`
- [ ] Understand project structure (see README.md)
- [ ] Read through example queries in this file

**If all boxes are checked**, you're ready to start developing! If not, see the troubleshooting section in README.md.
