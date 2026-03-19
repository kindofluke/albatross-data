# AGENT.md - Implementation Guide

## Project Status: Phase 2A Complete ✅

**Goal**: GPU-accelerated composable data pipeline bypassing traditional databases.

**Current State**: DataFusion frontend working. Converts SQL → Substrait protobuf plans.

## What's Been Built

### 1. DataFusion CLI (`data-embed/datafusion-cli/`)
- **Purpose**: Parse SQL queries and generate Substrait plans
- **Input**: SQL query string + Parquet file path
- **Output**: Substrait protobuf (`.pb` file)
- **Status**: ✅ Working

**Key Code**: `data-embed/datafusion-cli/src/main.rs`
- Uses `datafusion` crate for SQL parsing and optimization
- Uses `datafusion-substrait::logical_plan::producer::to_substrait_plan()` for serialization
- Outputs binary protobuf files ready for GPU execution

### 2. Test Data Generator (`data-embed/generate-test-data/`)
- **Purpose**: Create sample Parquet datasets for testing
- **Output**: `data/orders.parquet` (10,000 rows)
- **Schema**:
  - `id`: Int64
  - `customer_id`: Int64
  - `amount`: Float64
  - `quantity`: Int32
  - `status`: String (pending/shipped/delivered/cancelled)
- **Status**: ✅ Working

### 3. Test Queries Generated
Four example Substrait plans in `data-embed/output/`:
- `query1.pb`: Simple aggregations (COUNT, SUM, AVG)
- `query2.pb`: GROUP BY with aggregations
- `query3.pb`: WHERE filter with LIMIT
- `query4.pb`: Complex query (GROUP BY + ORDER BY + LIMIT)

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
- ❌ **Sirius**: Not yet integrated (requires CUDA hardware)
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

```bash
cd data-embed

# Generate test data (already done)
cargo run --release --bin generate-test-data

# Convert SQL to Substrait
cargo run --release --bin datafusion-cli -- \
  -q "SELECT COUNT(*), SUM(amount) FROM orders WHERE status = 'shipped'" \
  -p data/orders.parquet \
  -o output/my_query.pb \
  -v
```

**Supported SQL**:
- SELECT with projections
- WHERE filters (comparison operators)
- GROUP BY
- Aggregations: COUNT, SUM, AVG, MIN, MAX
- ORDER BY
- LIMIT
- Column aliases

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
│   ├── datafusion-cli/       # SQL → Substrait CLI
│   │   ├── Cargo.toml
│   │   └── src/main.rs       # Main CLI logic
│   ├── generate-test-data/   # Parquet generator
│   │   ├── Cargo.toml
│   │   └── src/main.rs
│   ├── data/                 # Test datasets
│   │   └── orders.parquet    # 10K rows
│   ├── output/               # Generated Substrait plans
│   │   ├── query1.pb
│   │   ├── query2.pb
│   │   ├── query3.pb
│   │   └── query4.pb
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

### C++ (lib/sirius/)
- CUDA Toolkit - GPU programming
- cuDF - GPU DataFrame library
- RMM - RAPIDS Memory Manager
- DuckDB - (Sirius is a DuckDB extension)

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

**Phase 2A Complete**: DataFusion frontend working. SQL queries successfully convert to Substrait protobuf plans.

**Next Milestone**: Sirius integration on CUDA hardware. Pass `.pb` files to GPU, execute, return results.

**Blocker**: Need NVIDIA GPU access (Mac has Metal, not CUDA).

**Ready to Continue**: All code is in place for frontend. Next session should focus on Sirius build and FFI bridge.
