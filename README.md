# Albatross Data

A GPU-accelerated composable data pipeline that bypasses traditional databases by combining DataFusion (SQL parsing), Substrait (language-agnostic IR), and GPU execution for high-performance data analytics.

## Overview

Albatross Data consists of three main components:

1. **data-embed**: Rust-based SQL execution engine using DataFusion that converts SQL queries to Substrait protobuf plans
2. **data-kernel**: Python Jupyter kernel that provides an interactive interface for SQL queries with FFI bridge to Rust
3. **lib/sirius**: GPU execution engine (C++/CUDA) for running Substrait plans on NVIDIA GPUs

## Architecture

```
SQL Query
    ↓
DataFusion (Rust) - Parse & Optimize SQL
    ↓
Substrait Protobuf - Language-agnostic IR
    ↓
Sirius (C++/CUDA) - GPU Execution
    ↓
Results (Arrow Format)
```

## Prerequisites

### Required
- **Rust** (1.70+): Install via [rustup](https://rustup.rs/)
- **Python** (3.10+): For the data-kernel component
- **uv**: Fast Python package installer - `pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`

### Optional (for GPU execution)
- **CUDA Toolkit**: Required for Sirius GPU execution (Linux with NVIDIA GPU only)
- **cuDF/RAPIDS**: GPU DataFrame libraries
- **Docker**: For containerized deployment

### Development Tools
- **Make**: For running build commands
- **Git**: Version control

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd albatross-data
```

### 2. Build the Rust Components

```bash
cd data-embed
cargo build --release
```

This builds:
- `executor`: Unified SQL executor with multiple output modes
- `datafusion-cli`: Legacy SQL → Substrait converter
- `generate-test-data`: Test data generator

### 3. Generate Test Data

```bash
cd data-embed
cargo run --release --bin generate-test-data
```

This creates `data/orders.parquet` with 10,000 sample rows.

### 4. Test SQL Execution

```bash
# Execute a simple query
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT COUNT(*) FROM orders"

# Show the physical execution plan
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT status, COUNT(*) as cnt FROM orders GROUP BY status" \
  --physical-plan

# Generate Substrait plan
cargo run -p executor -- \
  -f data/orders.parquet \
  -q "SELECT * FROM orders LIMIT 10" \
  --substrait-text
```

### 5. Set Up Python Kernel (Optional)

```bash
cd ../data-kernel

# Use uv to sync dependencies and set up the environment
uv sync

# Activate the virtual environment
source .venv/bin/activate

# Build the full system (Rust + Python)
cd ..
make build
```

**Important**: This project uses `uv sync` for Python dependency management, which is faster and more reliable than traditional pip-based workflows.

### 6. Test Python Integration

```bash
cd data-kernel
export DATA_PATH=../data-embed/data
python test_integration.py
```

## Project Structure

```
albatross-data/
├── README.md                      # This file
├── AGENT.md                       # Detailed implementation guide for developers
├── INTEGRATION_COMPLETE.md        # FFI integration documentation
├── Makefile                       # Build automation
│
├── data-embed/                    # Rust SQL execution engine
│   ├── Cargo.toml                # Workspace definition
│   ├── executor/                 # Main SQL executor (unified CLI)
│   │   ├── src/
│   │   │   ├── main.rs          # CLI with multiple output modes
│   │   │   ├── lib.rs           # FFI exports for Python integration
│   │   │   └── executor.rs      # Core execution logic
│   │   └── Cargo.toml
│   ├── datafusion-cli/           # Legacy SQL → Substrait CLI
│   ├── generate-test-data/       # Test data generator
│   ├── data/                     # Parquet test datasets
│   └── output/                   # Generated Substrait plans
│
├── data-kernel/                   # Python Jupyter kernel
│   ├── pyproject.toml            # Python project config (uses uv)
│   ├── src/data_kernel/
│   │   ├── __init__.py          # Module entry point
│   │   ├── kernel.py            # Jupyter kernel implementation
│   │   ├── execute.py           # Query execution logic
│   │   └── arrow_bridge.c       # FFI bridge to Rust
│   ├── docker/                   # Docker build context
│   └── test_integration.py       # Integration tests
│
├── lib/sirius/                    # GPU execution engine (future)
│   └── CLAUDE.md                 # Sirius build instructions
│
└── scripts/                       # Build and deployment scripts
```

## Common Development Tasks

### Building

```bash
# Build only Rust components
cd data-embed && cargo build --release

# Build Rust + Python (requires uv sync first)
make build

# Build Python wheel for distribution
make data-kernel-wheel

# Build Docker image
make docker-build
```

### Testing

```bash
# Run Rust tests
cd data-embed
cargo test

# Run Python integration tests
cd data-kernel
uv sync  # Ensure dependencies are installed
source .venv/bin/activate
export DATA_PATH=../data-embed/data
python test_integration.py
```

### Running Queries

The `executor` binary supports multiple output modes:

```bash
# Execute and show results (default)
cargo run -p executor -- -f data/file.parquet -q "SELECT ..."

# Show logical plan
cargo run -p executor -- -f data/file.parquet -q "SELECT ..." --explain-only

# Show physical plan
cargo run -p executor -- -f data/file.parquet -q "SELECT ..." --physical-plan

# Generate Substrait (human-readable)
cargo run -p executor -- -f data/file.parquet -q "SELECT ..." --substrait-text

# Generate Substrait (binary protobuf)
cargo run -p executor -- -f data/file.parquet -q "SELECT ..." --substrait -o plan.pb

# CSV output
cargo run -p executor -- -f data/file.parquet -q "SELECT ..." --csv
```

### Working with Multiple Tables

```bash
# Provide multiple files with custom table names
cargo run -p executor -- \
  -f data/orders.parquet:orders \
  -f data/customers.parquet:customers \
  -q "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"
```

### Python Kernel Usage

```bash
# Set data directory
export DATA_PATH=/path/to/parquet/files

# Start Jupyter
cd data-kernel
uv sync
source .venv/bin/activate
jupyter notebook

# Or use the kernel directly
python -c "
from data_kernel import arrow_bridge
result = arrow_bridge.execute_query('SELECT COUNT(*) FROM orders')
print(result.to_pylist())
"
```

## Dependencies

### Rust (data-embed/)
- `datafusion` 43.0 - SQL parsing, optimization, execution
- `datafusion-substrait` 43.0 - Substrait protobuf serialization
- `arrow` 53.3 - Columnar data format
- `parquet` 53.3 - Parquet file I/O
- `tokio` 1.41 - Async runtime
- `clap` 4.5 - CLI argument parsing

### Python (data-kernel/)
- `ipykernel` - Jupyter kernel protocol
- `pandas` - DataFrame manipulation
- `pyarrow` - Arrow format bindings
- `jupyter-mimetypes` - MIME type support

Managed via `uv` for fast, reliable dependency resolution.

## Current Status

### Working
- SQL parsing and optimization via DataFusion
- Substrait protobuf generation
- Multiple Parquet file support
- FFI bridge (Rust ↔ Python)
- Arrow data transfer across language boundaries
- Jupyter kernel integration
- Aggregations: COUNT, SUM, AVG, MIN, MAX
- Filtering, GROUP BY, ORDER BY, LIMIT

### In Progress
- Sirius GPU execution engine integration
- Multi-table JOINs
- Window functions
- Comprehensive error handling

### Known Limitations
- GPU execution requires NVIDIA CUDA hardware (not available on Mac M1)
- Currently exports only first column of query results via FFI (full RecordBatch support coming)
- Single-table queries primarily tested
- No result validation against ground truth yet

## Development Workflow

### Setting Up Your Environment

```bash
# 1. Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2. Install uv for Python dependency management
pip install uv
# or
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Clone and build
git clone <repository-url>
cd albatross-data
cd data-embed && cargo build --release

# 4. Set up Python environment
cd ../data-kernel
uv sync  # This is the key command for Python setup!

# 5. Build everything
cd ..
make build
```

### Making Changes

1. **Rust changes**: Edit files in `data-embed/`, run `cargo build --release`
2. **Python changes**: Edit files in `data-kernel/src/data_kernel/`, run `make build` if FFI changes
3. **FFI bridge**: Changes to `arrow_bridge.c` or `lib.rs` require full rebuild via `make build`
4. **Dependencies**:
   - Rust: Edit `Cargo.toml`, run `cargo update`
   - Python: Edit `pyproject.toml`, run `uv sync`

### Adding New Features

See [AGENT.md](./AGENT.md) for detailed implementation guidance on:
- Adding new SQL operations
- Extending Substrait support
- FFI bridge enhancements
- GPU execution integration

## Documentation

- **[AGENT.md](./AGENT.md)**: Comprehensive implementation guide for developers
- **[INTEGRATION_COMPLETE.md](./INTEGRATION_COMPLETE.md)**: FFI integration details
- **[BENCHMARK_RESULTS.md](./BENCHMARK_RESULTS.md)**: Performance benchmarks
- **[lib/sirius/CLAUDE.md](./lib/sirius/CLAUDE.md)**: GPU engine build instructions

## Contributing

When contributing to this project:

1. Follow the existing code style (Rust: `cargo fmt`, Python: PEP 8)
2. Add tests for new features
3. Update documentation (README.md, AGENT.md)
4. Ensure all builds pass: `make build`
5. Test both Rust and Python components

## Troubleshooting

### Rust Build Errors
- Ensure you have the latest Rust: `rustup update`
- Check LLVM/clang installation if linker errors occur
- On macOS, ensure Xcode command line tools are installed

### Python Build Errors
- Use `uv sync` to ensure correct dependencies
- Check that Rust libraries are built: `cd data-embed && cargo build --release`
- Verify `DATA_PATH` environment variable is set correctly

### FFI Issues
- Ensure Rust library is in the correct location: `data-kernel/src/data_kernel/libexecutor.so` (Linux/Mac)
- Check Arrow version compatibility between Rust and Python
- Verify memory management with tools like `valgrind`

## License

[Include your license information here]

## Contact

[Include contact information or links to issue tracker]

---

**Note**: This project is in active development. The Sirius GPU execution component requires NVIDIA CUDA hardware and is currently being integrated. The DataFusion frontend is fully functional and can be used for SQL query analysis and Substrait plan generation on any platform.
