# Data-Embed GPU Implementation Architecture Summary

## Overview

The `data-embed` directory contains a GPU-accelerated SQL query executor built with Rust and WGPU (WebGPU abstraction layer). The system converts SQL queries to execution plans, then offloads computational operations to GPUs using platform-agnostic shader code.

## Directory Structure

```
data-embed/
├── README.md                          # High-level pipeline documentation
├── Cargo.toml                         # Workspace root
├── data/                              # Test data (Parquet files)
├── output/                            # Query output artifacts
├── dist/                              # Distribution/bundle artifacts
│
├── datafusion-cli/                    # SQL → Substrait converter
│   ├── Cargo.toml
│   ├── src/main.rs                    # CLI entry point
│   └── src/manifest.rs                # Manifest handling
│
├── generate-test-data/                # Test data generation utility
│   ├── Cargo.toml
│   └── src/main.rs                    # Generates sample Parquet files
│
└── executor/                          # Main GPU execution engine
    ├── Cargo.toml                     # Dependencies: wgpu, arrow, datafusion, etc.
    ├── README.md                      # Executor documentation
    ├── GPU_IMPLEMENTATION_SUMMARY.md  # Detailed GPU implementation notes
    ├── src/
    │   ├── lib.rs                     # FFI exports (C-compatible API)
    │   ├── main.rs                    # CLI binary for direct query execution
    │   ├── executor.rs                # CPU/GPU query dispatcher
    │   │
    │   ├── wgpu_engine.rs            # Main GPU engine facade (316 lines)
    │   ├── wgsl_shader.rs            # WGSL compute shader sources (527 lines)
    │   │
    │   ├── gpu_types.rs              # Result types (AggregateResult, GroupResult)
    │   ├── gpu_buffers.rs            # GPU memory management utilities
    │   ├── gpu_dispatch.rs           # Workgroup dispatch helpers
    │   ├── gpu_pipeline.rs           # Pipeline builder pattern
    │   │
    │   ├── aggregations.rs           # GPU aggregation operations
    │   ├── joins.rs                  # GPU hash join operations
    │   └── window.rs                 # GPU window function operations
    │
    └── examples/
        ├── benchmark_gpu.rs           # GPU vs CPU aggregation benchmarks
        ├── benchmark_gpu_100m.rs      # Large dataset benchmarks
        └── test_all_ops.rs            # Comprehensive operation tests
```

## Overall Architecture

### 1. SQL Processing Pipeline

```
SQL Query
   ↓
DataFusion (Parse & Optimize)
   ↓
Logical Plan
   ↓
Physical Plan
   ↓
DataFusion Execution (collect data)
   ↓
Arrow RecordBatch
   ↓
[Route to CPU or GPU]
   ├─→ CPU Path: Standard DataFusion execution
   └─→ GPU Path: Extract arrays → GPU compute → Read results
```

### 2. Query Execution Router

The system detects query type and routes appropriately:

```rust
// From executor.rs - execute_to_arrow_gpu()
Query Analysis:
- Is it a JOIN?        → execute_join_gpu()
- Is it AGGREGATION?   → execute_simple_agg_gpu()
- Otherwise?           → execute_table_scan_cpu()
```

## GPU Implementation Strategy

### 1. Cross-Platform GPU Support via WGPU

**Library:** `wgpu 23` (WebGPU abstraction layer)

**Key Benefits:**
- Unified abstraction for Metal (Mac), Vulkan (Linux), DX12 (Windows)
- Platform detection and backend selection automatic
- No native dependencies (no CUDA toolkit, Metal framework bindings, etc.)

**Backend Support:**
```
WGPU Instance
    ├─ Metal (macOS) - IntegratedGpu (Apple Silicon) or DiscreteGpu
    ├─ Vulkan (Linux) - Discrete/Integrated GPU support
    └─ DX12 (Windows) - GPU support via DirectX
```

### 2. GPU Detection and Info API

**File:** `wgpu_engine.rs` (lines 42-90)

```rust
pub async fn is_gpu_available() -> bool
    // Lightweight check for GPU adapter availability
    
pub async fn get_gpu_info() -> Option<GpuInfo>
    // Returns: name, backend, device_type, driver, driver_info
```

**FFI Exports (C-compatible):**
```rust
#[no_mangle] pub extern "C" fn check_gpu_available() -> i32
#[no_mangle] pub extern "C" fn get_gpu_information() -> *mut CGpuInfo
#[no_mangle] pub extern "C" fn free_gpu_info(info: *mut CGpuInfo)
```

### 3. Shader-Based Computation

**Language:** WGSL (WebGPU Shading Language)
**File:** `wgsl_shader.rs` (527 lines)

All shaders use:
- Compute shaders (`@compute` entry points)
- Workgroup size: 256 threads (tunable in `gpu_dispatch.rs`)
- 2D dispatch support for datasets > 16M elements

**Atomic Operations Pattern:**
Since WGSL lacks native float atomics, shaders use bitcast + compare-and-swap:
```wgsl
// Float addition via atomic CAS
loop {
    let old_bits = atomicLoad(&result);
    let old_val = bitcast<f32>(old_bits);
    let new_val = old_val + value;
    let new_bits = bitcast<u32>(new_val);
    let exchanged = atomicCompareExchangeWeak(&result, old_bits, new_bits);
    if (exchanged.exchanged) { break; }
}
```

## GPU Operation Implementation

### 1. Aggregation Operations (`aggregations.rs`)

**Two-Pass Global Aggregation Algorithm:**
```
Input: Values array (f32)
Output: AggregateResult { sum, count, min, max }

Pass 1 (GLOBAL_AGG_PASS1_SHADER):
  - Each workgroup (256 threads) reduces locally
  - Result: Array of per-workgroup aggregate results
  - Uses tree reduction + shared memory optimization
  
Pass 2 (GLOBAL_AGG_PASS2_SHADER):
  - Reduce all workgroup results to final value
  - Atomic operations for thread-safe updates
  - Result: Single final AggregateResult
```

**GROUP BY Aggregation:**
```
Input: Values array, Group keys array, Num groups
Output: Vec<GroupResult> - one result per group

Algorithm (GROUP_BY_AGG_SHADER):
  - Each thread: Load value & group key
  - Atomic operations on group results array
  - Sum, count, min, max per group
  - Handles atomic contention with CAS loops
```

**Status:** Functionally correct, GROUP BY working on 100+ groups

### 2. Join Operations (`joins.rs`)

**Hash Join Algorithm:**
```
Build Phase (HASH_JOIN_BUILD_SHADER):
  - Create hash table from build_keys
  - Table size: 2× build side (50% load factor)
  - Collision resolution: Linear probing with atomics
  
Probe Phase (HASH_JOIN_PROBE_SHADER):
  - For each probe key: lookup in hash table
  - On match: aggregate probe_values
  - Atomic updates to result aggregates
```

**Current Status:** Implemented but not integrated with DataFusion join execution

### 3. Window Functions (`window.rs`)

**Implemented:**
1. **ROW_NUMBER** (WINDOW_ROW_NUMBER_SHADER)
   - Trivial parallel operation
   - Each thread writes its index + 1

2. **RANK/DENSE_RANK** (WINDOW_RANK_FUNCTIONS_SHADER)
   - Pass 1: Peer group detection (compare adjacent sorted keys)
   - Pass 2: Parallel prefix sum on group starts
   - Requires pre-sorted data

3. **Cumulative Aggregation** (WINDOW_CUMULATIVE_AGG_SHADER)
   - Placeholder implementation
   - Requires multi-pass parallel scan (complex)
   - Host orchestration needed for full implementation

## Infrastructure Modules

### GPU Memory Management (`gpu_buffers.rs`)

```rust
Typed buffer utilities using bytemuck for zero-copy:
- create_storage_buffer<T>()        // Initialized GPU buffer
- create_output_buffer<T>()         // Uninitialized for output
- create_staging_buffer<T>()        // CPU-readable copy buffer
- read_buffer_single<T>()           // Async read single value
- read_buffer_vec<T>()              // Async read vector of values
- BufferBuilder pattern             // Convenient fluent interface
```

### Workgroup Dispatch (`gpu_dispatch.rs`)

```rust
Handles GPU hardware limits (max 65535 workgroups per dimension):

dispatch_1d_default(pass, element_count)
  // Single dispatch call, auto 2D fallback for large datasets
  
calculate_workgroup_dims(element_count, workgroup_size) → (x, y, z)
  // Returns actual workgroup counts

Support for:
- Datasets up to MAX_WORKGROUPS² × WORKGROUP_SIZE²
- Automatic 1D → 2D fallback
- Default 256-thread workgroups
```

### Pipeline Builder (`gpu_pipeline.rs`)

```rust
Builder pattern for compute pipelines:

PipelineBuilder::new(device, shader_source)
  .with_label("Pipeline Name")
  .add_buffer(BufferAccess::ReadOnly)      // Input
  .add_buffer(BufferAccess::ReadWrite)     // Output
  .build(&[&input_buf, &output_buf])
  
// Returns: (ComputePipeline, BindGroup, BindGroupLayout)
```

### Result Types (`gpu_types.rs`)

```rust
#[repr(C)]  // C-compatible memory layout
struct AggregateResult {
    sum: u32,       // Stored as f32 bits
    count: u32,
    min: u32,       // Stored as f32 bits
    max: u32,       // Stored as f32 bits
}

Helper methods: sum_f32(), min_f32(), max_f32(), avg()
```

## FFI Interface (C-compatible API)

**File:** `lib.rs` (358 lines)

### Query Execution Functions

```rust
#[no_mangle]
pub extern "C" fn execute_query_to_arrow(
    query: *const c_char,
    data_path: *const c_char,
    array_out: *mut *const FFI_ArrowArray,
    schema_out: *mut *const FFI_ArrowSchema,
) -> i32

// Routes to CPU or GPU based on availability and query type
// Returns: 0 on success, negative error codes

#[no_mangle]
pub extern "C" fn execute_query_cpu(...)  // CPU-only execution
pub extern "C" fn execute_query_gpu(...)  // GPU-only execution
```

### Arrow FFI Export

```rust
// Arrow C Data Interface (zero-copy across FFI boundary)
- FFI_ArrowArray        // C-compatible array structure
- FFI_ArrowSchema       // C-compatible schema structure

// Memory management
pub extern "C" fn release_arrow_pointers(array, schema)
```

### GPU Detection Functions

```rust
pub extern "C" fn check_gpu_available() -> i32        // 1=available, 0=not
pub extern "C" fn get_gpu_information() -> *mut CGpuInfo
pub extern "C" fn free_gpu_info(info: *mut CGpuInfo)
```

## Dependencies

**Workspace Dependencies** (`Cargo.toml`):
- `datafusion 43.0` - SQL parsing and optimization
- `datafusion-substrait 43.0` - Substrait serialization
- `arrow 53.3` - Columnar data format with FFI support
- `parquet 53.3` - Parquet file I/O
- `tokio` - Async runtime
- `wgpu 23` - **GPU abstraction layer (cross-platform)**
- `bytemuck 1.14` - Zero-copy type casting
- `prost 0.13` - Protocol buffers

**Executor-specific:**
```toml
[dependencies]
wgpu = "23"                              # GPU compute
bytemuck = { version = "1.14", features = ["derive"] }
pollster = "0.3"                         # Async block_on
futures = "0.3"                          # Async utilities
arrow = { version = "53.3.0", features = ["ffi"] }  # FFI support
```

## Metal (macOS) Execution Strategy

**How it works:**
1. WGPU detects Metal backend automatically on macOS
2. Shader compilation: WGSL → Metal Shading Language (automatic)
3. Command buffer submission via Metal API (abstracted by WGPU)
4. Both Apple Silicon (IntegratedGpu) and discrete GPUs supported

**No explicit Metal code required:**
- WGPU handles all Metal-specific details
- Same WGSL shaders work on all platforms
- Device/Queue initialization is platform-agnostic

## NVIDIA GPU (Linux) Execution Strategy

**How it works:**
1. WGPU detects Vulkan backend on Linux
2. Shader compilation: WGSL → SPIR-V (automatic)
3. Vulkan command submission via GPU driver
4. Supported on discrete NVIDIA cards via Vulkan driver

**Requirements:**
- Vulkan-capable NVIDIA driver
- WGPU handles Vulkan initialization automatically
- No CUDA toolkit required (uses Vulkan, not CUDA)

## Abstraction Layers for Cross-Platform Support

### WGPU Layer
```
Application Code (Rust)
    ↓
WGPU API (Device, Queue, ComputePipeline, etc.)
    ↓
Platform Backend Selection:
    ├─ Metal backend (macOS)
    ├─ Vulkan backend (Linux)
    └─ DX12 backend (Windows)
    ↓
GPU Hardware (Metal runtime, Vulkan driver, DX12 runtime)
```

### Shader Abstraction
```
WGSL Compute Shader (universal)
    ↓
WGPU Compilation:
    ├─ Metal: WGSL → Metal Shading Language
    ├─ Vulkan: WGSL → SPIR-V
    └─ DX12: WGSL → HLSL
    ↓
GPU Executable Code
```

## Build Configuration

### Cargo.toml Features
```toml
[lib]
crate-type = ["cdylib", "rlib"]
# cdylib: Dynamic library for FFI
# rlib: Rust library for crate usage

[dependencies]
arrow = { version = "53.3.0", features = ["ffi"] }
# ffi feature: Enables Arrow C Data Interface for FFI
```

### No Platform-Specific Code Needed
- Single codebase works on macOS, Linux, Windows
- WGPU selects appropriate backend at runtime
- No conditional compilation for GPU code

## Data Flow Example: GROUP BY Aggregation

```
SQL Query: "SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status"
    ↓
DataFusion Parsing & Optimization
    ↓
Physical Plan Execution (get data into Arrow)
    ↓
Executor::execute_to_arrow_gpu() [executor.rs:116]
    ├─ Detect: Contains GROUP BY → route to GPU aggregation
    ├─ Extract columns: status (group keys), amount (values)
    ├─ Convert to Rust slices
    ↓
WgpuEngine::execute_group_by_aggregation() [wgpu_engine.rs:260]
    ↓
AggregationOps::execute_group_by_aggregation() [aggregations.rs:47]
    ├─ Create GPU storage buffers for input data
    ├─ Create output buffer for results
    ├─ Create GROUP_BY_AGG shader pipeline
    ├─ Dispatch: one thread per input element
    ├─ Each thread: atomic update to result[group_key]
    ↓
Staging buffer copy & async read
    ↓
Vec<GroupResult> with per-group aggregates
    ↓
Convert to Arrow RecordBatch
    ↓
FFI export: FFI_ArrowArray + FFI_ArrowSchema
```

## Current Status & Limitations

### Implemented & Working
- Global aggregations (SUM, COUNT, MIN, MAX)
- GROUP BY aggregations (functionally correct)
- Window function: ROW_NUMBER
- Window function: RANK/DENSE_RANK peer group detection
- GPU availability detection and info reporting
- FFI interface for C/C++ integration

### Functional But Not Fully Optimized
- Global aggregations (slower than CPU due to atomic contention)
  - Solution: Two-pass reduction (implemented)
- Atomic float operations via bitcast (not hardware-native)

### Not Yet Integrated
- GPU hash join with DataFusion's join operators
- GPU-aware WHERE clause filtering
- GPU-aware multi-column aggregations
- GPU memory persistence between operations
- Shared memory optimizations for aggregations

## Performance Characteristics

**GROUP BY (10M rows, 100 groups):**
- CPU: 90ms
- GPU: 298ms
- Status: Functionally correct, overhead from data transfer

**Optimization Opportunities:**
1. Keep data on GPU between operations (reduce PCIe transfers)
2. Use shared memory for workgroup reductions
3. Optimize memory coalescing patterns
4. Predicate pushdown to GPU (WHERE clauses)
5. Multi-pass reduction for global aggregations

## Code Statistics

```
Total executor code: 3,371 lines

Key modules:
- executor.rs:         560 lines (query dispatch, FFI)
- wgsl_shader.rs:      527 lines (all compute shaders)
- wgpu_engine.rs:      316 lines (GPU engine facade)
- gpu_buffers.rs:      268 lines (memory management)
- gpu_pipeline.rs:     256 lines (pipeline builder)
- aggregations.rs:     249 lines (aggregation ops)
- window.rs:           240 lines (window functions)
- joins.rs:            203 lines (join operations)
- gpu_dispatch.rs:     134 lines (workgroup dispatch)
- gpu_types.rs:         92 lines (result types)
```

## Key Design Decisions

1. **WGPU over native APIs**
   - Pro: Single codebase, cross-platform, no toolkit dependencies
   - Con: Slight abstraction overhead, limited to compute shaders (no graphics)

2. **Bitcast for float atomics**
   - Pro: Portable, works on all platforms
   - Con: Performance impact, precision concerns

3. **Two-pass reduction**
   - Pro: Avoids atomic contention bottleneck
   - Con: Additional GPU pass (mitigated by bandwidth utilization)

4. **FFI for C compatibility**
   - Pro: Language interoperability, Arrow C Data Interface
   - Con: Unsafe code, manual memory management needed

5. **Platform-specific shader compilation at runtime**
   - Pro: True portability, automatic backend selection
   - Con: First-use shader compilation overhead (minimal, cached)

## Extension Points

1. **New Operations**
   - Add shader to `wgsl_shader.rs`
   - Create new module (e.g., `sorting.rs`) with operations struct
   - Add to `WgpuEngine` facade

2. **New Data Types**
   - Add to `gpu_types.rs` with `#[repr(C)]` + Pod trait
   - Update buffer creation utilities
   - Update shaders for type-specific operations

3. **Custom Backends**
   - WGPU supports custom native backends
   - Could add native Metal or CUDA without changing shader layer
   - Requires implementing WGPU `wgpu-hal` backend

