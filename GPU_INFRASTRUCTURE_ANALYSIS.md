# Albatross Data GPU Infrastructure Analysis

## Executive Summary

The Albatross Data project uses **WGPU v23** as its GPU abstraction layer with **WGSL (WebGPU Shading Language)** for compute shaders. Currently, GPU execution is **disabled by default** in the CPU path due to incomplete feature support. The system is designed to run on Linux/NVIDIA with Vulkan backend, but the current WGSL shaders are **cross-platform and not NVIDIA-specific**.

### Current GPU Status
- **Shader Format**: WGSL (WebAssembly Shaders Language) - cross-platform
- **Backend**: WGPU with Vulkan support on Linux
- **Status**: GPU code present but **disabled** (see `lib.rs` line 78: `if false && gpu_available`)
- **CUDA Support**: None - would require significant rewriting

---

## 1. Shader Architecture

### Shader Files Location
All shaders are **embedded as Rust string constants** in:
- `/Users/luke.shulman/Projects/albatross-data/data-embed/executor/src/wgsl_shader.rs` (496 lines)

### Shader Types and Formats

All shaders are written in **WGSL** (WebGPU Shading Language):

| Shader | Purpose | Lines | Format |
|--------|---------|-------|--------|
| `GLOBAL_AGG_PASS1_SHADER` | Local reduction per workgroup | 62 | WGSL |
| `GLOBAL_AGG_PASS2_SHADER` | Final global aggregation reduction | 103 | WGSL |
| `GROUP_BY_AGG_SHADER` | GROUP BY aggregation | 66 | WGSL |
| `HASH_JOIN_BUILD_SHADER` | Hash table construction for joins | 49 | WGSL |
| `HASH_JOIN_PROBE_SHADER` | Hash table probing with aggregation | 94 | WGSL |
| `WINDOW_ROW_NUMBER_SHADER` | ROW_NUMBER window function | 11 | WGSL |
| `WINDOW_RANK_FUNCTIONS_SHADER` | RANK/DENSE_RANK peer group detection | 22 | WGSL |
| `WINDOW_CUMULATIVE_AGG_SHADER` | Cumulative aggregation (incomplete) | 21 | WGSL |
| `SUM_SHADER` | Simple floating-point sum | 30 | WGSL |

### Shader Compilation
- **No pre-compilation**: Shaders are compiled at runtime by WGPU
- **Build System**: Pure Rust - no external shader compilation tools (no glslc, shaderc, etc.)
- **Location**: `wgsl_shader.rs` contains raw shader source strings
- **Embedding**: Shaders embedded as `&'static str` constants

---

## 2. Vulkan Implementation Analysis

### GPU Initialization Code

**File**: `/Users/luke.shulman/Projects/albatross-data/data-embed/executor/src/wgpu_engine.rs`

#### Backend Selection Pattern
```rust
// Line 43-46 in wgpu_engine.rs
let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
    backends: wgpu::Backends::all(),  // Accepts ANY backend (Vulkan, Metal, DX12)
    ..Default::default()
});
```

#### Adapter Selection
```rust
// Line 79: Default (allows fallback)
let adapter = instance.request_adapter(&wgpu::RequestAdapterOptions::default()).await?;

// Line 221-225: High-performance preference
let adapter = instance.request_adapter(&wgpu::RequestAdapterOptions {
    power_preference: wgpu::PowerPreference::HighPerformance,
    compatible_surface: None,
    force_fallback_adapter: false,  // Will NOT use CPU fallback
}).await.context("Failed to find suitable GPU adapter")?;
```

### Why Vulkan Might Fail on Linux

**Critical Issue**: The code uses `wgpu::Backends::all()` which initializes **all available backends simultaneously**. On Linux with NVIDIA:

1. **Multiple Backend Competition**: Both Vulkan and GL/NVIDIA proprietary paths compete
2. **Vulkan Loader Chain**: Requires:
   - Vulkan ICD loader (`libvulkan.so.1`)
   - NVIDIA Vulkan ICD (`libnvidia-glvk.so`)
   - Vulkan validation layers (optional but common)
   - Correct environment setup (`VK_DRIVER_FILES`, `VK_ICD_FILENAMES`)

3. **Device Limits**: Shaders use 256-thread workgroups, which may exceed NVIDIA GPU capabilities
   ```wgsl
   @compute @workgroup_size(256)
   ```

4. **No Explicit Device Selection**: The code lets WGPU choose; on multi-GPU systems, it might pick the wrong one

### GPU Information Detection

**File**: Same as above, `wgpu_engine.rs`, lines 73-90

```rust
pub async fn get_gpu_info() -> Option<GpuInfo> {
    let instance = wgpu::Instance::new(...);
    let adapter = instance.request_adapter(...).await?;
    let info = adapter.get_info();
    
    // Returns: name, backend (e.g., "Vulkan"), device_type, driver info
}
```

This provides valuable diagnostics for identifying backend issues.

---

## 3. CUDA and NVIDIA-Specific Code

### Current State
- **No CUDA code exists** in the codebase
- **No NVIDIA-specific optimizations** 
- **No cuDF/RAPIDS integration**

### Search Results
```
grep -r "cuda\|CUDA" /executor --include="*.rs" --include="*.toml"
  → No results
```

### What Would Be Needed for CUDA

To replace WGSL with CUDA:

1. **Shader Rewriting**: WGSL → CUDA Kernels (10-15 hours)
   - Atomic operations in WGSL use `atomicAdd`, CUDA uses `atomicAdd` (similar, but different semantics)
   - Workgroup barriers → `__syncthreads()`
   - Shared memory (`var<workgroup>`) → `__shared__`
   - Storage buffers → Device pointers

2. **Build System**: No shader compilation currently needed
   - Would need `nvcc` integration in build script
   - Or use `cuda-sys` crate for runtime compilation

3. **FFI Modifications**: 
   - Replace WGPU calls with CUDA API calls
   - ~2-3 files to modify (`wgpu_engine.rs`, `aggregations.rs`, `gpu_pipeline.rs`)

4. **Device Detection**:
   - Replace WGPU device management with CUDA runtime API
   - Would need `cuda-sys` or `cudarc` crate

**Estimated Effort**: 40-60 hours of engineering work (medium-high complexity)

---

## 4. Build System and Shader Compilation

### Cargo Configuration

**File**: `/Users/luke.shulman/Projects/albatross-data/data-embed/executor/Cargo.toml`

```toml
[package]
name = "executor"
version = "0.1.0"

[lib]
crate-type = ["cdylib", "rlib"]  # C-compatible dynamic library + Rust library

[dependencies]
wgpu = "23"
bytemuck = { version = "1.14", features = ["derive"] }
pollster = "0.3"  # Async runtime blocker
```

### Build Process

1. **No Custom Build Script**: No `build.rs` file
2. **Runtime Shader Compilation**: Shaders compiled when pipelines created
3. **No Shader Optimization**: Direct WGSL → GPU driver compilation
4. **Embedded Shaders**: Zero external dependencies for shaders

### Compilation Flow
```
Rust Source Code
    ↓
Cargo Compilation
    ↓
WGSL Shader Strings Embedded in Binary
    ↓
Runtime: WGPU + Driver Compiles WGSL
    ↓
GPU Code Execution
```

---

## 5. GPU Abstraction Layer

### Architecture

Three-layer abstraction in `executor/src/`:

**Layer 1: GPU Types** (`gpu_types.rs`)
- `AggregateResult`: SUM, COUNT, MIN, MAX results
- `GroupResult`: Per-group aggregation results
- All C-compatible with `#[repr(C)]`

**Layer 2: GPU Buffers** (`gpu_buffers.rs`)
- `create_storage_buffer()`: Input data
- `create_output_buffer()`: Output data
- `create_staging_buffer()`: GPU→CPU transfer
- `read_buffer_vec()`: Async GPU read

**Layer 3: GPU Operations** (4 modules)
- `aggregations.rs`: Global and GROUP BY aggregations
- `joins.rs`: Hash join operations
- `window.rs`: Window function operations
- `gpu_dispatch.rs`: Workgroup dispatch utilities

**Layer 4: High-level API** (`wgpu_engine.rs`)
- `WgpuEngine`: Main facade
- `is_gpu_available()`: Lightweight adapter check
- `get_gpu_info()`: Detailed GPU info

### Module Dependencies
```
wgpu_engine.rs (GPU initialization facade)
    ↓
aggregations.rs, joins.rs, window.rs (Operation implementations)
    ↓
gpu_buffers.rs (Buffer management)
gpu_types.rs (Data structures)
gpu_dispatch.rs (Workgroup math)
gpu_pipeline.rs (Pipeline builder)
    ↓
wgsl_shader.rs (Embedded shader code)
    ↓
wgpu v23 (WGPU library)
```

---

## 6. GPU Execution Path (Disabled)

### Why It's Disabled

**File**: `/Users/luke.shulman/Projects/albatross-data/data-embed/executor/src/lib.rs`, lines 72-82

```rust
pub extern "C" fn execute_query_to_arrow(
    query: *const c_char,
    ...
) -> i32 {
    let gpu_available = get_runtime().block_on(async {
        is_gpu_available().await
    });

    // TODO: Re-enable GPU once execute_to_arrow_gpu supports:
    // - WHERE clauses
    // - GROUP BY
    // - COUNT, MIN, MAX, AVG (not just SUM)
    // - Multiple aggregations in one query
    if false && gpu_available {  // ← DISABLED HERE
        execute_query_gpu(query, data_path, array_out, schema_out)
    } else {
        execute_query_cpu(query, data_path, array_out, schema_out)
    }
}
```

### Missing Features
1. WHERE clause filtering on GPU
2. GROUP BY implementation
3. COUNT, MIN, MAX, AVG aggregations (only SUM partially works)
4. Multiple aggregations in single query
5. ORDER BY on GPU
6. LIMIT on GPU

---

## 7. Vulkan-Specific Issues

### Identified Problems

**1. Workgroup Size Mismatch**
- WGSL specifies: `@workgroup_size(256)`
- NVIDIA GPUs: Max 1024 threads/block
- Other GPUs: May have different limits (128-512)
- **Fix**: Use device-reported limits

**2. 2D Dispatch Indexing Bug**
```wgsl
let idx = global_id.y * 65535u * 256u + global_id.x;
```
- Assumes 256-thread workgroups
- Hard-coded for specific hardware
- Won't work if limit exceeded

**3. No Vulkan Validation**
- Code doesn't use Vulkan validation layers
- No error checking for Vulkan device losses
- No recovery from GPU memory pressure

**4. Backend Preference Not Explicit**
```rust
backends: wgpu::Backends::all()  // Ambiguous on Linux
```
- Should be: `wgpu::Backends::VULKAN` (explicit)
- Or: `wgpu::Backends::GL` (fallback)

### Vulkan Driver Setup

To make Vulkan work on Linux with NVIDIA:

```bash
# 1. Install Vulkan SDK
sudo apt-get install vulkan-tools vulkan-headers libvulkan-dev

# 2. Verify driver support
vulkaninfo | grep "Driver Version"

# 3. Check NVIDIA ICD loader
ls -la /etc/vulkan/icd.d/
  → nvidia_icd.json

# 4. Test adapter detection (requires debug build)
export VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
```

---

## 8. Shader Capabilities Analysis

### What Shaders Can Do (Currently Implemented)

1. **Aggregations**: SUM, COUNT, MIN, MAX (verified)
2. **GROUP BY**: Per-group aggregations (untested)
3. **Joins**: Hash join build/probe (untested)
4. **Window Functions**: ROW_NUMBER, RANK detection (incomplete)
5. **Atomic Operations**: Float-safe CAS loops

### Atomics Approach

WGSL doesn't support `atomic<f32>`, so shaders use **compare-and-swap loops**:
```wgsl
loop {
    let old_bits = atomicLoad(&result.sum);
    let old_val = bitcast<f32>(old_bits);
    let new_val = old_val + val;
    let new_bits = bitcast<u32>(new_val);
    let exchanged = atomicCompareExchangeWeak(&result.sum, old_bits, new_bits);
    if (exchanged.exchanged) { break; }
}
```

This is **inefficient** but portable. CUDA has `atomicAdd` for floats (more efficient).

---

## 9. Effort to Migrate to CUDA

### Reusability Assessment

| Component | Reusable? | Effort |
|-----------|-----------|--------|
| Shader algorithms | **No** | Need rewrite to CUDA |
| Buffer management | **Partial** | Replace with CUDA malloc/memcpy |
| Type definitions | **Yes** | Keep C-compatible types |
| Pipeline builders | **No** | Rewrite for CUDA |
| Dispatch logic | **Partial** | Adapt to CUDA grid/block model |
| FFI layer | **Yes** | Keep as-is |
| CPU path | **Yes** | Keep as-is |

### Effort Breakdown

| Task | Estimated Hours | Difficulty |
|------|-----------------|------------|
| Port 9 WGSL shaders to CUDA | 15-20 | Medium |
| Rewrite GPU engine for CUDA API | 10-15 | Medium |
| Implement GPU buffer management | 8-10 | Easy |
| Device detection & error handling | 5-8 | Easy |
| Testing & validation | 10-15 | Medium |
| **Total** | **48-68 hours** | **Medium-High** |

### Most Difficult Parts
1. Atomics: WGSL CAS loops → CUDA atomics (careful semantics)
2. Workgroup reduction: Careful barrier placement
3. 2D dispatch calculations: Different hardware limits

### Easiest Parts
1. Type definitions (no changes needed)
2. Data marshaling (Arrow FFI stays same)
3. CPU path stays untouched

---

## 10. Key Files Summary

| File | Lines | Purpose | Status |
|------|-------|---------|--------|
| `wgsl_shader.rs` | 496 | All shader code (embedded strings) | Complete |
| `wgpu_engine.rs` | 317 | GPU initialization & facade | Functional |
| `aggregations.rs` | 249 | Aggregation operations | Partial |
| `gpu_buffers.rs` | ~150 | Buffer management | Complete |
| `gpu_pipeline.rs` | 257 | Pipeline builder utilities | Complete |
| `gpu_dispatch.rs` | 135 | Workgroup dispatch math | Complete |
| `gpu_types.rs` | 93 | Result data structures | Complete |
| `executor.rs` | ~400 | Query execution (CPU fallback) | Complete |
| `lib.rs` | 359 | FFI exports (GPU disabled) | GPU disabled |
| `main.rs` | CLI tool | CLI wrapper | Functional |

**Total GPU-related code**: ~2,100 lines of Rust

---

## 11. Recommendations

### For Linux Vulkan Issues

1. **Immediate**: Add Vulkan backend selection
   ```rust
   backends: wgpu::Backends::VULKAN  // Explicit, not ::all()
   ```

2. **Short-term**: Enable Vulkan validation layers in debug builds
   ```rust
   #[cfg(debug_assertions)]
   let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
       backends: wgpu::Backends::VULKAN,
       flags: wgpu::InstanceFlags::GPU_BASED_VALIDATION,
   });
   ```

3. **Diagnostic**: Add GPU info logging to startup
   ```rust
   let gpu_info = get_gpu_info().await;
   eprintln!("GPU: {} (Backend: {})", gpu_info.name, gpu_info.backend);
   ```

### For CUDA Migration

1. **Phase 1**: Port shaders only (15-20 hrs)
   - Keep WGPU for CPU fallback testing
   - Create CUDA kernel files alongside WGSL

2. **Phase 2**: Implement GPU engine abstraction (10-15 hrs)
   - `CudaEngine` struct mirrors `WgpuEngine`
   - Device detection via CUDA runtime API

3. **Phase 3**: Integration (10-15 hrs)
   - Feature flag: `cargo build --features cuda`
   - Conditional compilation for device selection

### For Reusing WGSL Shaders with CUDA

**Not recommended**:
- WGSL is not convertible to CUDA
- Different memory models, synchronization, atomics
- Manual translation needed

**Better approach**:
- Keep algorithm specs, reimplement in CUDA
- Use same data structures (already C-compatible)
- Share test cases between implementations

---

## Conclusion

The Albatross Data GPU layer is:
- **Architecture**: Well-designed abstraction (4 layers)
- **Shaders**: Complete for basic aggregations, but disabled
- **Vulkan**: Configured but may need debugging on Linux
- **CUDA**: Would require significant rewrite, ~50-70 hours
- **Reusability**: Low for shaders, high for infrastructure

The project is currently **CPU-only** due to incomplete GPU feature support, not due to Vulkan issues.
