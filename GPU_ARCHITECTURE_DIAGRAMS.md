# GPU Architecture Visual Diagrams

## 1. High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SQL Query Input                                 │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│          DataFusion Query Processing                                │
│  ├─ SQL Parser                                                      │
│  ├─ Logical Optimizer                                               │
│  ├─ Physical Planner                                                │
│  └─ Execution (Data Collection to Arrow RecordBatch)               │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│          Executor Query Router (executor.rs)                        │
│  Detects: JOIN? / AGGREGATION? / TABLE_SCAN?                       │
└─────────┬──────────────┬──────────────┬──────────────────────────────┘
          │              │              │
    ┌─────▼──┐      ┌────▼────┐   ┌────▼────┐
    │ JOIN    │      │  AGGS   │   │ SCAN    │
    │ (GPU)   │      │ (GPU)   │   │ (CPU)   │
    └─────┬──┘      └────┬────┘   └────┬────┘
          │              │             │
          └──────────────┼─────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  WGPU Execution Engine        │
         │  ┌─────────────────────────┐  │
         │  │ Device / Queue          │  │
         │  │ (GPU Resource Manager)  │  │
         │  └─────────────────────────┘  │
         │                               │
         │  ┌─────────────────────────┐  │
         │  │ Pipeline Builder        │  │
         │  │ (Shader compilation)    │  │
         │  └─────────────────────────┘  │
         │                               │
         │  ┌─────────────────────────┐  │
         │  │ Operation Modules       │  │
         │  │ ├─ Aggregations         │  │
         │  │ ├─ Joins                │  │
         │  │ └─ Window Functions     │  │
         │  └─────────────────────────┘  │
         └───────────────┬────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  Platform Backend Selection   │
         │  ┌──────┐ ┌────────┐ ┌──────┐ │
         │  │Metal │ │ Vulkan │ │ DX12 │ │
         │  │(macOS)│ │ (Linux)│ │(Win) │ │
         │  └──────┘ └────────┘ └──────┘ │
         └───────────────┬────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  GPU Hardware                 │
         │  ├─ Apple Silicon (M1/M2/etc) │
         │  ├─ NVIDIA (via Vulkan)       │
         │  └─ AMD (via Vulkan)          │
         └───────────────┬────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  Results (Arrow Format)       │
         │  ├─ FFI_ArrowArray            │
         │  └─ FFI_ArrowSchema           │
         └───────────────────────────────┘
```

## 2. WGPU Cross-Platform Abstraction

```
┌────────────────────────────────────┐
│   Application Code (Rust)          │
│   ├─ wgpu::Instance                │
│   ├─ wgpu::Device                  │
│   ├─ wgpu::Queue                   │
│   ├─ wgpu::ComputePipeline         │
│   └─ wgpu::BindGroup               │
└────────────────┬───────────────────┘
                 │ WGPU API Calls
                 ▼
     ┌───────────────────────────┐
     │  WGPU Runtime             │
     │  Backend Selection Logic  │
     └───┬───────────────────┬───┘
         │                   │
    ┌────▼────┐        ┌────▼────┐       ┌────────┐
    │ macOS?  │        │ Linux?  │       │Windows?│
    │         │        │         │       │        │
    │  YES    │        │  YES    │       │  YES   │
    │   ↓     │        │   ↓     │       │   ↓    │
    │ Metal  │        │ Vulkan  │       │ DX12   │
    └────┬────┘        └────┬────┘       └────┬───┘
         │                   │                │
    ┌────▼───────────────────▼────────────────▼──┐
    │  Shader Compilation (Runtime)             │
    │                                            │
    │  WGSL Source Code                         │
    │         │                                  │
    │    ┌────▼────┐    ┌────────┐ ┌────────┐  │
    │    │ Metal   │    │ SPIR-V │ │  HLSL  │  │
    │    │Shading  │    │(Vulkan)│ │(DX12)  │  │
    │    │Language │    │        │ │        │  │
    │    └────┬────┘    └────┬───┘ └───┬────┘  │
    └─────────┼──────────────┼─────────┼────────┘
              │              │         │
    ┌─────────▼──────────────▼─────────▼──────┐
    │         GPU Driver / Runtime            │
    │  ├─ Metal Runtime (macOS)               │
    │  ├─ Vulkan Driver (Linux)               │
    │  └─ DX12 Runtime (Windows)              │
    └─────────┬──────────────────────────────┘
              │
    ┌─────────▼──────────────────────────────┐
    │         GPU Hardware                    │
    │  ├─ GPU Compute Units                   │
    │  ├─ GPU Memory                          │
    │  └─ GPU Command Processors              │
    └────────────────────────────────────────┘
```

## 3. GPU Computation Pipeline

```
┌──────────────────────────────────────────────────────────┐
│          Input: Arrow Array Data                         │
│  (Parquet file → DataFusion → RecordBatch)              │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  WgpuEngine::new()             │
    │  ├─ Request GPU adapter        │
    │  ├─ Create device              │
    │  └─ Create queue               │
    └────────────────┬───────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Operation Module              │
    │  (aggregations/joins/window)   │
    └────────────────┬───────────────┘
                     │
        ┌────────────▼────────────┐
        │                         │
        ▼                         ▼
   ┌─────────┐             ┌──────────┐
   │ Input   │             │ Create   │
   │ Buffers │             │ Output   │
   │ (GPU)   │             │ Buffers  │
   └────┬────┘             └────┬─────┘
        │                       │
        └───────────┬───────────┘
                    │
                    ▼
        ┌─────────────────────────────┐
        │  PipelineBuilder            │
        │  ├─ Load WGSL shader        │
        │  ├─ Create bind group layout│
        │  ├─ Compile compute shader  │
        │  └─ Create compute pipeline │
        └────────────┬────────────────┘
                     │
                     ▼
        ┌──────────────────────────────┐
        │  GPU Dispatch                │
        │  ├─ Calculate workgroup dims │
        │  ├─ Handle 2D dispatch fallback
        │  │  (if > 65535 workgroups) │
        │  └─ Submit command buffer   │
        └────────────┬────────────────┘
                     │
                     ▼
        ┌──────────────────────────────┐
        │  GPU Compute Execution       │
        │  ├─ Workgroup scheduling     │
        │  ├─ Thread execution         │
        │  ├─ Atomic operations        │
        │  └─ Synchronization          │
        └────────────┬────────────────┘
                     │
                     ▼
        ┌──────────────────────────────┐
        │  Copy to Staging Buffer      │
        │  (GPU → CPU-accessible RAM) │
        └────────────┬────────────────┘
                     │
                     ▼
        ┌──────────────────────────────┐
        │  Async Read Results          │
        │  └─ Map staging buffer       │
        │  └─ Convert to Vec<T>        │
        └────────────┬────────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Output: Rust Native Types     │
    │  (Vec<AggregateResult> etc)    │
    └────────────────┬───────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Convert to Arrow RecordBatch  │
    └────────────────┬───────────────┘
                     │
                     ▼
    ┌────────────────────────────────┐
    │  Export via FFI Interface      │
    │  (FFI_ArrowArray + Schema)     │
    └────────────────────────────────┘
```

## 4. Global Aggregation Two-Pass Algorithm

```
Input Array: [a0, a1, a2, ... aN]

═══════════════════════════════════════════════════════════════════

PASS 1: LOCAL REDUCTION (per workgroup)

  Workgroup 0 (256 threads)          Workgroup 1 (256 threads)
  ┌──────────────────────────┐      ┌──────────────────────────┐
  │ Elements 0-255           │      │ Elements 256-511         │
  │ ┌──────────────────────┐ │      │ ┌──────────────────────┐ │
  │ │ Load to shared mem   │ │      │ │ Load to shared mem   │ │
  │ │ Tree reduction       │ │      │ │ Tree reduction       │ │
  │ │ ▼▼▼ (sync at each)   │ │      │ │ ▼▼▼ (sync at each)   │ │
  │ │ Local: sum/min/max   │ │      │ │ Local: sum/min/max   │ │
  │ │ Write to result[0]   │ │      │ │ Write to result[1]   │ │
  │ └──────────────────────┘ │      │ └──────────────────────┘ │
  └──────────────────────────┘      └──────────────────────────┘

  Result: [Agg(0-255), Agg(256-511), Agg(512-767), ...]
          (One aggregation result per workgroup)

═══════════════════════════════════════════════════════════════════

PASS 2: FINAL REDUCTION

  All Workgroup Results
  [Agg0, Agg1, Agg2, Agg3, ...]

  Final Workgroup (256 threads)
  ┌────────────────────────────────────┐
  │ Load all workgroup results         │
  │ Tree reduction within workgroup    │
  │ ▼▼▼ (sync at each stride)          │
  │ Atomic updates to final result     │
  │ ├─ atomicAdd() for sum/count       │
  │ ├─ atomicCAS() for min/max         │
  │ └─ Write to final buffer           │
  └────────────────────────────────────┘

  Output: Single AggregateResult {sum, count, min, max}
```

## 5. GROUP BY Aggregation Single-Pass Algorithm

```
Input Arrays:
  values:    [v0, v1, v2, v3, v4, v5, v6, v7, ...]
  group_keys: [0,  1,  0,  2,  0,  1,  2,  2, ...]

═══════════════════════════════════════════════════════════════════

GPU Dispatch: 1 thread per input element
Total threads: 8 (matching input size)

Thread 0: value=v0, group_key=0
  ├─ atomicAdd(&results[0].count, 1)
  ├─ atomicCAS(&results[0].sum, ...) for sum
  ├─ atomicCAS(&results[0].min, ...) for min
  └─ atomicCAS(&results[0].max, ...) for max

Thread 1: value=v1, group_key=1
  ├─ atomicAdd(&results[1].count, 1)
  ├─ atomicCAS(&results[1].sum, ...)
  ├─ atomicCAS(&results[1].min, ...)
  └─ atomicCAS(&results[1].max, ...)

Thread 2: value=v2, group_key=0
  ├─ atomicAdd(&results[0].count, 1)  ← Contention point!
  ├─ atomicCAS(&results[0].sum, ...)  ← Retry loop
  ├─ atomicCAS(&results[0].min, ...)
  └─ atomicCAS(&results[0].max, ...)

... (all threads run in parallel)

═══════════════════════════════════════════════════════════════════

Final Results Buffer:
  results[0] = {sum: v0+v2+v4, count: 3, min: min(...), max: max(...)}
  results[1] = {sum: v1+v5,     count: 2, min: min(...), max: max(...)}
  results[2] = {sum: v3+v6+v7,  count: 3, min: min(...), max: max(...)}
```

## 6. Hash Join Algorithm

```
Build Phase:
───────────
Input: build_keys = [10, 20, 30, 15, 10]

┌─ Hash Table (size = next_pow2(5*2) = 16)
│
├─ Thread per build_key
│  ├─ key=10: hash(10)=10, probe_until_empty, insert
│  ├─ key=20: hash(20)=4,  probe_until_empty, insert
│  ├─ key=30: hash(30)=14, insert
│  ├─ key=15: hash(15)=15, insert
│  └─ key=10: hash(10)=10, already exists, skip
│
└─ Result: Hash table with entries at indices [4, 10, 14, 15]

Probe Phase:
────────────
Input: probe_keys   = [20, 30, 20, 99]
       probe_values = [2.0, 3.0, 4.0, 5.0]

┌─ Thread per probe_key
│  ├─ key=20: hash(20)=4, found! aggregate 2.0
│  ├─ key=30: hash(30)=14, found! aggregate 3.0
│  ├─ key=20: hash(20)=4, found! aggregate 4.0
│  └─ key=99: hash(99)=?, probe, not found, skip
│
└─ Result: AggregateResult {sum: 2.0+3.0+4.0=9.0, count: 3, ...}
```

## 7. Shader Compilation Pipeline

```
┌──────────────────────────┐
│  WGSL Source Code        │
│  (Platform-independent) │
│  ├─ Compute shaders      │
│  ├─ Atomic operations    │
│  ├─ Shared memory        │
│  └─ Workgroup barriers   │
└────────────┬─────────────┘
             │
             ▼
    ┌────────────────────┐
    │  WGPU Compiler     │
    │  (Runtime)         │
    └────┬───────────────┘
         │
    ┌────┴─────────────────────────────┐
    │                                  │
    ▼                                  ▼
┌────────────────┐            ┌────────────────────┐
│  Metal macOS   │            │  Vulkan Linux      │
│                │            │                    │
│ WGSL ────→     │            │ WGSL ────→         │
│  MSL           │            │  SPIR-V            │
│  (.metallib)   │            │  (.spv)            │
└───────┬────────┘            └────────┬───────────┘
        │                              │
        ▼                              ▼
    ┌────────────────┐          ┌──────────────┐
    │ Metal Runtime  │          │ Vulkan Driver│
    │ (GPU Compiler) │          │ (GPU Compiler)
    └────────┬───────┘          └──────┬───────┘
             │                         │
             └────────────┬────────────┘
                          │
                          ▼
            ┌─────────────────────────┐
            │  GPU-Specific Binary    │
            │  (Ready for execution)  │
            └─────────────────────────┘
```

## 8. Memory Layout: Atomic Float Operations

```
Standard Float (f32):
┌─────────────────────────┐
│ Sign(1) Exponent(8) Mantissa(23) │
└─────────────────────────┘

Atomic Operations need u32:
┌─────────────────────────────┐
│ Bitcast to u32              │
│ └─ Preserves bit pattern    │
│ └─ Allows atomicCAS()       │
└─────────────────────────────┘

WGSL Pattern:
┌──────────────────────────────────┐
│ let old_bits = atomicLoad(...)   │ Read u32 bits
│ let old_val = bitcast<f32>(...)  │ Interpret as f32
│ let new_val = old_val + value    │ Floating point math
│ let new_bits = bitcast<u32>(...)│ Convert back to bits
│ atomicCAS(..., old_bits, ...)    │ Atomic swap
└──────────────────────────────────┘

Advantages:
  ✓ Works on all platforms (no hardware float atomics needed)
  ✓ Portable (no platform-specific extensions)

Disadvantages:
  ✗ Performance: CAS loop vs. hardware atomic add
  ✗ Precision: Floating point math not associative
```

## 9. FFI Memory Layout

```
Rust Side (GPU Engine):              C/C++ Side (Caller)
┌──────────────────────┐            ┌──────────────────────┐
│ GPU Compute Results  │            │ Needs Arrow Data     │
│ Vec<T>               │            │                      │
└──────────┬───────────┘            └──────────┬───────────┘
           │                                    │
           ▼                                    ▼
┌──────────────────────────────────────────────────────────┐
│     Arrow RecordBatch Conversion                         │
│  (Columnar format, Arrow schema)                         │
└──────────┬───────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  FFI Conversion                                          │
│  ├─ RecordBatch → StructArray                           │
│  ├─ ArrayData extraction                                │
│  └─ Arrow C Data Interface                              │
│     └─ FFI_ArrowArray (C-compatible struct)             │
│     └─ FFI_ArrowSchema (C-compatible schema)            │
└──────────┬───────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  Memory Export                                           │
│  └─ Pointers in heap memory                             │
│  └─ Caller retains ownership responsibility             │
│  └─ Must call release_arrow_pointers() to free          │
└──────────────────────────────────────────────────────────┘
```

## 10. Query Type Detection and Routing

```
┌─────────────────────────────────┐
│  Input SQL Query                │
│  Execute with DataFusion        │
│  Get Physical Plan              │
└──────────────┬──────────────────┘
               │
               ▼
    ┌──────────────────────────┐
    │  Query Analysis          │
    │  (Pattern matching)      │
    └──────┬──────────┬────────┘
           │          │
    ┌──────▼┐  ┌─────▼──┐
    │ JOIN? │  │ GROUP  │   ┌──────────┐
    │       │  │ BY or  │   │ Table    │
    │ YES ──┼──┤ AGG?   ├───┤ Scan or  │
    │       │  │        │   │ Other    │
    │ NO    │  │ YES ───┘   └──────────┘
    └──────┬┘  └─────┬──┐
           │         │  │
           ▼         ▼  ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │execute_  │ │execute_  │ │execute_  │
    │ join_gpu │ │simple_   │ │table_    │
    │          │ │agg_gpu   │ │scan_cpu  │
    └────┬─────┘ └────┬─────┘ └────┬─────┘
         │             │            │
         └─────────────┼────────────┘
                       │
                       ▼
         ┌─────────────────────────┐
         │  GPU or CPU Execution   │
         │  (Per query path)       │
         └─────────────────────────┘
```
