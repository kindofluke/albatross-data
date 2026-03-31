# Shader Code Reference Guide

Quick reference for all WGSL shaders and their GPU implementation details.

## File Structure

All shaders are in: `/data-embed/executor/src/wgsl_shader.rs`

## Shader Inventory

### 1. Global Aggregation - Pass 1: Local Reduction

**Constant**: `GLOBAL_AGG_PASS1_SHADER`  
**Lines**: 5-67 (62 lines)  
**Purpose**: Reduce data per workgroup to get workgroup sum/count/min/max

**Key Features**:
- Workgroup size: 256 threads
- Tree reduction algorithm
- Workgroup barriers for synchronization
- Output: One `AggregateResult` per workgroup

**Algorithm**:
```
1. Each thread loads ONE element
2. Store in workgroup shared memory
3. Tree reduction (stride 128, 64, 32, ..., 1)
4. Thread 0 writes final result
```

**Limitations**:
- Assumes 256-thread workgroups (NVIDIA: 1024 max, OK)
- 2D dispatch indexing: `global_id.y * 65535u * 256u + global_id.x`
- Fixed workgroup memory layout

---

### 2. Global Aggregation - Pass 2: Final Reduction

**Constant**: `GLOBAL_AGG_PASS2_SHADER`  
**Lines**: 222-325 (103 lines)  
**Purpose**: Reduce all workgroup results to single final result

**Key Features**:
- Takes workgroup results as input
- Atomic CAS loops for float operations
- Final result written with atomics

**Atomic Pattern for Floats**:
```wgsl
loop {
    let old_bits = atomicLoad(&final_result.sum);
    let old_val = bitcast<f32>(old_bits);
    let new_val = old_val + local_sum[0];
    let new_bits = bitcast<u32>(new_val);
    let exchanged = atomicCompareExchangeWeak(&final_result.sum, old_bits, new_bits);
    if (exchanged.exchanged) { break; }
}
```

**Why**: WGSL has no `atomic<f32>`, so floats stored as `u32` bit patterns

---

### 3. GROUP BY Aggregation

**Constant**: `GROUP_BY_AGG_SHADER`  
**Lines**: 329-395 (66 lines)  
**Purpose**: Compute aggregations for each group independently

**Key Features**:
- Direct atomic updates to group result array
- No barriers (each thread independent)
- Linear performance (no synchronization overhead)

**Data Layout**:
```
Input:
  values[n]     - Float values to aggregate
  group_keys[n] - Which group each value belongs to

Output:
  results[num_groups] - One AggregateResult per group
```

**Thread Model**:
```
Each thread i:
  1. Load values[i] and group_keys[i]
  2. Atomically update results[group_keys[i]]
  3. No synchronization needed
```

---

### 4. Hash Join - Build Phase

**Constant**: `HASH_JOIN_BUILD_SHADER`  
**Lines**: 70-119 (49 lines)  
**Purpose**: Create hash table from build side keys

**Key Features**:
- Linear probing for collision resolution
- Atomic compare-and-swap to claim slots
- Hash function: `key % table_size`

**Hash Function**:
```wgsl
fn hash(key: i32, table_size: u32) -> u32 {
    let k = u32(key);
    return k % table_size;
}
```

**Data Layout**:
```
Input:
  build_keys[n] - Keys from build side table

Output:
  hash_table[size] - Array of (key, exists) pairs
                    - exists=0: empty, exists=1: occupied
```

**Collision Resolution**:
```wgsl
var slot = hash(key, table_size);
for (var probe = 0u; probe < table_size; probe++) {
    if (atomicLoad(&hash_table[slot].exists) == 0u) {
        // Try to claim
        if (atomicCompareExchangeWeak(...).exchanged) {
            atomicStore(&hash_table[slot].key, key);
            return;
        }
    }
    slot = (slot + 1u) % table_size;  // Linear probing
}
```

---

### 5. Hash Join - Probe Phase

**Constant**: `HASH_JOIN_PROBE_SHADER`  
**Lines**: 122-216 (94 lines)  
**Purpose**: Probe hash table and aggregate matching values

**Key Features**:
- Reads from hash table (built in previous pass)
- Atomic aggregation of matches
- Same linear probing logic as build

**Data Layout**:
```
Input:
  probe_keys[n] - Keys from probe side
  probe_values[n] - Values to aggregate
  hash_table[size] - Built hash table

Output:
  result - Single AggregateResult with aggregated matches
```

**Match Logic**:
```wgsl
var slot = hash(key, table_size);
for (var probe = 0u; probe < table_size; probe++) {
    let exists = atomicLoad(&hash_table[slot].exists);
    if (exists == 0u) { return; }  // Key not found
    
    let stored_key = atomicLoad(&hash_table[slot].key);
    if (stored_key == key) {
        // Match found! Atomically aggregate
        atomicAdd(&result.count, 1u);
        // ... CAS loops for sum/min/max
        return;
    }
    slot = (slot + 1u) % table_size;
}
```

---

### 6. Window Function: ROW_NUMBER

**Constant**: `WINDOW_ROW_NUMBER_SHADER`  
**Lines**: 403-414 (11 lines)  
**Purpose**: Assign sequential row numbers

**Key Features**:
- Simplest shader (one line per thread)
- Assumes data already sorted
- Output: Row number = global_id + 1

**Implementation**:
```wgsl
@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&out_row_numbers)) { return; }
    out_row_numbers[idx] = idx + 1u;
}
```

---

### 7. Window Function: RANK Detection

**Constant**: `WINDOW_RANK_FUNCTIONS_SHADER`  
**Lines**: 434-456 (22 lines)  
**Purpose**: Detect peer group boundaries (same key = same rank)

**Key Features**:
- First pass of rank calculation
- Requires sorted input
- Outputs: 1 if group starts, 0 otherwise

**Algorithm**:
```wgsl
if (idx == 0u) {
    is_start = 1u;  // First row always starts group
} else if (sorted_keys[idx] != sorted_keys[idx - 1u]) {
    is_start = 1u;  // Different key = new group
} else {
    is_start = 0u;  // Same key = same group
}
group_starts[idx] = is_start;
```

**Post-Processing Required**:
1. Host computes prefix sum on `group_starts` → DENSE_RANK
2. Host propagates RANK values to non-start rows

---

### 8. Window Function: Cumulative Aggregation

**Constant**: `WINDOW_CUMULATIVE_AGG_SHADER`  
**Lines**: 474-495 (21 lines)  
**Purpose**: Compute running sum/count (SUM() OVER ORDER BY)

**Status**: INCOMPLETE - placeholder only

**What's Needed**:
- Multi-pass implementation
- Pass 1: Local scan within each workgroup
- Pass 2: Scan the block sums
- Pass 3: Add back block sum to local results

**Algorithm Outline**:
```
1. Each workgroup computes local inclusive scan
2. Block sum written to temporary buffer
3. Recursive scan on block sums
4. Finalize: Add global block sum to local results
```

**Example for 8 elements with 4-thread workgroup**:
```
Input:  [1, 2, 3, 4, 5, 6, 7, 8]

Block 1 (threads 0-3, elements 0-3):
  Pass 1 local scan: [1, 3, 6, 10]
  Block sum: 10

Block 2 (threads 0-3, elements 4-7):
  Pass 1 local scan: [5, 11, 18, 26]
  Block sum: 26

Pass 2 (on block sums):
  Scan [10, 26] = [10, 36]

Pass 3 (finalize):
  Block 1: [1, 3, 6, 10]
  Block 2: [10+5, 10+11, 10+18, 10+26] = [15, 21, 28, 36]

Output: [1, 3, 6, 10, 15, 21, 28, 36]
```

---

### 9. Simple SUM Aggregation

**Constant**: `SUM_SHADER`  
**Lines**: 497-527 (30 lines)  
**Purpose**: Simple floating-point sum (used for testing)

**Key Features**:
- Single shader (no two-pass)
- Used in `run_sum_aggregation()`
- Direct atomic CAS loop

**Implementation**:
```wgsl
let value = input[index];
let value_bits = bitcast<u32>(value);

loop {
    let old_bits = atomicLoad(&output);
    let old_value = bitcast<f32>(old_bits);
    let new_value = old_value + value;
    let new_bits = bitcast<u32>(new_value);
    let exchanged = atomicCompareExchangeWeak(&output, old_bits, new_bits);
    if (exchanged.exchanged) { break; }
}
```

---

## GPU Implementation Details

### Memory Layout

**AggregateResult Structure** (16 bytes):
```rust
#[repr(C)]
pub struct AggregateResult {
    pub sum: u32,    // Float stored as u32 bits
    pub count: u32,
    pub min: u32,    // Float stored as u32 bits
    pub max: u32,    // Float stored as u32 bits
}
```

**GroupResult Structure** (16 bytes):
```rust
#[repr(C)]
pub struct GroupResult {
    pub sum: u32,    // Same as AggregateResult
    pub count: u32,
    pub min: u32,
    pub max: u32,
}
```

### Dispatch Patterns

**1D Dispatch** (most shaders):
```
element_count = 1,000,000
workgroup_size = 256
workgroups_needed = 1,000,000 / 256 = 3,906

If <= 65535:
  dispatch_workgroups(3906, 1, 1)

If > 65535:
  x = 65535
  y = (3906 + 65535 - 1) / 65535 = 1
  dispatch_workgroups(x, y, 1)
```

**2D Dispatch** (for very large datasets):
```
For 20,000,000 elements:
  workgroups_needed = 78,125
  
  x = 65535
  y = (78125 + 65535 - 1) / 65535 = 2
  
  dispatch_workgroups(65535, 2, 1)
  
  Actual coverage: 65535 * 2 * 256 = 33,554,430 elements ✓
```

### Synchronization

**Within Workgroup**:
```wgsl
workgroupBarrier()  // Waits for all 256 threads
```

**Across Workgroups**:
```wgsl
atomicCompareExchangeWeak(...)  // Hardware-level CAS
```

**No Cross-Grid Sync**: GPU has no barrier across workgroups
- Solution: Two-pass algorithm
- Pass 1: Each workgroup produces partial result
- Pass 2: Combine partial results

---

## Performance Characteristics

### Pass 1 Reduction (GLOBAL_AGG_PASS1_SHADER)

**Complexity**: O(log n) iterations with 256-thread synchronization

**Example: 10M elements**
```
Workgroups: 10,000,000 / 256 = 39,063
Iterations: log2(256) = 8
Barrier invocations per WG: 8

Total work: 39,063 workgroups * 256 threads = 10M threads
Total barriers: 39,063 * 8 = 312,504 barriers
```

### Pass 2 Reduction (GLOBAL_AGG_PASS2_SHADER)

**Complexity**: O(log m) where m = number of workgroups

**Example: 10M elements → 39,063 workgroups**
```
Workgroups for pass 2: 39,063 / 256 = 153 (rounded up)
Iterations: log2(153) ≈ 8
CAS loops: Variable (depends on contention)
```

### Atomic Contention

**CAS Loop Impact**:
```
Best case: 1 iteration per thread (no contention)
Worst case: Many iterations if threads collide

For final float aggregation:
  Loop { atomicLoad, bitcast, add, atomicCAS }
  
With 256 threads all writing to same location:
  Average wait time ∝ log(threads)
```

---

## Limitations and Bugs

### Known Issues

1. **Hard-coded Workgroup Size**
   - Uses 256, but NVIDIA supports up to 1024
   - Will fail on GPUs with max < 256

2. **2D Dispatch Index Bug**
   ```wgsl
   let idx = global_id.y * 65535u * 256u + global_id.x;
   ```
   - Assumes 256-thread workgroup
   - Should use dynamic value from push constants

3. **No Shared Memory Limit Checking**
   - Workgroup memory: 256*4 + 256*4 + 256*4 + 256*4 = 4096 bytes
   - Safe on all GPUs (96 KB minimum), but not verified

4. **Hash Function Too Simple**
   - Modulo hash: `k % table_size`
   - No quality analysis for collision rates

5. **No Progress Detection**
   - If hash table fills up, loop never exits
   - Should validate table_size >= num_keys

---

## CUDA Equivalents (For Reference)

### SUM Aggregation

**WGSL**:
```wgsl
loop {
    let old_bits = atomicLoad(&result.sum);
    let old_val = bitcast<f32>(old_bits);
    let new_val = old_val + val;
    let new_bits = bitcast<u32>(new_val);
    if (atomicCompareExchangeWeak(&result.sum, old_bits, new_bits).exchanged) {
        break;
    }
}
```

**CUDA**:
```cuda
float old_val, new_val;
unsigned int *result_bits = (unsigned int*)&result.sum;
unsigned int old_bits, new_bits;

do {
    old_bits = atomicCAS(result_bits, old_bits, new_bits);
} while (old_bits != *(unsigned int*)&old_val);
```

Or more simply:
```cuda
atomicAdd(&result.sum, val);  // CUDA has native float atomics!
```

---

## Testing Shaders

### Compile-time Check
```bash
cargo build --release
# WGSL compiler errors will appear here
```

### Runtime Check
```rust
#[tokio::main]
async fn test_shader() {
    let shader = wgsl_shader::GLOBAL_AGG_PASS1_SHADER;
    let device = create_device().await;
    
    // This will fail if shader is invalid
    let module = device.create_shader_module(
        wgpu::ShaderModuleDescriptor {
            label: Some("Test"),
            source: wgpu::ShaderSource::Wgsl(shader.into()),
        }
    );
}
```

---

## Resources

- [WGSL Specification](https://www.w3.org/TR/WGSL/)
- [WGPU Examples](https://github.com/gfx-rs/wgpu/tree/master/examples)
- [Compute Shader Best Practices](https://gpuopen.com/)

