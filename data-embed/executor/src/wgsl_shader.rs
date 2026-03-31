// WGSL compute shaders for GPU-accelerated aggregations

/// Two-pass global aggregation shader - Pass 1: Local reduction per workgroup
/// Each workgroup of 256 threads reduces to a single result
pub const GLOBAL_AGG_PASS1_SHADER: &str = r#"
struct AggregateResult {
    sum: f32,
    count: u32,
    min: f32,
    max: f32,
}

@group(0) @binding(0) var<storage, read> values: array<f32>;
@group(0) @binding(1) var<storage, read_write> workgroup_results: array<AggregateResult>;

var<workgroup> local_sum: array<f32, 256>;
var<workgroup> local_min: array<f32, 256>;
var<workgroup> local_max: array<f32, 256>;
var<workgroup> local_count: array<u32, 256>;

@compute @workgroup_size(256)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) workgroup_id: vec3<u32>
) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    let local_idx = local_id.x;
    
    // Initialize local memory
    local_sum[local_idx] = 0.0;
    local_min[local_idx] = 3.40282347e+38;  // f32::MAX
    local_max[local_idx] = -3.40282347e+38; // f32::MIN
    local_count[local_idx] = 0u;
    
    // Load and accumulate
    if (idx < arrayLength(&values)) {
        let val = values[idx];
        local_sum[local_idx] = val;
        local_min[local_idx] = val;
        local_max[local_idx] = val;
        local_count[local_idx] = 1u;
    }
    
    workgroupBarrier();
    
    // Tree reduction within workgroup
    for (var stride = 128u; stride > 0u; stride = stride / 2u) {
        if (local_idx < stride) {
            local_sum[local_idx] += local_sum[local_idx + stride];
            local_count[local_idx] += local_count[local_idx + stride];
            local_min[local_idx] = min(local_min[local_idx], local_min[local_idx + stride]);
            local_max[local_idx] = max(local_max[local_idx], local_max[local_idx + stride]);
        }
        workgroupBarrier();
    }
    
    // First thread writes workgroup result
    if (local_idx == 0u) {
        let wg_idx = workgroup_id.y * 65535u + workgroup_id.x;
        workgroup_results[wg_idx].sum = local_sum[0];
        workgroup_results[wg_idx].count = local_count[0];
        workgroup_results[wg_idx].min = local_min[0];
        workgroup_results[wg_idx].max = local_max[0];
    }
}
"#;

/// Hash join shader - Build phase: Create hash table from build side keys
/// Uses FNV-1a hash function and quadratic probing for better performance
pub const HASH_JOIN_BUILD_SHADER: &str = r#"
struct HashEntry {
    key: atomic<i32>,
    exists: atomic<u32>,
}

@group(0) @binding(0) var<storage, read> build_keys: array<i32>;
@group(0) @binding(1) var<storage, read_write> hash_table: array<HashEntry>;

// FNV-1a hash function - better distribution than simple modulo
fn hash(key: i32, table_size: u32) -> u32 {
    // FNV-1a 32-bit constants
    let FNV_PRIME: u32 = 16777619u;
    let FNV_OFFSET: u32 = 2166136261u;

    var hash_val = FNV_OFFSET;

    // Hash each byte of the key
    var k = bitcast<u32>(key);
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ k) * FNV_PRIME;

    return hash_val % table_size;
}

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&build_keys)) {
        return;
    }

    let key = build_keys[idx];
    let table_size = arrayLength(&hash_table);
    let initial_slot = hash(key, table_size);

    // Quadratic probing to find empty slot
    // Reduces clustering compared to linear probing
    for (var probe = 0u; probe < table_size; probe = probe + 1u) {
        // Quadratic probing: h(k, i) = (h(k) + i + i²) mod table_size
        let slot = (initial_slot + probe + probe * probe) % table_size;

        let current_exists = atomicLoad(&hash_table[slot].exists);
        if (current_exists == 0u) {
            // Try to claim this slot
            let old = atomicCompareExchangeWeak(&hash_table[slot].exists, 0u, 1u);
            if (old.exchanged) {
                // We claimed it, write the key
                atomicStore(&hash_table[slot].key, key);
                return;
            }
        }

        // Check if this slot already has our key
        let stored_key = atomicLoad(&hash_table[slot].key);
        if (stored_key == key) {
            return;
        }
    }
}
"#;

/// Hash join shader - Probe phase: Probe hash table and aggregate matches
/// Uses FNV-1a hash function and quadratic probing for better performance
pub const HASH_JOIN_PROBE_SHADER: &str = r#"
struct HashEntry {
    key: atomic<i32>,
    exists: atomic<u32>,
}

struct AggregateResult {
    sum: atomic<u32>,
    count: atomic<u32>,
    min: atomic<u32>,
    max: atomic<u32>,
}

@group(0) @binding(0) var<storage, read> probe_keys: array<i32>;
@group(0) @binding(1) var<storage, read> probe_values: array<f32>;
@group(0) @binding(2) var<storage, read> hash_table: array<HashEntry>;
@group(0) @binding(3) var<storage, read_write> result: AggregateResult;

// FNV-1a hash function - same as build phase for consistency
fn hash(key: i32, table_size: u32) -> u32 {
    // FNV-1a 32-bit constants
    let FNV_PRIME: u32 = 16777619u;
    let FNV_OFFSET: u32 = 2166136261u;

    var hash_val = FNV_OFFSET;

    // Hash each byte of the key
    var k = bitcast<u32>(key);
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ (k & 0xFFu)) * FNV_PRIME;
    k = k >> 8u;
    hash_val = (hash_val ^ k) * FNV_PRIME;

    return hash_val % table_size;
}

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&probe_keys)) {
        return;
    }

    let key = probe_keys[idx];
    let val = probe_values[idx];
    let table_size = arrayLength(&hash_table);
    let initial_slot = hash(key, table_size);

    // Quadratic probing to find matching key
    for (var probe = 0u; probe < table_size; probe = probe + 1u) {
        // Quadratic probing: h(k, i) = (h(k) + i + i²) mod table_size
        let slot = (initial_slot + probe + probe * probe) % table_size;

        let exists = atomicLoad(&hash_table[slot].exists);
        if (exists == 0u) {
            // Empty slot means key not found
            return;
        }

        let stored_key = atomicLoad(&hash_table[slot].key);
        if (stored_key == key) {
            // Match found! Aggregate the value
            atomicAdd(&result.count, 1u);

            // Sum
            loop {
                let old_bits = atomicLoad(&result.sum);
                let old_val = bitcast<f32>(old_bits);
                let new_val = old_val + val;
                let new_bits = bitcast<u32>(new_val);
                let exchanged = atomicCompareExchangeWeak(&result.sum, old_bits, new_bits);
                if (exchanged.exchanged) {
                    break;
                }
            }

            // Min
            loop {
                let old_bits = atomicLoad(&result.min);
                let old_val = bitcast<f32>(old_bits);
                if (val >= old_val) {
                    break;
                }
                let new_bits = bitcast<u32>(val);
                let exchanged = atomicCompareExchangeWeak(&result.min, old_bits, new_bits);
                if (exchanged.exchanged) {
                    break;
                }
            }

            // Max
            loop {
                let old_bits = atomicLoad(&result.max);
                let old_val = bitcast<f32>(old_bits);
                if (val <= old_val) {
                    break;
                }
                let new_bits = bitcast<u32>(val);
                let exchanged = atomicCompareExchangeWeak(&result.max, old_bits, new_bits);
                if (exchanged.exchanged) {
                    break;
                }
            }

            return;
        }
    }
}
"#;



/// Two-pass global aggregation shader - Pass 2: Final reduction
/// Reduces workgroup results to single final result
pub const GLOBAL_AGG_PASS2_SHADER: &str = r#"
struct AggregateResult {
    sum: f32,
    count: u32,
    min: f32,
    max: f32,
}

struct AtomicResult {

    sum: atomic<u32>,
    count: atomic<u32>,
    min: atomic<u32>,
    max: atomic<u32>,
}

@group(0) @binding(0) var<storage, read> workgroup_results: array<AggregateResult>;
@group(0) @binding(1) var<storage, read_write> final_result: AtomicResult;

var<workgroup> local_sum: array<f32, 256>;
var<workgroup> local_min: array<f32, 256>;
var<workgroup> local_max: array<f32, 256>;
var<workgroup> local_count: array<u32, 256>;

@compute @workgroup_size(256)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>
) {
    let idx = global_id.x;
    let local_idx = local_id.x;
    
    // Load workgroup results
    if (idx < arrayLength(&workgroup_results)) {
        local_sum[local_idx] = workgroup_results[idx].sum;
        local_count[local_idx] = workgroup_results[idx].count;
        local_min[local_idx] = workgroup_results[idx].min;
        local_max[local_idx] = workgroup_results[idx].max;
    } else {
        local_sum[local_idx] = 0.0;
        local_count[local_idx] = 0u;
        local_min[local_idx] = 3.40282347e+38;
        local_max[local_idx] = -3.40282347e+38;
    }
    
    workgroupBarrier();
    
    // Tree reduction within workgroup
    for (var stride = 128u; stride > 0u; stride = stride / 2u) {
        if (local_idx < stride) {
            local_sum[local_idx] += local_sum[local_idx + stride];
            local_count[local_idx] += local_count[local_idx + stride];
            local_min[local_idx] = min(local_min[local_idx], local_min[local_idx + stride]);
            local_max[local_idx] = max(local_max[local_idx], local_max[local_idx + stride]);
        }
        workgroupBarrier();
    }
    
    // First thread of each workgroup atomically updates final result
    if (local_idx == 0u) {
        atomicAdd(&final_result.count, local_count[0]);
        
        // Sum with atomic CAS
        loop {
            let old_bits = atomicLoad(&final_result.sum);
            let old_val = bitcast<f32>(old_bits);
            let new_val = old_val + local_sum[0];
            let new_bits = bitcast<u32>(new_val);
            let exchanged = atomicCompareExchangeWeak(&final_result.sum, old_bits, new_bits);
            if (exchanged.exchanged) {
                break;
            }
        }
        
        // Min with atomic CAS
        loop {
            let old_bits = atomicLoad(&final_result.min);
            let old_val = bitcast<f32>(old_bits);
            if (local_min[0] >= old_val) {
                break;
            }
            let new_bits = bitcast<u32>(local_min[0]);
            let exchanged = atomicCompareExchangeWeak(&final_result.min, old_bits, new_bits);
            if (exchanged.exchanged) {
                break;
            }
        }
        
        // Max with atomic CAS
        loop {
            let old_bits = atomicLoad(&final_result.max);
            let old_val = bitcast<f32>(old_bits);
            if (local_max[0] <= old_val) {
                break;
            }
            let new_bits = bitcast<u32>(local_max[0]);
            let exchanged = atomicCompareExchangeWeak(&final_result.max, old_bits, new_bits);
            if (exchanged.exchanged) {
                break;
            }
        }
    }
}
"#;

/// GROUP BY aggregation shader
/// Computes SUM, COUNT, MIN, MAX for each group
/// Two-pass GROUP BY aggregation shader - Pass 1: Workgroup-local reduction
/// Each workgroup processes its portion and outputs partial results per group
pub const GROUP_BY_AGG_PASS1_SHADER: &str = r#"
struct GroupResult {
    sum: f32,
    count: u32,
    min: f32,
    max: f32,
}

struct WorkgroupPartial {
    group_id: u32,
    sum: f32,
    count: u32,
    min: f32,
    max: f32,
}

@group(0) @binding(0) var<storage, read> values: array<f32>;
@group(0) @binding(1) var<storage, read> group_keys: array<u32>;
@group(0) @binding(2) var<storage, read_write> workgroup_partials: array<WorkgroupPartial>;
@group(0) @binding(3) var<storage, read_write> num_groups_buffer: array<atomic<u32>>;

// Shared memory for workgroup-local reduction
// We'll accumulate results for each group locally first
var<workgroup> local_group_sums: array<f32, 256>;
var<workgroup> local_group_counts: array<u32, 256>;
var<workgroup> local_group_mins: array<f32, 256>;
var<workgroup> local_group_maxs: array<f32, 256>;
var<workgroup> local_group_ids: array<u32, 256>;
var<workgroup> unique_groups: array<u32, 256>;
var<workgroup> group_count: atomic<u32>;

@compute @workgroup_size(256)
fn main(
    @builtin(global_invocation_id) global_id: vec3<u32>,
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) workgroup_id: vec3<u32>,
    @builtin(num_workgroups) num_workgroups: vec3<u32>
) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    let local_idx = local_id.x;

    // Initialize local storage
    local_group_sums[local_idx] = 0.0;
    local_group_counts[local_idx] = 0u;
    local_group_mins[local_idx] = 3.40282347e+38;  // f32::MAX
    local_group_maxs[local_idx] = -3.40282347e+38; // f32::MIN
    local_group_ids[local_idx] = 0xFFFFFFFFu;      // Invalid marker

    if (local_idx == 0u) {
        atomicStore(&group_count, 0u);
    }

    workgroupBarrier();

    // Load value and group for this thread
    var my_group: u32 = 0xFFFFFFFFu;
    var my_val: f32 = 0.0;
    var has_value: bool = false;

    if (idx < arrayLength(&values)) {
        my_val = values[idx];
        my_group = group_keys[idx];
        has_value = true;

        // Store in local arrays
        local_group_ids[local_idx] = my_group;
        local_group_sums[local_idx] = my_val;
        local_group_counts[local_idx] = 1u;
        local_group_mins[local_idx] = my_val;
        local_group_maxs[local_idx] = my_val;
    }

    workgroupBarrier();

    // Parallel reduction within workgroup for matching groups
    // Each thread looks for other threads with the same group and combines
    for (var stride = 1u; stride < 256u; stride = stride * 2u) {
        if (has_value && (local_idx % (stride * 2u)) == 0u) {
            let other_idx = local_idx + stride;
            if (other_idx < 256u && local_group_ids[other_idx] == my_group) {
                local_group_sums[local_idx] += local_group_sums[other_idx];
                local_group_counts[local_idx] += local_group_counts[other_idx];
                local_group_mins[local_idx] = min(local_group_mins[local_idx], local_group_mins[other_idx]);
                local_group_maxs[local_idx] = max(local_group_maxs[local_idx], local_group_maxs[other_idx]);

                // Mark the other slot as consumed
                local_group_ids[other_idx] = 0xFFFFFFFFu;
            }
        }
        workgroupBarrier();
    }

    // Identify unique groups and write partial results
    if (has_value && local_group_ids[local_idx] != 0xFFFFFFFFu) {
        let unique_idx = atomicAdd(&group_count, 1u);
        unique_groups[unique_idx] = local_idx;
    }

    workgroupBarrier();

    // Write out workgroup partials
    let num_unique = atomicLoad(&group_count);
    let wg_idx = workgroup_id.y * num_workgroups.x + workgroup_id.x;
    let max_groups_per_wg = 256u;  // Max unique groups per workgroup

    if (local_idx < num_unique) {
        let result_idx = local_idx;
        let offset = wg_idx * max_groups_per_wg + result_idx;

        if (offset < arrayLength(&workgroup_partials)) {
            let source_idx = unique_groups[local_idx];
            workgroup_partials[offset].group_id = local_group_ids[source_idx];
            workgroup_partials[offset].sum = local_group_sums[source_idx];
            workgroup_partials[offset].count = local_group_counts[source_idx];
            workgroup_partials[offset].min = local_group_mins[source_idx];
            workgroup_partials[offset].max = local_group_maxs[source_idx];
        }
    }

    // Store number of unique groups for this workgroup
    if (local_idx == 0u) {
        atomicStore(&num_groups_buffer[wg_idx], num_unique);
    }
}
"#;

/// Two-pass GROUP BY aggregation shader - Pass 2: Merge workgroup partials
/// Combines partial results from all workgroups into final per-group results
pub const GROUP_BY_AGG_PASS2_SHADER: &str = r#"
struct WorkgroupPartial {
    group_id: u32,
    sum: f32,
    count: u32,
    min: f32,
    max: f32,
}

struct GroupResult {
    sum: atomic<u32>,
    count: atomic<u32>,
    min: atomic<u32>,
    max: atomic<u32>,
}

@group(0) @binding(0) var<storage, read> workgroup_partials: array<WorkgroupPartial>;
@group(0) @binding(1) var<storage, read> num_groups_buffer: array<atomic<u32>>;
@group(0) @binding(2) var<storage, read_write> results: array<GroupResult>;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.x;
    if (idx >= arrayLength(&workgroup_partials)) {
        return;
    }

    let partial = workgroup_partials[idx];

    // Skip invalid entries
    if (partial.count == 0u) {
        return;
    }

    let group_id = partial.group_id;

    // Atomic count
    atomicAdd(&results[group_id].count, partial.count);

    // Sum using compare-and-swap
    loop {
        let old_bits = atomicLoad(&results[group_id].sum);
        let old_val = bitcast<f32>(old_bits);
        let new_val = old_val + partial.sum;
        let new_bits = bitcast<u32>(new_val);
        let exchanged = atomicCompareExchangeWeak(&results[group_id].sum, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }

    // Min using compare-and-swap
    loop {
        let old_bits = atomicLoad(&results[group_id].min);
        let old_val = bitcast<f32>(old_bits);
        if (partial.min >= old_val) {
            break;
        }
        let new_bits = bitcast<u32>(partial.min);
        let exchanged = atomicCompareExchangeWeak(&results[group_id].min, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }

    // Max using compare-and-swap
    loop {
        let old_bits = atomicLoad(&results[group_id].max);
        let old_val = bitcast<f32>(old_bits);
        if (partial.max <= old_val) {
            break;
        }
        let new_bits = bitcast<u32>(partial.max);
        let exchanged = atomicCompareExchangeWeak(&results[group_id].max, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }
}
"#;

// Legacy single-pass GROUP BY shader kept for backward compatibility
pub const GROUP_BY_AGG_SHADER: &str = GROUP_BY_AGG_PASS1_SHADER;

/// Window function: ROW_NUMBER
///
/// Assigns a unique, sequential integer to each row.
/// This shader assumes the data has already been sorted by the `ORDER BY` clause.
/// It's a simple pass-through that writes the global invocation index + 1.
/// Prerequisite: The data must be sorted before this shader is run.
pub const WINDOW_ROW_NUMBER_SHADER: &str = r#"
@group(0) @binding(0) var<storage, read_write> out_row_numbers: array<u32>;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&out_row_numbers)) {
        return;
    }
    out_row_numbers[idx] = idx + 1u;
}
"#;

/// Window functions: RANK and DENSE_RANK
///
/// Computes the rank of each row within its partition. This is a multi-pass algorithm
/// that requires host-side orchestration.
/// Prerequisite: The data must be sorted by the key upon which ranking is based.
///
/// **Pass 1: Peer Group Detection**
/// A shader identifies the start of each peer group (rows with the same key).
/// It outputs an array of flags (1 for start, 0 otherwise). The shader below is an
/// example of this pass.
///
/// **Pass 2: DENSE_RANK Calculation**
/// Run a parallel prefix sum (cumulative sum) on the `group_starts` flags from Pass 1.
/// The resulting array is the `DENSE_RANK`.
///
/// **Pass 3: RANK Calculation**
/// The `RANK` is the `ROW_NUMBER` at the start of each peer group. A propagation
/// pass is needed to fill in the rank for other rows in the same peer group.
pub const WINDOW_RANK_FUNCTIONS_SHADER: &str = r#"
// This shader implements Pass 1: Peer Group Detection.
@group(0) @binding(0) var<storage, read> sorted_keys: array<i32>;
@group(0) @binding(1) var<storage, read_write> group_starts: array<u32>;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&sorted_keys)) {
        return;
    }
    
    var is_start = 0u;
    if (idx == 0u) {
        is_start = 1u;
    } else {
        if (sorted_keys[idx] != sorted_keys[idx - 1u]) {
            is_start = 1u;
        }
    }
    group_starts[idx] = is_start;
}
"#;

/// Window functions: SUM() OVER (ORDER BY ...) and COUNT() OVER (ORDER BY ...)
///
/// Computes a cumulative sum or count over a sorted dataset.
/// Prerequisite: The data must be sorted according to the `ORDER BY` clause.
///
/// This is achieved using a parallel prefix scan (cumulative sum) algorithm.
/// A full, high-performance implementation for arbitrary data sizes is complex
/// and typically requires multiple passes orchestrated by the host:
///
/// 1.  **Local Scan**: Each workgroup computes a prefix scan of its local chunk
///     of the data and writes out the block's total sum to a temporary buffer.
/// 2.  **Block Scan**: A prefix scan is run on the buffer of block sums from Pass 1.
/// 3.  **Finalize**: The results from the block scan are added back to the
///     local scan results to get the final global cumulative sum.
///
/// For `COUNT()`, the input would be an array of 1s.
pub const WINDOW_CUMULATIVE_AGG_SHADER: &str = r#"
// This is a conceptual placeholder. A full implementation requires multiple shaders
// for the different passes as described above. The core of each pass would be a
// parallel scan algorithm, like the Blelloch or Hillis-Steele scan.
//
// Example of a single-workgroup inclusive scan (for Pass 1):

@group(0) @binding(0) var<storage, read> in_values: array<f32>;
@group(0) @binding(1) var<storage, read_write> out_cumulative_values: array<f32>;

var<workgroup> shared_data: array<f32, 256>;

@compute @workgroup_size(256)
fn main(
    @builtin(local_invocation_id) local_id: vec3<u32>,
    @builtin(workgroup_id) workgroup_id: vec3<u32>
) {
    // Implementation of a single-workgroup prefix scan would go here.
    // This is non-trivial to show in a small, correct example.
    // The host would need to manage multiple dispatches for large datasets.
}
"#;

pub const SUM_SHADER: &str = r#"
@group(0) @binding(0)
var<storage, read> input: array<f32>;

@group(0) @binding(1)
var<storage, read_write> output: atomic<u32>;

@compute
@workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    let index = global_id.x;
    if (index >= arrayLength(&input)) {
        return;
    }

    let value = input[index];
    let value_bits = bitcast<u32>(value);

    // Atomic floating-point addition using compare-and-swap
    loop {
        let old_bits = atomicLoad(&output);
        let old_value = bitcast<f32>(old_bits);
        let new_value = old_value + value;
        let new_bits = bitcast<u32>(new_value);
        let exchanged = atomicCompareExchangeWeak(&output, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }
}
"#;
