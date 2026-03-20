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
pub const GROUP_BY_AGG_SHADER: &str = r#"
struct GroupResult {
    sum: atomic<u32>,
    count: atomic<u32>,
    min: atomic<u32>,
    max: atomic<u32>,
}

@group(0) @binding(0) var<storage, read> values: array<f32>;
@group(0) @binding(1) var<storage, read> group_keys: array<u32>;
@group(0) @binding(2) var<storage, read_write> results: array<GroupResult>;

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id: vec3<u32>) {
    // Support 2D dispatch for large datasets
    let idx = global_id.y * 65535u * 256u + global_id.x;
    if (idx >= arrayLength(&values)) {
        return;
    }
    
    let val = values[idx];
    let group_id = group_keys[idx];
    
    // Atomic count
    atomicAdd(&results[group_id].count, 1u);
    
    // Sum using compare-and-swap
    loop {
        let old_bits = atomicLoad(&results[group_id].sum);
        let old_val = bitcast<f32>(old_bits);
        let new_val = old_val + val;
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
        if (val >= old_val) {
            break;
        }
        let new_bits = bitcast<u32>(val);
        let exchanged = atomicCompareExchangeWeak(&results[group_id].min, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }
    
    // Max using compare-and-swap
    loop {
        let old_bits = atomicLoad(&results[group_id].max);
        let old_val = bitcast<f32>(old_bits);
        if (val <= old_val) {
            break;
        }
        let new_bits = bitcast<u32>(val);
        let exchanged = atomicCompareExchangeWeak(&results[group_id].max, old_bits, new_bits);
        if (exchanged.exchanged) {
            break;
        }
    }
}
"#;
