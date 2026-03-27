//! GPU-accelerated join operations
//!
//! This module implements hash join operations using GPU compute shaders.

use anyhow::Result;
use bytemuck::{Pod, Zeroable};

use crate::gpu_buffers::{
    create_staging_buffer, create_storage_buffer, create_storage_buffer_single, read_buffer_single,
};
use crate::gpu_dispatch::dispatch_1d_default;
use crate::gpu_pipeline::{BufferAccess, PipelineBuilder};
use crate::gpu_types::AggregateResult;
use crate::wgsl_shader::{HASH_JOIN_BUILD_SHADER, HASH_JOIN_PROBE_SHADER};

/// Hash table entry for hash join
///
/// Represents a single entry in the hash table with a key and existence flag.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
struct HashEntry {
    /// The key value
    key: i32,
    /// 1 if entry exists, 0 if empty
    exists: u32,
}

/// Join operations executor
///
/// Holds references to GPU device and queue for executing join operations.
pub struct JoinOps<'a> {
    pub(crate) device: &'a wgpu::Device,
    pub(crate) queue: &'a wgpu::Queue,
}

impl<'a> JoinOps<'a> {
    /// Create a new join operations executor
    pub fn new(device: &'a wgpu::Device, queue: &'a wgpu::Queue) -> Self {
        Self { device, queue }
    }

    /// Execute hash join with aggregation
    ///
    /// Builds a hash table from build keys, probes with probe keys, and aggregates
    /// matching probe values. Uses linear probing for collision resolution.
    ///
    /// # Algorithm
    /// 1. **Build Phase**: Construct hash table from build_keys using atomic operations
    ///    - Hash table size is 2x build side (50% load factor)
    ///    - Linear probing handles collisions
    /// 2. **Probe Phase**: For each probe key, lookup in hash table and aggregate matching values
    ///    - Only values with matching keys are aggregated
    ///    - Uses atomic operations for aggregate updates
    ///
    /// # Arguments
    /// * `build_keys` - Keys for the build side (hash table)
    /// * `probe_keys` - Keys for the probe side
    /// * `probe_values` - Values to aggregate for matching probe keys
    ///
    /// # Returns
    /// AggregateResult containing sum, count, min, max of matched values
    ///
    /// # Example
    /// ```ignore
    /// // Build side: [1, 2, 3]
    /// // Probe side keys: [2, 3, 3, 5] values: [10.0, 20.0, 30.0, 40.0]
    /// // Result: matches on keys 2,3,3 -> sum=60.0, count=3
    /// let result = join_ops.execute_hash_join_aggregate(&build_keys, &probe_keys, &probe_values).await?;
    /// ```
    pub async fn execute_hash_join_aggregate(
        &self,
        build_keys: &[i32],
        probe_keys: &[i32],
        probe_values: &[f32],
    ) -> Result<AggregateResult> {
        // Hash table size: 2x build side for ~50% load factor
        // Using power of 2 for efficient modulo via bitwise AND
        let table_size = (build_keys.len() * 2).next_power_of_two();

        // === PHASE 1: Build hash table ===

        let build_keys_buffer = create_storage_buffer(
            self.device,
            Some("Build Keys Buffer"),
            build_keys,
            wgpu::BufferUsages::empty(),
        );

        // Initialize hash table with empty entries
        let init_table: Vec<HashEntry> = vec![
            HashEntry {
                key: 0,
                exists: 0
            };
            table_size
        ];

        let hash_table_buffer = create_storage_buffer(
            self.device,
            Some("Hash Table Buffer"),
            &init_table,
            wgpu::BufferUsages::COPY_SRC,
        );

        let (build_pipeline, build_bind_group, _) =
            PipelineBuilder::new(self.device, HASH_JOIN_BUILD_SHADER)
                .with_label("Hash Join Build")
                .add_buffer(BufferAccess::ReadOnly)  // build_keys
                .add_buffer(BufferAccess::ReadWrite) // hash_table
                .build(&[&build_keys_buffer, &hash_table_buffer]);

        // === PHASE 2: Probe and aggregate ===

        let probe_keys_buffer = create_storage_buffer(
            self.device,
            Some("Probe Keys Buffer"),
            probe_keys,
            wgpu::BufferUsages::empty(),
        );

        let probe_values_buffer = create_storage_buffer(
            self.device,
            Some("Probe Values Buffer"),
            probe_values,
            wgpu::BufferUsages::empty(),
        );

        let init_result = AggregateResult {
            sum: 0_f32.to_bits(),
            count: 0,
            min: f32::MAX.to_bits(),
            max: f32::MIN.to_bits(),
        };

        let result_buffer = create_storage_buffer_single(
            self.device,
            Some("Result Buffer"),
            &init_result,
            wgpu::BufferUsages::COPY_SRC,
        );

        let (probe_pipeline, probe_bind_group, _) =
            PipelineBuilder::new(self.device, HASH_JOIN_PROBE_SHADER)
                .with_label("Hash Join Probe")
                .add_buffer(BufferAccess::ReadOnly)  // probe_keys
                .add_buffer(BufferAccess::ReadOnly)  // probe_values
                .add_buffer(BufferAccess::ReadOnly)  // hash_table
                .add_buffer(BufferAccess::ReadWrite) // result
                .build(&[
                    &probe_keys_buffer,
                    &probe_values_buffer,
                    &hash_table_buffer,
                    &result_buffer,
                ]);

        let staging_buffer =
            create_staging_buffer::<AggregateResult>(self.device, Some("Staging Buffer"), 1);

        // === Execute both phases ===

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Hash Join Encoder"),
            });

        // Build phase: construct hash table
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Build Pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&build_pipeline);
            pass.set_bind_group(0, &build_bind_group, &[]);
            dispatch_1d_default(&mut pass, build_keys.len() as u32);
        }

        // Probe phase: lookup and aggregate
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Probe Pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&probe_pipeline);
            pass.set_bind_group(0, &probe_bind_group, &[]);
            dispatch_1d_default(&mut pass, probe_keys.len() as u32);
        }

        // Copy result to staging buffer
        encoder.copy_buffer_to_buffer(
            &result_buffer,
            0,
            &staging_buffer,
            0,
            std::mem::size_of::<AggregateResult>() as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        // Read result from GPU
        read_buffer_single::<AggregateResult>(self.device, &staging_buffer).await
    }
}
