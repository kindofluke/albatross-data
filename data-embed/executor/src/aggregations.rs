//! GPU-accelerated aggregation operations
//!
//! This module implements global and GROUP BY aggregations using GPU compute shaders.

use anyhow::Result;

use crate::gpu_buffers::{
    create_output_buffer, create_staging_buffer, create_storage_buffer,
    create_storage_buffer_single, read_buffer_single, read_buffer_vec,
};
use crate::gpu_dispatch::{calculate_workgroup_dims, dispatch_1d_default, DEFAULT_WORKGROUP_SIZE};
use crate::gpu_pipeline::{BufferAccess, PipelineBuilder};
use crate::gpu_types::{AggregateResult, GroupResult};
use crate::wgsl_shader::{GLOBAL_AGG_PASS1_SHADER, GLOBAL_AGG_PASS2_SHADER, GROUP_BY_AGG_SHADER};

/// Aggregation operations executor
///
/// Holds references to GPU device and queue for executing aggregation operations.
pub struct AggregationOps<'a> {
    pub(crate) device: &'a wgpu::Device,
    pub(crate) queue: &'a wgpu::Queue,
}

impl<'a> AggregationOps<'a> {
    /// Create a new aggregation operations executor
    pub fn new(device: &'a wgpu::Device, queue: &'a wgpu::Queue) -> Self {
        Self { device, queue }
    }

    /// Execute global aggregation (SUM, COUNT, MIN, MAX) over all values
    ///
    /// Uses a two-pass reduction algorithm:
    /// - Pass 1: Each workgroup (256 threads) reduces its portion locally
    /// - Pass 2: Final reduction of all workgroup results
    ///
    /// # Arguments
    /// * `values` - Input values to aggregate
    ///
    /// # Returns
    /// AggregateResult containing sum, count, min, and max
    ///
    /// # Example
    /// ```ignore
    /// let result = agg_ops.execute_global_aggregation(&values).await?;
    /// println!("Sum: {}, Avg: {}", result.sum_f32(), result.avg());
    /// ```
    pub async fn execute_global_aggregation(&self, values: &[f32]) -> Result<AggregateResult> {
        // Calculate workgroup dimensions for pass 1
        let (wg_x, wg_y, _) = calculate_workgroup_dims(values.len() as u32, DEFAULT_WORKGROUP_SIZE);
        let total_workgroups = (wg_x * wg_y) as usize;

        // === PASS 1: Local reduction per workgroup ===

        let values_buffer = create_storage_buffer(
            self.device,
            Some("Values Buffer"),
            values,
            wgpu::BufferUsages::empty(),
        );

        let workgroup_results_buffer = create_output_buffer::<AggregateResult>(
            self.device,
            Some("Workgroup Results Buffer"),
            total_workgroups,
            wgpu::BufferUsages::COPY_SRC,
        );

        let (pass1_pipeline, pass1_bind_group, _) = PipelineBuilder::new(self.device, GLOBAL_AGG_PASS1_SHADER)
            .with_label("Global Agg Pass 1")
            .add_buffer(BufferAccess::ReadOnly)
            .add_buffer(BufferAccess::ReadWrite)
            .build(&[&values_buffer, &workgroup_results_buffer]);

        // === PASS 2: Final reduction ===

        let init_result = AggregateResult {
            sum: 0_f32.to_bits(),
            count: 0,
            min: f32::MAX.to_bits(),
            max: f32::MIN.to_bits(),
        };

        let final_result_buffer = create_storage_buffer_single(
            self.device,
            Some("Final Result Buffer"),
            &init_result,
            wgpu::BufferUsages::COPY_SRC,
        );

        let (pass2_pipeline, pass2_bind_group, _) = PipelineBuilder::new(self.device, GLOBAL_AGG_PASS2_SHADER)
            .with_label("Global Agg Pass 2")
            .add_buffer(BufferAccess::ReadOnly)
            .add_buffer(BufferAccess::ReadWrite)
            .build(&[&workgroup_results_buffer, &final_result_buffer]);

        let staging_buffer =
            create_staging_buffer::<AggregateResult>(self.device, Some("Staging Buffer"), 1);

        // === Execute both passes ===

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Two-Pass Encoder"),
            });

        // Pass 1
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Pass 1"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&pass1_pipeline);
            pass.set_bind_group(0, &pass1_bind_group, &[]);
            pass.dispatch_workgroups(wg_x, wg_y, 1);
        }

        // Pass 2 - reduce workgroup results
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Pass 2"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&pass2_pipeline);
            pass.set_bind_group(0, &pass2_bind_group, &[]);

            let pass2_workgroups = (total_workgroups as u32)
                .div_ceil(DEFAULT_WORKGROUP_SIZE)
                .max(1);
            pass.dispatch_workgroups(pass2_workgroups, 1, 1);
        }

        // Copy result to staging buffer
        encoder.copy_buffer_to_buffer(
            &final_result_buffer,
            0,
            &staging_buffer,
            0,
            std::mem::size_of::<AggregateResult>() as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        // Read result from GPU
        read_buffer_single::<AggregateResult>(self.device, &staging_buffer).await
    }

    /// Execute GROUP BY aggregation (SUM, COUNT, MIN, MAX per group)
    ///
    /// Computes aggregates for each group using atomic operations on the GPU.
    /// Each thread processes one value and atomically updates its group's aggregate.
    ///
    /// # Arguments
    /// * `values` - Input values to aggregate
    /// * `group_keys` - Group ID for each value (must be same length as values)
    /// * `num_groups` - Total number of distinct groups
    ///
    /// # Returns
    /// Vector of GroupResult, one per group (indexed by group ID)
    ///
    /// # Example
    /// ```ignore
    /// let results = agg_ops.execute_group_by_aggregation(&values, &group_ids, 10).await?;
    /// for (group_id, result) in results.iter().enumerate() {
    ///     println!("Group {}: sum={}", group_id, result.sum_f32());
    /// }
    /// ```
    pub async fn execute_group_by_aggregation(
        &self,
        values: &[f32],
        group_keys: &[u32],
        num_groups: usize,
    ) -> Result<Vec<GroupResult>> {
        // Create input buffers
        let values_buffer = create_storage_buffer(
            self.device,
            Some("Values Buffer"),
            values,
            wgpu::BufferUsages::COPY_DST,
        );

        let group_keys_buffer = create_storage_buffer(
            self.device,
            Some("Group Keys Buffer"),
            group_keys,
            wgpu::BufferUsages::COPY_DST,
        );

        // Initialize results buffer with default values
        let init_results: Vec<GroupResult> = (0..num_groups)
            .map(|_| GroupResult {
                sum: 0_f32.to_bits(),
                count: 0,
                min: f32::MAX.to_bits(),
                max: f32::MIN.to_bits(),
            })
            .collect();

        let results_buffer = create_storage_buffer(
            self.device,
            Some("Results Buffer"),
            &init_results,
            wgpu::BufferUsages::COPY_SRC,
        );

        let staging_buffer =
            create_staging_buffer::<GroupResult>(self.device, Some("Staging Buffer"), num_groups);

        // Create compute pipeline
        let (pipeline, bind_group, _) = PipelineBuilder::new(self.device, GROUP_BY_AGG_SHADER)
            .with_label("GROUP BY Aggregation")
            .add_buffer(BufferAccess::ReadOnly)  // values
            .add_buffer(BufferAccess::ReadOnly)  // group_keys
            .add_buffer(BufferAccess::ReadWrite) // results
            .build(&[&values_buffer, &group_keys_buffer, &results_buffer]);

        // Encode and submit
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("GROUP BY Encoder"),
            });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("GROUP BY Pass"),
                timestamp_writes: None,
            });
            compute_pass.set_pipeline(&pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);

            // Dispatch with automatic 2D fallback for large datasets
            dispatch_1d_default(&mut compute_pass, values.len() as u32);
        }

        encoder.copy_buffer_to_buffer(
            &results_buffer,
            0,
            &staging_buffer,
            0,
            (std::mem::size_of::<GroupResult>() * num_groups) as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        // Read results from GPU
        read_buffer_vec::<GroupResult>(self.device, &staging_buffer).await
    }
}
