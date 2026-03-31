//! GPU-accelerated aggregation operations
//!
//! This module implements global and GROUP BY aggregations using GPU compute shaders.

use anyhow::Result;

use crate::gpu_buffers::{
    create_output_buffer, create_staging_buffer, create_storage_buffer,
    create_storage_buffer_single, read_buffer_single, read_buffer_vec,
};
use crate::gpu_dispatch::{calculate_workgroup_dims, DEFAULT_WORKGROUP_SIZE};
use crate::gpu_pipeline::{BufferAccess, PipelineBuilder};
use crate::gpu_types::{AggregateResult, GroupResult, WorkgroupPartial};
use crate::wgsl_shader::{
    GLOBAL_AGG_PASS1_SHADER, GLOBAL_AGG_PASS2_SHADER, GROUP_BY_AGG_PASS1_SHADER,
    GROUP_BY_AGG_PASS2_SHADER,
};

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
    /// Uses a two-pass reduction algorithm to minimize atomic contention:
    /// - Pass 1: Each workgroup reduces its portion locally using shared memory
    /// - Pass 2: Merge workgroup partial results into final per-group results
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

        let group_keys_buffer = create_storage_buffer(
            self.device,
            Some("Group Keys Buffer"),
            group_keys,
            wgpu::BufferUsages::empty(),
        );

        // Allocate buffer for workgroup partials
        // Each workgroup can output up to 256 unique groups (worst case)
        let max_groups_per_workgroup = 256;
        let max_partials = total_workgroups * max_groups_per_workgroup;

        let init_partials: Vec<WorkgroupPartial> = vec![
            WorkgroupPartial {
                group_id: 0,
                sum: 0.0,
                count: 0,
                min: f32::MAX,
                max: f32::MIN,
            };
            max_partials
        ];

        let workgroup_partials_buffer = create_storage_buffer(
            self.device,
            Some("Workgroup Partials Buffer"),
            &init_partials,
            wgpu::BufferUsages::COPY_SRC,
        );

        // Buffer to track number of groups per workgroup
        let num_groups_buffer = create_output_buffer::<u32>(
            self.device,
            Some("Num Groups Buffer"),
            total_workgroups,
            wgpu::BufferUsages::empty(),
        );

        let (pass1_pipeline, pass1_bind_group, _) =
            PipelineBuilder::new(self.device, GROUP_BY_AGG_PASS1_SHADER)
                .with_label("GROUP BY Pass 1")
                .add_buffer(BufferAccess::ReadOnly)  // values
                .add_buffer(BufferAccess::ReadOnly)  // group_keys
                .add_buffer(BufferAccess::ReadWrite) // workgroup_partials
                .add_buffer(BufferAccess::ReadWrite) // num_groups_buffer
                .build(&[
                    &values_buffer,
                    &group_keys_buffer,
                    &workgroup_partials_buffer,
                    &num_groups_buffer,
                ]);

        // === PASS 2: Merge workgroup partials ===

        // Initialize final results buffer
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

        let (pass2_pipeline, pass2_bind_group, _) =
            PipelineBuilder::new(self.device, GROUP_BY_AGG_PASS2_SHADER)
                .with_label("GROUP BY Pass 2")
                .add_buffer(BufferAccess::ReadOnly)  // workgroup_partials
                .add_buffer(BufferAccess::ReadOnly)  // num_groups_buffer
                .add_buffer(BufferAccess::ReadWrite) // results
                .build(&[
                    &workgroup_partials_buffer,
                    &num_groups_buffer,
                    &results_buffer,
                ]);

        let staging_buffer =
            create_staging_buffer::<GroupResult>(self.device, Some("Staging Buffer"), num_groups);

        // === Execute both passes ===

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Two-Pass GROUP BY Encoder"),
            });

        // Pass 1: Workgroup-local reduction
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("GROUP BY Pass 1"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&pass1_pipeline);
            pass.set_bind_group(0, &pass1_bind_group, &[]);
            pass.dispatch_workgroups(wg_x, wg_y, 1);
        }

        // Pass 2: Merge partial results
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("GROUP BY Pass 2"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&pass2_pipeline);
            pass.set_bind_group(0, &pass2_bind_group, &[]);

            // Dispatch enough workgroups to process all partials
            let pass2_workgroups = (max_partials as u32)
                .div_ceil(DEFAULT_WORKGROUP_SIZE)
                .max(1);
            pass.dispatch_workgroups(pass2_workgroups, 1, 1);
        }

        // Copy result to staging buffer
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
