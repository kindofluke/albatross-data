//! GPU-accelerated window function operations
//!
//! This module implements window functions (ROW_NUMBER, RANK, cumulative aggregations)
//! using GPU compute shaders.

use anyhow::Result;

use crate::gpu_buffers::{
    create_output_buffer, create_staging_buffer, create_storage_buffer, read_buffer_vec,
};
use crate::gpu_dispatch::dispatch_1d_default;
use crate::gpu_pipeline::{BufferAccess, PipelineBuilder};
use crate::wgsl_shader::{
    WINDOW_CUMULATIVE_AGG_SHADER, WINDOW_RANK_FUNCTIONS_SHADER, WINDOW_ROW_NUMBER_SHADER,
};

/// Window operations executor
///
/// Holds references to GPU device and queue for executing window function operations.
pub struct WindowOps<'a> {
    pub(crate) device: &'a wgpu::Device,
    pub(crate) queue: &'a wgpu::Queue,
}

impl<'a> WindowOps<'a> {
    /// Create a new window operations executor
    pub fn new(device: &'a wgpu::Device, queue: &'a wgpu::Queue) -> Self {
        Self { device, queue }
    }

    /// Execute ROW_NUMBER window function
    ///
    /// Assigns sequential row numbers starting from 1. This is a trivially parallel
    /// operation where each thread simply writes its thread index + 1.
    ///
    /// # Arguments
    /// * `num_rows` - Number of rows to number
    ///
    /// # Returns
    /// Vector of row numbers [1, 2, 3, ..., num_rows]
    ///
    /// # Example
    /// ```ignore
    /// let row_numbers = window_ops.execute_window_row_number(1000).await?;
    /// assert_eq!(row_numbers[0], 1);
    /// assert_eq!(row_numbers[999], 1000);
    /// ```
    pub async fn execute_window_row_number(&self, num_rows: usize) -> Result<Vec<u32>> {
        let results_buffer = create_output_buffer::<u32>(
            self.device,
            Some("Row Number Results Buffer"),
            num_rows,
            wgpu::BufferUsages::COPY_SRC,
        );

        let staging_buffer = create_staging_buffer::<u32>(self.device, Some("Staging Buffer"), num_rows);

        let (pipeline, bind_group, _) = PipelineBuilder::new(self.device, WINDOW_ROW_NUMBER_SHADER)
            .with_label("ROW_NUMBER")
            .add_buffer(BufferAccess::ReadWrite)
            .build(&[&results_buffer]);

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("ROW_NUMBER Encoder"),
            });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("ROW_NUMBER Pass"),
                timestamp_writes: None,
            });
            compute_pass.set_pipeline(&pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);
            dispatch_1d_default(&mut compute_pass, num_rows as u32);
        }

        encoder.copy_buffer_to_buffer(
            &results_buffer,
            0,
            &staging_buffer,
            0,
            (std::mem::size_of::<u32>() * num_rows) as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        read_buffer_vec::<u32>(self.device, &staging_buffer).await
    }

    /// Execute RANK/DENSE_RANK peer group detection
    ///
    /// For pre-sorted data, detects where peer groups start by comparing adjacent keys.
    /// Output indicates whether each row starts a new peer group (1) or continues
    /// the previous group (0).
    ///
    /// This is used as a building block for implementing RANK and DENSE_RANK functions.
    ///
    /// # Arguments
    /// * `sorted_keys` - Pre-sorted partition keys
    ///
    /// # Returns
    /// Vector where 1 indicates peer group start, 0 indicates continuation
    ///
    /// # Example
    /// ```ignore
    /// // Input keys: [1, 1, 2, 2, 2, 3]
    /// // Output:     [1, 0, 1, 0, 0, 1]  (starts of new groups)
    /// let group_starts = window_ops.execute_window_rank_detection(&sorted_keys).await?;
    /// ```
    pub async fn execute_window_rank_detection(&self, sorted_keys: &[i32]) -> Result<Vec<u32>> {
        let num_rows = sorted_keys.len();

        let keys_buffer = create_storage_buffer(
            self.device,
            Some("Sorted Keys Buffer"),
            sorted_keys,
            wgpu::BufferUsages::empty(),
        );

        let group_starts_buffer = create_output_buffer::<u32>(
            self.device,
            Some("Group Starts Buffer"),
            num_rows,
            wgpu::BufferUsages::COPY_SRC,
        );

        let staging_buffer = create_staging_buffer::<u32>(self.device, Some("Staging Buffer"), num_rows);

        let (pipeline, bind_group, _) = PipelineBuilder::new(self.device, WINDOW_RANK_FUNCTIONS_SHADER)
            .with_label("RANK Detection")
            .add_buffer(BufferAccess::ReadOnly)  // keys
            .add_buffer(BufferAccess::ReadWrite) // group_starts
            .build(&[&keys_buffer, &group_starts_buffer]);

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("RANK Detection Encoder"),
            });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("RANK Detection Pass"),
                timestamp_writes: None,
            });
            compute_pass.set_pipeline(&pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);
            dispatch_1d_default(&mut compute_pass, num_rows as u32);
        }

        encoder.copy_buffer_to_buffer(
            &group_starts_buffer,
            0,
            &staging_buffer,
            0,
            (std::mem::size_of::<u32>() * num_rows) as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        read_buffer_vec::<u32>(self.device, &staging_buffer).await
    }

    /// Execute cumulative aggregation window function
    ///
    /// Computes cumulative SUM or COUNT over input values. Currently a placeholder
    /// implementation that needs a proper multi-pass prefix scan algorithm.
    ///
    /// # Status
    /// ⚠️ This is a placeholder implementation. A proper parallel prefix scan
    /// (e.g., Blelloch scan) needs to be implemented for correct results.
    ///
    /// # Arguments
    /// * `in_values` - Input values for cumulative aggregation
    ///
    /// # Returns
    /// Vector of cumulative aggregates
    ///
    /// # Example
    /// ```ignore
    /// // Input:  [1.0, 2.0, 3.0, 4.0]
    /// // Output: [1.0, 3.0, 6.0, 10.0] (cumulative sum)
    /// let cumulative = window_ops.execute_window_cumulative_agg(&values).await?;
    /// ```
    pub async fn execute_window_cumulative_agg(&self, in_values: &[f32]) -> Result<Vec<f32>> {
        let num_rows = in_values.len();

        let in_buffer = create_storage_buffer(
            self.device,
            Some("Cumulative Agg Input Buffer"),
            in_values,
            wgpu::BufferUsages::empty(),
        );

        let out_buffer = create_output_buffer::<f32>(
            self.device,
            Some("Cumulative Agg Output Buffer"),
            num_rows,
            wgpu::BufferUsages::COPY_SRC,
        );

        let staging_buffer = create_staging_buffer::<f32>(self.device, Some("Staging Buffer"), num_rows);

        let (pipeline, bind_group, _) = PipelineBuilder::new(self.device, WINDOW_CUMULATIVE_AGG_SHADER)
            .with_label("Cumulative Agg")
            .add_buffer(BufferAccess::ReadOnly)  // input
            .add_buffer(BufferAccess::ReadWrite) // output
            .build(&[&in_buffer, &out_buffer]);

        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("Cumulative Agg Encoder"),
            });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Cumulative Agg Pass"),
                timestamp_writes: None,
            });
            compute_pass.set_pipeline(&pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);
            dispatch_1d_default(&mut compute_pass, num_rows as u32);
        }

        encoder.copy_buffer_to_buffer(
            &out_buffer,
            0,
            &staging_buffer,
            0,
            std::mem::size_of_val(in_values) as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        read_buffer_vec::<f32>(self.device, &staging_buffer).await
    }
}
