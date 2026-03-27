//! Main GPU execution engine facade
//!
//! This module provides the `WgpuEngine` struct which acts as a facade for all GPU operations.
//! It initializes the GPU device and delegates to specialized operation modules.

use anyhow::{Context, Result};

// Re-export types from submodules for backward compatibility
pub use crate::gpu_types::{AggregateResult, GroupResult};

use crate::aggregations::AggregationOps;
use crate::joins::JoinOps;
use crate::window::WindowOps;

/// Main GPU execution engine
///
/// Provides access to GPU-accelerated operations for data processing.
/// Acts as a facade that delegates to specialized operation modules.
///
/// # Example
/// ```ignore
/// let engine = WgpuEngine::new().await?;
/// let result = engine.execute_global_aggregation(&values).await?;
/// println!("Sum: {}, Count: {}", result.sum_f32(), result.count);
/// ```
pub struct WgpuEngine {
    device: wgpu::Device,
    queue: wgpu::Queue,
}

impl WgpuEngine {
    /// Initialize a new GPU execution engine
    ///
    /// Requests a GPU adapter with high performance preference and creates
    /// a device and command queue for compute operations.
    ///
    /// # Returns
    /// A new WgpuEngine instance ready for computation
    ///
    /// # Errors
    /// Returns an error if no suitable GPU adapter is found or device creation fails
    pub async fn new() -> Result<Self> {
        let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
            backends: wgpu::Backends::all(),
            ..Default::default()
        });

        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
            })
            .await
            .context("Failed to find suitable GPU adapter")?;

        let (device, queue) = adapter
            .request_device(
                &wgpu::DeviceDescriptor {
                    label: Some("GPU Compute Device"),
                    required_features: wgpu::Features::empty(),
                    required_limits: wgpu::Limits::default(),
                    memory_hints: Default::default(),
                },
                None,
            )
            .await
            .context("Failed to create GPU device")?;

        Ok(Self { device, queue })
    }

    // ========== Aggregation Operations ==========

    /// Execute global aggregation (SUM, COUNT, MIN, MAX) over all values
    ///
    /// Delegates to `AggregationOps::execute_global_aggregation`.
    /// See that method for detailed documentation.
    pub async fn execute_global_aggregation(&self, values: &[f32]) -> Result<AggregateResult> {
        let ops = AggregationOps::new(&self.device, &self.queue);
        ops.execute_global_aggregation(values).await
    }

    /// Execute GROUP BY aggregation (SUM, COUNT, MIN, MAX per group)
    ///
    /// Delegates to `AggregationOps::execute_group_by_aggregation`.
    /// See that method for detailed documentation.
    pub async fn execute_group_by_aggregation(
        &self,
        values: &[f32],
        group_keys: &[u32],
        num_groups: usize,
    ) -> Result<Vec<GroupResult>> {
        let ops = AggregationOps::new(&self.device, &self.queue);
        ops.execute_group_by_aggregation(values, group_keys, num_groups)
            .await
    }

    // ========== Join Operations ==========

    /// Execute hash join with aggregation
    ///
    /// Delegates to `JoinOps::execute_hash_join_aggregate`.
    /// See that method for detailed documentation.
    pub async fn execute_hash_join_aggregate(
        &self,
        build_keys: &[i32],
        probe_keys: &[i32],
        probe_values: &[f32],
    ) -> Result<AggregateResult> {
        let ops = JoinOps::new(&self.device, &self.queue);
        ops.execute_hash_join_aggregate(build_keys, probe_keys, probe_values)
            .await
    }

    // ========== Window Function Operations ==========

    /// Execute ROW_NUMBER window function
    ///
    /// Delegates to `WindowOps::execute_window_row_number`.
    /// See that method for detailed documentation.
    pub async fn execute_window_row_number(&self, num_rows: usize) -> Result<Vec<u32>> {
        let ops = WindowOps::new(&self.device, &self.queue);
        ops.execute_window_row_number(num_rows).await
    }

    /// Execute RANK/DENSE_RANK peer group detection
    ///
    /// Delegates to `WindowOps::execute_window_rank_detection`.
    /// See that method for detailed documentation.
    pub async fn execute_window_rank_detection(&self, sorted_keys: &[i32]) -> Result<Vec<u32>> {
        let ops = WindowOps::new(&self.device, &self.queue);
        ops.execute_window_rank_detection(sorted_keys).await
    }

    /// Execute cumulative aggregation window function
    ///
    /// Delegates to `WindowOps::execute_window_cumulative_agg`.
    /// See that method for detailed documentation.
    pub async fn execute_window_cumulative_agg(&self, in_values: &[f32]) -> Result<Vec<f32>> {
        let ops = WindowOps::new(&self.device, &self.queue);
        ops.execute_window_cumulative_agg(in_values).await
    }
}
