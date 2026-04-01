//! Main GPU execution engine facade
//!
//! This module provides the `WgpuEngine` struct which acts as a facade for all GPU operations.
//! It initializes the GPU device and delegates to specialized operation modules.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Float64Array, Float32Array, Int32Array, Int64Array, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;

// Re-export types from submodules for backward compatibility
pub use crate::gpu_types::{AggregateResult, GroupResult};

use crate::aggregations::AggregationOps;
use crate::joins::JoinOps;
use crate::window::WindowOps;
use crate::wgsl_shader;
use crate::gpu_dispatch::{calculate_workgroup_dims, DEFAULT_WORKGROUP_SIZE};
use crate::gpu_pipeline::{BufferAccess, PipelineBuilder};
use crate::gpu_buffers::{create_storage_buffer, create_output_buffer, create_storage_buffer_single, create_staging_buffer, read_buffer_single};

/// Information about the GPU device
#[derive(Debug, Clone)]
pub struct GpuInfo {
    /// Name of the GPU adapter
    pub name: String,
    /// GPU backend type (Vulkan, Metal, DX12, etc.)
    pub backend: String,
    /// Device type (DiscreteGpu, IntegratedGpu, VirtualGpu, Cpu, Other)
    pub device_type: String,
    /// Driver name
    pub driver: String,
    /// Driver info/version
    pub driver_info: String,
    /// Whether the GPU is available and operational
    pub available: bool,
}

/// Check if a GPU is available for computation
///
/// This is a lightweight check that attempts to find a suitable GPU adapter
/// without creating a full device.
///
/// # Returns
/// True if a GPU adapter is available, false otherwise
pub async fn is_gpu_available() -> bool {
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
        backends: wgpu::Backends::all(),
        ..Default::default()
    });

    instance
        .request_adapter(&wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            compatible_surface: None,
            force_fallback_adapter: false,
        })
        .await
        .is_some()
}

/// Get detailed information about available GPU
///
/// Queries the system for GPU adapter information including name, backend type,
/// device type, and driver information. This confirms that the GPU hardware is
/// present AND that the necessary software stack (Vulkan, Metal, etc.) is working.
///
/// # Returns
/// GpuInfo struct with details about the GPU, or None if no GPU is available
///
/// # Device Types
/// - DiscreteGpu: Dedicated GPU card (e.g., NVIDIA, AMD discrete cards)
/// - IntegratedGpu: Integrated GPU (e.g., Intel integrated graphics)
/// - VirtualGpu: Virtual GPU (e.g., in a VM)
/// - Cpu: Software renderer (should not appear with force_fallback_adapter: false)
/// - Other: Unknown device type
pub async fn get_gpu_info() -> Option<GpuInfo> {
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
        backends: wgpu::Backends::all(),
        ..Default::default()
    });

    let adapter = instance.request_adapter(&wgpu::RequestAdapterOptions::default()).await?;
    let info = adapter.get_info();

    Some(GpuInfo {
        name: info.name,
        backend: format!("{:?}", info.backend),
        device_type: format!("{:?}", info.device_type),
        driver: info.driver,
        driver_info: info.driver_info,
        available: true,
    })
}

/// Runs a SUM aggregation on the GPU using two-pass reduction.
///
/// Supports Int32, Int64, UInt32, UInt64, Float32, and Float64 arrays.
/// Uses a two-pass approach to minimize atomic contention:
/// - Pass 1: Each workgroup reduces locally (no contention)
/// - Pass 2: Merge workgroup results (minimal contention)
pub async fn run_sum_aggregation(data: ArrayRef) -> Result<f64> {
    // 1. Convert input data to f32 (GPU shader expects f32)
    let values_f32: Vec<f32> = match data.data_type() {
        DataType::Float64 => {
            let arr = data.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.values().iter().map(|&v| v as f32).collect()
        }
        DataType::Float32 => {
            let arr = data.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.values().to_vec()
        }
        DataType::Int32 => {
            let arr = data.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.values().iter().map(|&v| v as f32).collect()
        }
        DataType::Int64 => {
            let arr = data.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.values().iter().map(|&v| v as f32).collect()
        }
        DataType::UInt32 => {
            let arr = data.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.values().iter().map(|&v| v as f32).collect()
        }
        DataType::UInt64 => {
            let arr = data.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.values().iter().map(|&v| v as f32).collect()
        }
        dt => {
            return Err(anyhow::anyhow!(
                "GPU execution does not support data type: {:?}. Supported types: Int32, Int64, UInt32, UInt64, Float32, Float64",
                dt
            ));
        }
    };

    // Handle empty input - SUM of empty set is 0
    if values_f32.is_empty() {
        return Ok(0.0);
    }

    // 2. Set up GPU device and queue
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::default());
    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
        .context("Failed to find GPU adapter")?;
    let (device, queue) = adapter
        .request_device(&wgpu::DeviceDescriptor::default(), None)
        .await?;

    // 3. Calculate workgroup dimensions for pass 1
    let (wg_x, wg_y, _) = calculate_workgroup_dims(values_f32.len() as u32, DEFAULT_WORKGROUP_SIZE);
    let total_workgroups = (wg_x * wg_y) as usize;

    // === PASS 1: Local reduction per workgroup ===

    let values_buffer = create_storage_buffer(
        &device,
        Some("Values Buffer"),
        &values_f32,
        wgpu::BufferUsages::empty(),
    );

    let workgroup_results_buffer = create_output_buffer::<AggregateResult>(
        &device,
        Some("Workgroup Results Buffer"),
        total_workgroups,
        wgpu::BufferUsages::COPY_SRC,
    );

    let (pass1_pipeline, pass1_bind_group, _) =
        PipelineBuilder::new(&device, wgsl_shader::GLOBAL_AGG_PASS1_SHADER)
            .with_label("Sum Pass 1")
            .add_buffer(BufferAccess::ReadOnly)  // values
            .add_buffer(BufferAccess::ReadWrite) // workgroup_results
            .build(&[&values_buffer, &workgroup_results_buffer]);

    // === PASS 2: Final reduction ===

    let init_result = AggregateResult {
        sum: 0_f32.to_bits(),
        count: 0,
        min: f32::MAX.to_bits(),
        max: f32::MIN.to_bits(),
    };

    let final_result_buffer = create_storage_buffer_single(
        &device,
        Some("Final Result Buffer"),
        &init_result,
        wgpu::BufferUsages::COPY_SRC,
    );

    let (pass2_pipeline, pass2_bind_group, _) =
        PipelineBuilder::new(&device, wgsl_shader::GLOBAL_AGG_PASS2_SHADER)
            .with_label("Sum Pass 2")
            .add_buffer(BufferAccess::ReadOnly)  // workgroup_results
            .add_buffer(BufferAccess::ReadWrite) // final_result
            .build(&[&workgroup_results_buffer, &final_result_buffer]);

    let staging_buffer = create_staging_buffer::<AggregateResult>(&device, Some("Staging Buffer"), 1);

    // === Execute both passes ===

    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
        label: Some("Two-Pass Sum Encoder"),
    });

    // Pass 1: Local reduction
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("Sum Pass 1"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&pass1_pipeline);
        pass.set_bind_group(0, &pass1_bind_group, &[]);
        pass.dispatch_workgroups(wg_x, wg_y, 1);
    }

    // Pass 2: Final reduction
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("Sum Pass 2"),
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

    queue.submit(Some(encoder.finish()));

    // 4. Read result from GPU and convert to f64
    let result = read_buffer_single::<AggregateResult>(&device, &staging_buffer).await?;
    Ok(result.sum_f32() as f64)
}

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
