//! Main GPU execution engine facade
//!
//! This module provides the `WgpuEngine` struct which acts as a facade for all GPU operations.
//! It initializes the GPU device and delegates to specialized operation modules.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Float64Array};
use wgpu::util::DeviceExt;

// Re-export types from submodules for backward compatibility
pub use crate::gpu_types::{AggregateResult, GroupResult};

use crate::aggregations::AggregationOps;
use crate::joins::JoinOps;
use crate::window::WindowOps;
use crate::wgsl_shader;

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

/// Runs a SUM aggregation on the GPU.
///
/// This is a placeholder and currently only supports `Float64Array`.
pub async fn run_sum_aggregation(data: ArrayRef) -> Result<f64> {
    // 1. Downcast to the concrete array type (placeholder)
    let float_data = data.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        anyhow::anyhow!("GPU execution currently only supports Float64Array")
    })?;

    let input_slice: &[f64] = float_data.values();

    // 2. Set up WGPU
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::default());
    let adapter = instance.request_adapter(&wgpu::RequestAdapterOptions::default()).await.unwrap();
    let (device, queue) = adapter.request_device(&wgpu::DeviceDescriptor::default(), None).await?;

    // 3. Create buffers
    let input_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("Input Buffer"),
        contents: bytemuck::cast_slice(input_slice),
        usage: wgpu::BufferUsages::STORAGE,
    });

    let output_buf = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("Output Buffer"),
        size: std::mem::size_of::<f64>() as u64,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });

    // 4. Load and configure the shader
    let shader_module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("Sum Shader"),
        source: wgpu::ShaderSource::Wgsl(wgsl_shader::SUM_SHADER.into()),
    });

    // 5. Set up the pipeline
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("Sum Pipeline"),
        layout: None,
        module: &shader_module,
        entry_point: Some("main"),
        compilation_options: Default::default(),
        cache: None,
    });

    // 6. Create bind group to link buffers to shader
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("Sum Bind Group"),
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: input_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: output_buf.as_entire_binding(),
            },
        ],
    });

    // 7. Dispatch the compute shader
    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
    {
        let mut cpass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor::default());
        cpass.set_pipeline(&pipeline);
        cpass.set_bind_group(0, &bind_group, &[]);
        cpass.dispatch_workgroups(input_slice.len() as u32, 1, 1); // One thread per element
    }
    queue.submit(Some(encoder.finish()));

    // 8. Read back the result
    let staging_buf = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("Staging Buffer"),
        size: std::mem::size_of::<f64>() as u64,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });

    let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
    encoder.copy_buffer_to_buffer(&output_buf, 0, &staging_buf, 0, std::mem::size_of::<f64>() as u64);
    queue.submit(Some(encoder.finish()));

    // 9. Map the staging buffer and get the result
    let buffer_slice = staging_buf.slice(..);
    buffer_slice.map_async(wgpu::MapMode::Read, |_| {});
    device.poll(wgpu::Maintain::Wait);

    let data = buffer_slice.get_mapped_range();
    let result: f64 = bytemuck::from_bytes::<f64>(&data).clone();

    Ok(result)
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
