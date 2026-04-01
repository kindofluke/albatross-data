//! Main GPU execution engine facade
//!
//! This module provides the `WgpuEngine` struct which acts as a facade for all GPU operations.
//! It initializes the GPU device and delegates to specialized operation modules.

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, Float64Array, Float32Array, Int32Array, Int64Array, UInt32Array, UInt64Array};
use arrow::datatypes::DataType;
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
/// Supports Int32, Int64, UInt32, UInt64, Float32, and Float64 arrays.
pub async fn run_sum_aggregation(data: ArrayRef) -> Result<f64> {
    // 1. Get a slice to the input data, avoiding copies where possible
    // For Float64, we can use the Arrow buffer directly without copying
    // For other types, we need to convert to f64

    // Use enum to handle both direct slice and owned vector cases
    enum InputData<'a> {
        Borrowed(&'a [f64]),
        Owned(Vec<f64>),
    }

    let input_data = match data.data_type() {
        DataType::Float64 => {
            let arr = data.as_any().downcast_ref::<Float64Array>().unwrap();
            // Direct slice access - no copy!
            InputData::Borrowed(arr.values())
        }
        DataType::Float32 => {
            let arr = data.as_any().downcast_ref::<Float32Array>().unwrap();
            InputData::Owned(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::Int32 => {
            let arr = data.as_any().downcast_ref::<Int32Array>().unwrap();
            InputData::Owned(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::Int64 => {
            let arr = data.as_any().downcast_ref::<Int64Array>().unwrap();
            InputData::Owned(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::UInt32 => {
            let arr = data.as_any().downcast_ref::<UInt32Array>().unwrap();
            InputData::Owned(arr.values().iter().map(|&v| v as f64).collect())
        }
        DataType::UInt64 => {
            let arr = data.as_any().downcast_ref::<UInt64Array>().unwrap();
            InputData::Owned(arr.values().iter().map(|&v| v as f64).collect())
        }
        dt => {
            return Err(anyhow::anyhow!(
                "GPU execution does not support data type: {:?}. Supported types: Int32, Int64, UInt32, UInt64, Float32, Float64",
                dt
            ));
        }
    };

    let input_slice = match &input_data {
        InputData::Borrowed(slice) => *slice,
        InputData::Owned(vec) => vec.as_slice(),
    };

    // Handle empty input data - SUM of empty set is 0
    if input_slice.is_empty() {
        return Ok(0.0);
    }

    // 2. Set up WGPU
    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::default());
    let adapter = instance.request_adapter(&wgpu::RequestAdapterOptions::default()).await.unwrap();
    let (device, queue) = adapter.request_device(&wgpu::DeviceDescriptor::default(), None).await?;

    // 3. Process in chunks respecting both buffer size (128MB) and workgroup limits (65535)
    // Workgroup size is 256, max dispatch is 65535 workgroups
    // So max elements per chunk = 65535 * 256 = 16,777,088
    const MAX_WORKGROUPS: usize = 65535;
    const WORKGROUP_SIZE: usize = 256;
    const MAX_ELEMENTS_PER_CHUNK: usize = MAX_WORKGROUPS * WORKGROUP_SIZE; // ~16.7M elements
    
    let chunk_size = MAX_ELEMENTS_PER_CHUNK.min(input_slice.len());
    
    let mut total_sum = 0.0f64;
    
    for chunk_start in (0..input_slice.len()).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(input_slice.len());
        let chunk = &input_slice[chunk_start..chunk_end];
        
        // Create buffers for this chunk
        let input_buf = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Input Buffer"),
            contents: bytemuck::cast_slice(chunk),
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
        // Workgroup size is 256 (defined in shader), so dispatch ceil(n / 256) workgroups
        let workgroup_size = 256u32;
        let num_workgroups = (chunk.len() as u32 + workgroup_size - 1) / workgroup_size;
        
        let mut encoder = device.create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        {
            let mut cpass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor::default());
            cpass.set_pipeline(&pipeline);
            cpass.set_bind_group(0, &bind_group, &[]);
            cpass.dispatch_workgroups(num_workgroups, 1, 1);
        }
        queue.submit(Some(encoder.finish()));

        // 8. Read back the result for this chunk
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
        let chunk_sum: f64 = bytemuck::from_bytes::<f64>(&data).clone();
        
        total_sum += chunk_sum;
    }

    Ok(total_sum)
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
