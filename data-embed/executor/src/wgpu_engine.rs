use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use wgpu::util::DeviceExt;

use crate::wgsl_shader::{GLOBAL_AGG_PASS1_SHADER, GLOBAL_AGG_PASS2_SHADER, GROUP_BY_AGG_SHADER, HASH_JOIN_BUILD_SHADER, HASH_JOIN_PROBE_SHADER};

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct AggregateResult {
    pub sum: u32,    // Stored as bits, interpret as f32
    pub count: u32,
    pub min: u32,    // Stored as bits, interpret as f32
    pub max: u32,    // Stored as bits, interpret as f32
}

impl AggregateResult {
    pub fn sum_f32(&self) -> f32 {
        f32::from_bits(self.sum)
    }
    
    pub fn min_f32(&self) -> f32 {
        f32::from_bits(self.min)
    }
    
    pub fn max_f32(&self) -> f32 {
        f32::from_bits(self.max)
    }
    
    pub fn avg(&self) -> f32 {
        if self.count > 0 {
            self.sum_f32() / (self.count as f32)
        } else {
            0.0
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct GroupResult {
    pub sum: u32,
    pub count: u32,
    pub min: u32,
    pub max: u32,
}

impl GroupResult {
    pub fn sum_f32(&self) -> f32 {
        f32::from_bits(self.sum)
    }
    
    pub fn min_f32(&self) -> f32 {
        f32::from_bits(self.min)
    }
    
    pub fn max_f32(&self) -> f32 {
        f32::from_bits(self.max)
    }
    
    pub fn avg(&self) -> f32 {
        if self.count > 0 {
            self.sum_f32() / (self.count as f32)
        } else {
            0.0
        }
    }
}

pub struct WgpuEngine {
    device: wgpu::Device,
    queue: wgpu::Queue,
}

impl WgpuEngine {
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

    pub async fn execute_global_aggregation(&self, values: &[f32]) -> Result<AggregateResult> {
        // TWO-PASS REDUCTION for global aggregation
        
        // Calculate workgroup count
        let workgroup_count = (values.len() as u32 + 255) / 256;
        let (wg_x, wg_y) = if workgroup_count <= 65535 {
            (workgroup_count, 1)
        } else {
            (65535, (workgroup_count + 65534) / 65535)
        };
        let total_workgroups = (wg_x * wg_y) as usize;
        
        // PASS 1: Local reduction per workgroup
        let values_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Values Buffer"),
            contents: bytemuck::cast_slice(values),
            usage: wgpu::BufferUsages::STORAGE,
        });
        
        // Buffer to hold per-workgroup results
        let workgroup_results_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Workgroup Results Buffer"),
            size: (std::mem::size_of::<AggregateResult>() * total_workgroups) as u64,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        
        let pass1_shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Pass 1 Shader"),
            source: wgpu::ShaderSource::Wgsl(GLOBAL_AGG_PASS1_SHADER.into()),
        });
        
        let pass1_layout = self.device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Pass 1 Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });
        
        let pass1_pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Pass 1 Pipeline Layout"),
            bind_group_layouts: &[&pass1_layout],
            push_constant_ranges: &[],
        });
        
        let pass1_pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Pass 1 Pipeline"),
            layout: Some(&pass1_pipeline_layout),
            module: &pass1_shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        
        let pass1_bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Pass 1 Bind Group"),
            layout: &pass1_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: values_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: workgroup_results_buffer.as_entire_binding(),
                },
            ],
        });
        
        // PASS 2: Final reduction
        let init_result = AggregateResult {
            sum: 0_f32.to_bits(),
            count: 0,
            min: f32::MAX.to_bits(),
            max: f32::MIN.to_bits(),
        };
        let final_result_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Final Result Buffer"),
            contents: bytemuck::bytes_of(&init_result),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });
        
        let pass2_shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Pass 2 Shader"),
            source: wgpu::ShaderSource::Wgsl(GLOBAL_AGG_PASS2_SHADER.into()),
        });
        
        let pass2_layout = self.device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Pass 2 Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });
        
        let pass2_pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Pass 2 Pipeline Layout"),
            bind_group_layouts: &[&pass2_layout],
            push_constant_ranges: &[],
        });
        
        let pass2_pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Pass 2 Pipeline"),
            layout: Some(&pass2_pipeline_layout),
            module: &pass2_shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        
        let pass2_bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Pass 2 Bind Group"),
            layout: &pass2_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: workgroup_results_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: final_result_buffer.as_entire_binding(),
                },
            ],
        });
        
        // Staging buffer for reading final result
        let staging_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Staging Buffer"),
            size: std::mem::size_of::<AggregateResult>() as u64,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        
        // Encode both passes
        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
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
        
        // Pass 2 - may need multiple workgroups if we have > 256 workgroup results
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Pass 2"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&pass2_pipeline);
            pass.set_bind_group(0, &pass2_bind_group, &[]);
            // Calculate workgroups needed for pass 2
            let pass2_workgroups = ((total_workgroups as u32 + 255) / 256).max(1);
            pass.dispatch_workgroups(pass2_workgroups, 1, 1);
        }
        
        encoder.copy_buffer_to_buffer(
            &final_result_buffer,
            0,
            &staging_buffer,
            0,
            std::mem::size_of::<AggregateResult>() as u64,
        );
        
        self.queue.submit(Some(encoder.finish()));
        
        // Read result
        let buffer_slice = staging_buffer.slice(..);
        let (sender, receiver) = futures::channel::oneshot::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            sender.send(result).unwrap();
        });
        
        self.device.poll(wgpu::Maintain::Wait);
        receiver.await.context("Failed to map buffer")??;
        
        let data = buffer_slice.get_mapped_range();
        let result: AggregateResult = *bytemuck::from_bytes(&data);
        drop(data);
        staging_buffer.unmap();
        
        Ok(result)
    }

    pub async fn execute_group_by_aggregation(
        &self,
        values: &[f32],
        group_keys: &[u32],
        num_groups: usize,
    ) -> Result<Vec<GroupResult>> {
        // Create input buffers
        let values_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Values Buffer"),
            contents: bytemuck::cast_slice(values),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        });

        let group_keys_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Group Keys Buffer"),
            contents: bytemuck::cast_slice(group_keys),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
        });

        // Initialize results buffer
        let init_results: Vec<GroupResult> = (0..num_groups)
            .map(|_| GroupResult {
                sum: 0_f32.to_bits(),
                count: 0,
                min: f32::MAX.to_bits(),
                max: f32::MIN.to_bits(),
            })
            .collect();

        let results_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Results Buffer"),
            contents: bytemuck::cast_slice(&init_results),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });

        let staging_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Staging Buffer"),
            size: (std::mem::size_of::<GroupResult>() * num_groups) as u64,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        // Create compute pipeline
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("GROUP BY Aggregation Shader"),
            source: wgpu::ShaderSource::Wgsl(GROUP_BY_AGG_SHADER.into()),
        });

        let bind_group_layout = self.device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("GROUP BY Bind Group Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("GROUP BY Pipeline Layout"),
            bind_group_layouts: &[&bind_group_layout],
            push_constant_ranges: &[],
        });

        let pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("GROUP BY Pipeline"),
            layout: Some(&pipeline_layout),
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("GROUP BY Bind Group"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: values_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: group_keys_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: results_buffer.as_entire_binding(),
                },
            ],
        });

        // Encode and submit
        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("GROUP BY Encoder"),
        });

        {
            let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("GROUP BY Pass"),
                timestamp_writes: None,
            });
            compute_pass.set_pipeline(&pipeline);
            compute_pass.set_bind_group(0, &bind_group, &[]);
            
            let workgroup_count = (values.len() as u32 + 255) / 256;
            // GPU has max 65535 workgroups per dimension, use 2D dispatch if needed
            if workgroup_count <= 65535 {
                compute_pass.dispatch_workgroups(workgroup_count, 1, 1);
            } else {
                let x = 65535;
                let y = (workgroup_count + 65534) / 65535;
                compute_pass.dispatch_workgroups(x, y, 1);
            }
        }

        encoder.copy_buffer_to_buffer(
            &results_buffer,
            0,
            &staging_buffer,
            0,
            (std::mem::size_of::<GroupResult>() * num_groups) as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        // Read results
        let buffer_slice = staging_buffer.slice(..);
        let (sender, receiver) = futures::channel::oneshot::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            sender.send(result).unwrap();
        });
        
        self.device.poll(wgpu::Maintain::Wait);
        receiver.await.context("Failed to map buffer")??;

        let data = buffer_slice.get_mapped_range();
        let results: Vec<GroupResult> = bytemuck::cast_slice(&data).to_vec();
        drop(data);
        staging_buffer.unmap();

        Ok(results)
    }

    pub async fn execute_hash_join_aggregate(
        &self,
        build_keys: &[i32],
        probe_keys: &[i32],
        probe_values: &[f32],
    ) -> Result<AggregateResult> {
        // Hash table size: 2x build side for ~50% load factor
        let table_size = (build_keys.len() * 2).next_power_of_two();
        
        // PHASE 1: Build hash table
        let build_keys_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Build Keys Buffer"),
            contents: bytemuck::cast_slice(build_keys),
            usage: wgpu::BufferUsages::STORAGE,
        });

        // Hash table entries: (key: i32, exists: u32) = 8 bytes
        #[repr(C)]
        #[derive(Copy, Clone, Pod, Zeroable)]
        struct HashEntry {
            key: i32,
            exists: u32,
        }
        
        let init_table: Vec<HashEntry> = vec![HashEntry { key: 0, exists: 0 }; table_size];
        let hash_table_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Hash Table Buffer"),
            contents: bytemuck::cast_slice(&init_table),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });

        let build_shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Hash Join Build Shader"),
            source: wgpu::ShaderSource::Wgsl(HASH_JOIN_BUILD_SHADER.into()),
        });

        let build_layout = self.device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Build Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let build_pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Build Pipeline Layout"),
            bind_group_layouts: &[&build_layout],
            push_constant_ranges: &[],
        });

        let build_pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Build Pipeline"),
            layout: Some(&build_pipeline_layout),
            module: &build_shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

        let build_bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Build Bind Group"),
            layout: &build_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: build_keys_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: hash_table_buffer.as_entire_binding(),
                },
            ],
        });

        // PHASE 2: Probe and aggregate
        let probe_keys_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Probe Keys Buffer"),
            contents: bytemuck::cast_slice(probe_keys),
            usage: wgpu::BufferUsages::STORAGE,
        });

        let probe_values_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Probe Values Buffer"),
            contents: bytemuck::cast_slice(probe_values),
            usage: wgpu::BufferUsages::STORAGE,
        });

        let init_result = AggregateResult {
            sum: 0_f32.to_bits(),
            count: 0,
            min: f32::MAX.to_bits(),
            max: f32::MIN.to_bits(),
        };
        let result_buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("Result Buffer"),
            contents: bytemuck::bytes_of(&init_result),
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        });

        let probe_shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("Hash Join Probe Shader"),
            source: wgpu::ShaderSource::Wgsl(HASH_JOIN_PROBE_SHADER.into()),
        });

        let probe_layout = self.device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("Probe Layout"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: true },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
                wgpu::BindGroupLayoutEntry {
                    binding: 3,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let probe_pipeline_layout = self.device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("Probe Pipeline Layout"),
            bind_group_layouts: &[&probe_layout],
            push_constant_ranges: &[],
        });

        let probe_pipeline = self.device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("Probe Pipeline"),
            layout: Some(&probe_pipeline_layout),
            module: &probe_shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });

        let probe_bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("Probe Bind Group"),
            layout: &probe_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: probe_keys_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: probe_values_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: hash_table_buffer.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: result_buffer.as_entire_binding(),
                },
            ],
        });

        let staging_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("Staging Buffer"),
            size: std::mem::size_of::<AggregateResult>() as u64,
            usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });

        // Execute both phases
        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("Hash Join Encoder"),
        });

        // Build phase
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Build Pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&build_pipeline);
            pass.set_bind_group(0, &build_bind_group, &[]);
            
            let workgroup_count = (build_keys.len() as u32 + 255) / 256;
            if workgroup_count <= 65535 {
                pass.dispatch_workgroups(workgroup_count, 1, 1);
            } else {
                let x = 65535;
                let y = (workgroup_count + 65534) / 65535;
                pass.dispatch_workgroups(x, y, 1);
            }
        }

        // Probe phase
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("Probe Pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&probe_pipeline);
            pass.set_bind_group(0, &probe_bind_group, &[]);
            
            let workgroup_count = (probe_keys.len() as u32 + 255) / 256;
            if workgroup_count <= 65535 {
                pass.dispatch_workgroups(workgroup_count, 1, 1);
            } else {
                let x = 65535;
                let y = (workgroup_count + 65534) / 65535;
                pass.dispatch_workgroups(x, y, 1);
            }
        }

        encoder.copy_buffer_to_buffer(
            &result_buffer,
            0,
            &staging_buffer,
            0,
            std::mem::size_of::<AggregateResult>() as u64,
        );

        self.queue.submit(Some(encoder.finish()));

        // Read result
        let buffer_slice = staging_buffer.slice(..);
        let (sender, receiver) = futures::channel::oneshot::channel();
        buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
            sender.send(result).unwrap();
        });
        
        self.device.poll(wgpu::Maintain::Wait);
        receiver.await.context("Failed to map buffer")??;

        let data = buffer_slice.get_mapped_range();
        let result: AggregateResult = *bytemuck::from_bytes(&data);
        drop(data);
        staging_buffer.unmap();

        Ok(result)
    }
}
