//! GPU compute pipeline builder utilities
//!
//! This module provides a builder pattern for creating WGPU compute pipelines
//! and bind groups, significantly reducing boilerplate code.

/// Buffer access mode for bind group layout entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferAccess {
    /// Read-only storage buffer
    ReadOnly,
    /// Read-write storage buffer
    ReadWrite,
}

/// Helper to create a bind group layout entry for a storage buffer
///
/// # Arguments
/// * `binding` - Binding index
/// * `access` - Read-only or read-write access
fn create_buffer_layout_entry(binding: u32, access: BufferAccess) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::COMPUTE,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Storage {
                read_only: access == BufferAccess::ReadOnly,
            },
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    }
}

/// Builder for creating compute pipelines with less boilerplate
///
/// # Example
/// ```ignore
/// let (pipeline, bind_group) = PipelineBuilder::new(&device, shader_source)
///     .with_label("My Pipeline")
///     .add_buffer(BufferAccess::ReadOnly)
///     .add_buffer(BufferAccess::ReadWrite)
///     .build(&[&input_buffer, &output_buffer]);
/// ```
pub struct PipelineBuilder<'a> {
    device: &'a wgpu::Device,
    shader_source: &'a str,
    label: Option<&'a str>,
    buffer_access: Vec<BufferAccess>,
    entry_point: &'a str,
}

impl<'a> PipelineBuilder<'a> {
    /// Create a new pipeline builder
    ///
    /// # Arguments
    /// * `device` - The WGPU device
    /// * `shader_source` - WGSL shader source code
    pub fn new(device: &'a wgpu::Device, shader_source: &'a str) -> Self {
        Self {
            device,
            shader_source,
            label: None,
            buffer_access: Vec::new(),
            entry_point: "main",
        }
    }

    /// Set a label for debugging (applied to all created resources)
    pub fn with_label(mut self, label: &'a str) -> Self {
        self.label = Some(label);
        self
    }

    /// Set the shader entry point (default: "main")
    pub fn with_entry_point(mut self, entry_point: &'a str) -> Self {
        self.entry_point = entry_point;
        self
    }

    /// Add a buffer binding with specified access mode
    ///
    /// Buffers are added in order and will be bound to sequential binding indices (0, 1, 2, ...).
    ///
    /// # Arguments
    /// * `access` - Whether the buffer is read-only or read-write
    pub fn add_buffer(mut self, access: BufferAccess) -> Self {
        self.buffer_access.push(access);
        self
    }

    /// Build the pipeline and create a bind group with the provided buffers
    ///
    /// # Arguments
    /// * `buffers` - Slice of buffers to bind (must match the order of add_buffer calls)
    ///
    /// # Returns
    /// Tuple of (ComputePipeline, BindGroup, BindGroupLayout)
    ///
    /// # Panics
    /// Panics if the number of buffers doesn't match the number of buffer bindings added
    pub fn build(
        self,
        buffers: &[&wgpu::Buffer],
    ) -> (wgpu::ComputePipeline, wgpu::BindGroup, wgpu::BindGroupLayout) {
        assert_eq!(
            buffers.len(),
            self.buffer_access.len(),
            "Number of buffers must match number of buffer bindings"
        );

        // Create shader module
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: self.label,
            source: wgpu::ShaderSource::Wgsl(self.shader_source.into()),
        });

        // Create bind group layout
        let layout_entries: Vec<_> = self
            .buffer_access
            .iter()
            .enumerate()
            .map(|(i, &access)| create_buffer_layout_entry(i as u32, access))
            .collect();

        let bind_group_layout =
            self.device
                .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                    label: self.label,
                    entries: &layout_entries,
                });

        // Create pipeline layout
        let pipeline_layout =
            self.device
                .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                    label: self.label,
                    bind_group_layouts: &[&bind_group_layout],
                    push_constant_ranges: &[],
                });

        // Create compute pipeline
        let pipeline = self
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: self.label,
                layout: Some(&pipeline_layout),
                module: &shader,
                entry_point: Some(self.entry_point),
                compilation_options: Default::default(),
                cache: None,
            });

        // Create bind group
        let bind_group_entries: Vec<_> = buffers
            .iter()
            .enumerate()
            .map(|(i, buffer)| wgpu::BindGroupEntry {
                binding: i as u32,
                resource: buffer.as_entire_binding(),
            })
            .collect();

        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: self.label,
            layout: &bind_group_layout,
            entries: &bind_group_entries,
        });

        (pipeline, bind_group, bind_group_layout)
    }

    /// Build just the pipeline and bind group layout without creating a bind group
    ///
    /// Use this when you need to create the bind group later with different buffers.
    ///
    /// # Returns
    /// Tuple of (ComputePipeline, BindGroupLayout)
    pub fn build_pipeline_only(self) -> (wgpu::ComputePipeline, wgpu::BindGroupLayout) {
        // Create shader module
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: self.label,
            source: wgpu::ShaderSource::Wgsl(self.shader_source.into()),
        });

        // Create bind group layout
        let layout_entries: Vec<_> = self
            .buffer_access
            .iter()
            .enumerate()
            .map(|(i, &access)| create_buffer_layout_entry(i as u32, access))
            .collect();

        let bind_group_layout =
            self.device
                .create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
                    label: self.label,
                    entries: &layout_entries,
                });

        // Create pipeline layout
        let pipeline_layout =
            self.device
                .create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                    label: self.label,
                    bind_group_layouts: &[&bind_group_layout],
                    push_constant_ranges: &[],
                });

        // Create compute pipeline
        let pipeline = self
            .device
            .create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: self.label,
                layout: Some(&pipeline_layout),
                module: &shader,
                entry_point: Some(self.entry_point),
                compilation_options: Default::default(),
                cache: None,
            });

        (pipeline, bind_group_layout)
    }
}

/// Helper to create a bind group from a layout and buffers
///
/// # Arguments
/// * `device` - The WGPU device
/// * `layout` - The bind group layout
/// * `label` - Optional label for debugging
/// * `buffers` - Slice of buffers to bind
///
/// # Returns
/// A bind group with the buffers bound in order
pub fn create_bind_group(
    device: &wgpu::Device,
    layout: &wgpu::BindGroupLayout,
    label: Option<&str>,
    buffers: &[&wgpu::Buffer],
) -> wgpu::BindGroup {
    let entries: Vec<_> = buffers
        .iter()
        .enumerate()
        .map(|(i, buffer)| wgpu::BindGroupEntry {
            binding: i as u32,
            resource: buffer.as_entire_binding(),
        })
        .collect();

    device.create_bind_group(&wgpu::BindGroupDescriptor {
        label,
        layout,
        entries: &entries,
    })
}
