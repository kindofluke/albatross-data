//! GPU buffer management utilities
//!
//! This module provides typed wrappers around WGPU buffers with conveniences
//! for creating, copying, and reading buffer data.

use anyhow::{Context, Result};
use bytemuck::Pod;
use wgpu::util::DeviceExt;

/// Creates a GPU storage buffer initialized with data from a slice
///
/// # Arguments
/// * `device` - The WGPU device
/// * `label` - Optional label for debugging
/// * `data` - Slice of data to initialize the buffer with (must impl Pod)
/// * `usage` - Additional buffer usage flags (STORAGE is always included)
///
/// # Returns
/// A buffer initialized with the data
pub fn create_storage_buffer<T: Pod>(
    device: &wgpu::Device,
    label: Option<&str>,
    data: &[T],
    usage: wgpu::BufferUsages,
) -> wgpu::Buffer {
    device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label,
        contents: bytemuck::cast_slice(data),
        usage: wgpu::BufferUsages::STORAGE | usage,
    })
}

/// Creates a GPU storage buffer for a single value
///
/// # Arguments
/// * `device` - The WGPU device
/// * `label` - Optional label for debugging
/// * `value` - Single value to initialize the buffer with (must impl Pod)
/// * `usage` - Additional buffer usage flags (STORAGE is always included)
///
/// # Returns
/// A buffer initialized with the value
pub fn create_storage_buffer_single<T: Pod>(
    device: &wgpu::Device,
    label: Option<&str>,
    value: &T,
    usage: wgpu::BufferUsages,
) -> wgpu::Buffer {
    device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label,
        contents: bytemuck::bytes_of(value),
        usage: wgpu::BufferUsages::STORAGE | usage,
    })
}

/// Creates an uninitialized GPU storage buffer for output
///
/// # Arguments
/// * `device` - The WGPU device
/// * `label` - Optional label for debugging
/// * `count` - Number of elements the buffer should hold
/// * `usage` - Additional buffer usage flags (STORAGE is always included)
///
/// # Returns
/// An uninitialized buffer
pub fn create_output_buffer<T: Pod>(
    device: &wgpu::Device,
    label: Option<&str>,
    count: usize,
    usage: wgpu::BufferUsages,
) -> wgpu::Buffer {
    device.create_buffer(&wgpu::BufferDescriptor {
        label,
        size: (std::mem::size_of::<T>() * count) as u64,
        usage: wgpu::BufferUsages::STORAGE | usage,
        mapped_at_creation: false,
    })
}

/// Creates a staging buffer for reading GPU results back to CPU
///
/// Staging buffers are CPU-readable and used to transfer data from GPU to CPU.
///
/// # Arguments
/// * `device` - The WGPU device
/// * `label` - Optional label for debugging
/// * `count` - Number of elements the buffer should hold
///
/// # Returns
/// A staging buffer with MAP_READ | COPY_DST usage
pub fn create_staging_buffer<T: Pod>(
    device: &wgpu::Device,
    label: Option<&str>,
    count: usize,
) -> wgpu::Buffer {
    device.create_buffer(&wgpu::BufferDescriptor {
        label,
        size: (std::mem::size_of::<T>() * count) as u64,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    })
}

/// Reads a single value from a staging buffer asynchronously
///
/// # Arguments
/// * `device` - The WGPU device (needed for polling)
/// * `buffer` - The staging buffer to read from
///
/// # Returns
/// The value read from the buffer
///
/// # Example
/// ```ignore
/// let result: AggregateResult = read_buffer_single(&device, &staging_buffer).await?;
/// ```
pub async fn read_buffer_single<T: Pod + Copy>(
    device: &wgpu::Device,
    buffer: &wgpu::Buffer,
) -> Result<T> {
    let buffer_slice = buffer.slice(..);
    let (sender, receiver) = futures::channel::oneshot::channel();

    buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
        sender.send(result).unwrap();
    });

    device.poll(wgpu::Maintain::Wait);
    receiver.await.context("Failed to map buffer")??;

    let data = buffer_slice.get_mapped_range();
    let result: T = *bytemuck::from_bytes(&data);
    drop(data);
    buffer.unmap();

    Ok(result)
}

/// Reads a vector of values from a staging buffer asynchronously
///
/// # Arguments
/// * `device` - The WGPU device (needed for polling)
/// * `buffer` - The staging buffer to read from
///
/// # Returns
/// A vector containing the data read from the buffer
///
/// # Example
/// ```ignore
/// let results: Vec<GroupResult> = read_buffer_vec(&device, &staging_buffer).await?;
/// ```
pub async fn read_buffer_vec<T: Pod>(
    device: &wgpu::Device,
    buffer: &wgpu::Buffer,
) -> Result<Vec<T>> {
    let buffer_slice = buffer.slice(..);
    let (sender, receiver) = futures::channel::oneshot::channel();

    buffer_slice.map_async(wgpu::MapMode::Read, move |result| {
        sender.send(result).unwrap();
    });

    device.poll(wgpu::Maintain::Wait);
    receiver.await.context("Failed to map buffer")??;

    let data = buffer_slice.get_mapped_range();
    let results: Vec<T> = bytemuck::cast_slice(&data).to_vec();
    drop(data);
    buffer.unmap();

    Ok(results)
}

/// Helper to copy a buffer to a staging buffer and read the result
///
/// This combines the common pattern of copying from a GPU buffer to a staging
/// buffer and then reading the staging buffer.
///
/// # Arguments
/// * `device` - The WGPU device
/// * `encoder` - Command encoder to record the copy operation
/// * `src_buffer` - Source GPU buffer
/// * `dst_staging` - Destination staging buffer
/// * `size` - Number of bytes to copy
pub fn copy_to_staging(
    encoder: &mut wgpu::CommandEncoder,
    src_buffer: &wgpu::Buffer,
    dst_staging: &wgpu::Buffer,
    size: u64,
) {
    encoder.copy_buffer_to_buffer(src_buffer, 0, dst_staging, 0, size);
}

/// Typed buffer builder for convenient buffer creation
///
/// # Example
/// ```ignore
/// let buffer = BufferBuilder::new(&device)
///     .with_label("My Buffer")
///     .with_data(&values)
///     .with_usage(wgpu::BufferUsages::COPY_DST)
///     .build();
/// ```
pub struct BufferBuilder<'a, T: Pod> {
    device: &'a wgpu::Device,
    label: Option<&'a str>,
    data: Option<&'a [T]>,
    single_value: Option<&'a T>,
    count: Option<usize>,
    usage: wgpu::BufferUsages,
}

impl<'a, T: Pod> BufferBuilder<'a, T> {
    /// Create a new buffer builder
    pub fn new(device: &'a wgpu::Device) -> Self {
        Self {
            device,
            label: None,
            data: None,
            single_value: None,
            count: None,
            usage: wgpu::BufferUsages::STORAGE,
        }
    }

    /// Set the buffer label for debugging
    pub fn with_label(mut self, label: &'a str) -> Self {
        self.label = Some(label);
        self
    }

    /// Initialize buffer with slice data
    pub fn with_data(mut self, data: &'a [T]) -> Self {
        self.data = Some(data);
        self
    }

    /// Initialize buffer with a single value
    pub fn with_single_value(mut self, value: &'a T) -> Self {
        self.single_value = Some(value);
        self
    }

    /// Create uninitialized buffer with element count
    pub fn with_count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Add additional buffer usage flags (STORAGE is always included)
    pub fn with_usage(mut self, usage: wgpu::BufferUsages) -> Self {
        self.usage = wgpu::BufferUsages::STORAGE | usage;
        self
    }

    /// Build the buffer
    pub fn build(self) -> wgpu::Buffer {
        if let Some(data) = self.data {
            create_storage_buffer(self.device, self.label, data, self.usage)
        } else if let Some(value) = self.single_value {
            create_storage_buffer_single(self.device, self.label, value, self.usage)
        } else if let Some(count) = self.count {
            create_output_buffer::<T>(self.device, self.label, count, self.usage)
        } else {
            panic!("BufferBuilder requires either data, single_value, or count");
        }
    }
}
