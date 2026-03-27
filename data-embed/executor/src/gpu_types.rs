//! GPU result types and shared data structures
//!
//! This module contains the result types used for GPU computations.
//! All types use #[repr(C)] for zero-copy transfer between GPU and CPU.

use bytemuck::{Pod, Zeroable};

/// Result structure for global aggregations (SUM, COUNT, MIN, MAX)
///
/// Float values are stored as u32 bit patterns because WGSL doesn't support
/// native float atomics. Use the helper methods to convert back to f32.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct AggregateResult {
    /// Sum stored as f32 bits (use sum_f32() to convert)
    pub sum: u32,
    /// Count of elements
    pub count: u32,
    /// Minimum value stored as f32 bits (use min_f32() to convert)
    pub min: u32,
    /// Maximum value stored as f32 bits (use max_f32() to convert)
    pub max: u32,
}

impl AggregateResult {
    /// Convert sum from u32 bit pattern to f32
    pub fn sum_f32(&self) -> f32 {
        f32::from_bits(self.sum)
    }

    /// Convert min from u32 bit pattern to f32
    pub fn min_f32(&self) -> f32 {
        f32::from_bits(self.min)
    }

    /// Convert max from u32 bit pattern to f32
    pub fn max_f32(&self) -> f32 {
        f32::from_bits(self.max)
    }

    /// Calculate average (sum / count)
    pub fn avg(&self) -> f32 {
        if self.count > 0 {
            self.sum_f32() / (self.count as f32)
        } else {
            0.0
        }
    }
}

/// Result structure for GROUP BY aggregations
///
/// Similar to AggregateResult but used for per-group computations.
/// Float values are stored as u32 bit patterns for atomic operations.
#[repr(C)]
#[derive(Copy, Clone, Debug, Pod, Zeroable)]
pub struct GroupResult {
    /// Sum stored as f32 bits (use sum_f32() to convert)
    pub sum: u32,
    /// Count of elements in this group
    pub count: u32,
    /// Minimum value stored as f32 bits (use min_f32() to convert)
    pub min: u32,
    /// Maximum value stored as f32 bits (use max_f32() to convert)
    pub max: u32,
}

impl GroupResult {
    /// Convert sum from u32 bit pattern to f32
    pub fn sum_f32(&self) -> f32 {
        f32::from_bits(self.sum)
    }

    /// Convert min from u32 bit pattern to f32
    pub fn min_f32(&self) -> f32 {
        f32::from_bits(self.min)
    }

    /// Convert max from u32 bit pattern to f32
    pub fn max_f32(&self) -> f32 {
        f32::from_bits(self.max)
    }

    /// Calculate average (sum / count)
    pub fn avg(&self) -> f32 {
        if self.count > 0 {
            self.sum_f32() / (self.count as f32)
        } else {
            0.0
        }
    }
}
