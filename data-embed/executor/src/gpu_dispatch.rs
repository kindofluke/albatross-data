//! GPU workgroup dispatch utilities
//!
//! This module provides helper functions for calculating and dispatching
//! GPU compute workgroups, handling hardware limits and 2D dispatch fallback.

use wgpu::ComputePass;

/// Default workgroup size used across all compute operations
pub const DEFAULT_WORKGROUP_SIZE: u32 = 256;

/// Maximum workgroups per dimension (hardware limit)
pub const MAX_WORKGROUPS_PER_DIM: u32 = 65535;

/// Dispatch a 1D compute operation, automatically using 2D dispatch if needed
///
/// GPUs have a maximum of 65535 workgroups per dimension. If the element count
/// requires more workgroups, this function automatically switches to a 2D dispatch
/// pattern (x=65535, y=ceil(total/65535)).
///
/// # Arguments
/// * `pass` - The compute pass to dispatch on
/// * `element_count` - Total number of elements to process
/// * `workgroup_size` - Number of threads per workgroup (typically 256)
///
/// # Example
/// ```ignore
/// dispatch_1d(&mut compute_pass, values.len() as u32, 256);
/// ```
pub fn dispatch_1d(pass: &mut ComputePass, element_count: u32, workgroup_size: u32) {
    let workgroup_count = element_count.div_ceil(workgroup_size);

    if workgroup_count <= MAX_WORKGROUPS_PER_DIM {
        // Simple 1D dispatch
        pass.dispatch_workgroups(workgroup_count, 1, 1);
    } else {
        // Fall back to 2D dispatch for very large datasets
        let x = MAX_WORKGROUPS_PER_DIM;
        let y = workgroup_count.div_ceil(MAX_WORKGROUPS_PER_DIM);
        pass.dispatch_workgroups(x, y, 1);
    }
}

/// Dispatch a 1D compute operation with default workgroup size (256)
///
/// Convenience wrapper around `dispatch_1d` using the default workgroup size.
///
/// # Arguments
/// * `pass` - The compute pass to dispatch on
/// * `element_count` - Total number of elements to process
pub fn dispatch_1d_default(pass: &mut ComputePass, element_count: u32) {
    dispatch_1d(pass, element_count, DEFAULT_WORKGROUP_SIZE);
}

/// Calculate workgroup dimensions for a 1D dispatch
///
/// Returns (x, y, z) dimensions for the dispatch, automatically handling
/// the 2D fallback for large element counts.
///
/// # Arguments
/// * `element_count` - Total number of elements to process
/// * `workgroup_size` - Number of threads per workgroup
///
/// # Returns
/// Tuple of (x, y, z) workgroup counts
pub fn calculate_workgroup_dims(element_count: u32, workgroup_size: u32) -> (u32, u32, u32) {
    let workgroup_count = element_count.div_ceil(workgroup_size);

    if workgroup_count <= MAX_WORKGROUPS_PER_DIM {
        (workgroup_count, 1, 1)
    } else {
        let x = MAX_WORKGROUPS_PER_DIM;
        let y = workgroup_count.div_ceil(MAX_WORKGROUPS_PER_DIM);
        (x, y, 1)
    }
}

/// Calculate number of workgroups needed for an element count
///
/// # Arguments
/// * `element_count` - Total number of elements to process
/// * `workgroup_size` - Number of threads per workgroup
///
/// # Returns
/// Total number of workgroups needed (may exceed MAX_WORKGROUPS_PER_DIM)
pub fn calculate_workgroup_count(element_count: u32, workgroup_size: u32) -> u32 {
    element_count.div_ceil(workgroup_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_workgroup_count() {
        assert_eq!(calculate_workgroup_count(256, 256), 1);
        assert_eq!(calculate_workgroup_count(257, 256), 2);
        assert_eq!(calculate_workgroup_count(512, 256), 2);
        assert_eq!(calculate_workgroup_count(1000, 256), 4);
    }

    #[test]
    fn test_calculate_workgroup_dims_small() {
        // Small dataset - 1D dispatch
        let (x, y, z) = calculate_workgroup_dims(1000, 256);
        assert_eq!(x, 4);
        assert_eq!(y, 1);
        assert_eq!(z, 1);
    }

    #[test]
    fn test_calculate_workgroup_dims_large() {
        // Large dataset requiring 2D dispatch
        let element_count = 20_000_000; // 20M elements
        let (x, y, z) = calculate_workgroup_dims(element_count, 256);
        assert_eq!(x, MAX_WORKGROUPS_PER_DIM);
        assert!(y > 1); // Should need multiple Y workgroups
        assert_eq!(z, 1);

        // Verify we can process all elements
        let total_workgroups = x * y;
        let can_process = total_workgroups * 256;
        assert!(can_process >= element_count);
    }

    #[test]
    fn test_calculate_workgroup_dims_exact_limit() {
        // Exactly at the limit
        let element_count = MAX_WORKGROUPS_PER_DIM * 256;
        let (x, y, z) = calculate_workgroup_dims(element_count, 256);
        assert_eq!(x, MAX_WORKGROUPS_PER_DIM);
        assert_eq!(y, 1);
        assert_eq!(z, 1);
    }
}
