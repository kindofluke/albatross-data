# Vulkan Debugging Guide for Albatross Data

This guide helps debug why Vulkan might not be working on Linux with NVIDIA GPUs.

## Quick Diagnostics

### 1. Check GPU Detection

Run this Rust code snippet to see what WGPU detects:

```rust
use wgpu::{Instance, Backends, RequestAdapterOptions};

#[tokio::main]
async fn main() {
    let instance = Instance::new(wgpu::InstanceDescriptor {
        backends: Backends::all(),
        ..Default::default()
    });
    
    match instance.request_adapter(&RequestAdapterOptions::default()).await {
        Some(adapter) => {
            let info = adapter.get_info();
            println!("GPU Found:");
            println!("  Name: {}", info.name);
            println!("  Backend: {:?}", info.backend);
            println!("  Device Type: {:?}", info.device_type);
            println!("  Driver: {}", info.driver);
            println!("  Driver Info: {}", info.driver_info);
        }
        None => println!("No GPU adapter found!"),
    }
}
```

### 2. Check Vulkan ICD Loader

```bash
# Verify Vulkan SDK is installed
apt-cache policy vulkan-tools libvulkan-dev

# Check NVIDIA ICD file
cat /etc/vulkan/icd.d/nvidia_icd.json

# Should output something like:
# {
#    "file_format_version" : "1.0.0",
#    "ICD": {
#        "library_path": "libnvidia-glvk.so.1",
#        "api_version" : "1.2.148"
#    }
# }

# Test with vulkaninfo
vulkaninfo | head -50
```

### 3. Test NVIDIA Driver

```bash
# Check driver version
nvidia-smi

# Verify Vulkan support
lspci | grep NVIDIA
nvidia-smi -q | grep "Driver Version"

# Check if Vulkan library is available
ldconfig -p | grep vulkan
```

## Common Vulkan Issues on Linux

### Issue 1: "No GPU Adapter Found"

**Cause**: Vulkan loader not found or misconfigured

**Fix**:
```bash
# Install Vulkan SDK
sudo apt-get install vulkan-tools vulkan-headers libvulkan-dev

# For NVIDIA, also install:
sudo apt-get install libnvidia-gl-495  # Replace 495 with your driver version

# Verify installation
pkg-config --modversion vulkan
```

### Issue 2: "Vulkan Device Creation Failed"

**Cause**: Wrong device limits or missing extensions

**Fix**: Modify `wgpu_engine.rs` to use default limits:

```rust
// Current code (may fail):
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
    .await?;

// If this fails, check adapter limits:
let adapter_limits = adapter.limits();
println!("Max Workgroup Size X: {}", adapter_limits.max_compute_workgroup_size_x);
println!("Max Workgroup Size Y: {}", adapter_limits.max_compute_workgroup_size_y);
println!("Max Workgroup Size Z: {}", adapter_limits.max_compute_workgroup_size_z);
```

### Issue 3: "Shader Compilation Failed"

**Cause**: Shader uses unsupported features or exceeds limits

**Fix**: Check shader workgroup size:

```wgsl
// Current in wgsl_shader.rs:
@compute @workgroup_size(256)  // May be too large!

// For maximum compatibility:
@compute @workgroup_size(128)  // More portable
```

Also verify that workgroup memory doesn't exceed limits:

```rust
// Add this to WgpuEngine::new():
let adapter_limits = adapter.limits();
println!("Max Workgroup Memory: {} bytes", adapter_limits.max_compute_workgroup_storage_size);
```

## Proposed Fixes for Albatross

### Fix 1: Make Vulkan Backend Explicit

**File**: `executor/src/wgpu_engine.rs`, lines 43-46

**Current**:
```rust
let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
    backends: wgpu::Backends::all(),
    ..Default::default()
});
```

**Proposed**:
```rust
#[cfg(target_os = "linux")]
let backends = wgpu::Backends::VULKAN;
#[cfg(target_os = "macos")]
let backends = wgpu::Backends::METAL;
#[cfg(target_os = "windows")]
let backends = wgpu::Backends::DX12;

let instance = wgpu::Instance::new(wgpu::InstanceDescriptor {
    backends,
    flags: wgpu::InstanceFlags::empty(),
    ..Default::default()
});
```

### Fix 2: Add Diagnostic Logging

**File**: `executor/src/wgpu_engine.rs`, lines 214-227

**Add after WgpuEngine::new() creates device**:
```rust
#[cfg(debug_assertions)]
{
    let limits = adapter.limits();
    eprintln!("GPU Adapter Info:");
    eprintln!("  Name: {}", info.name);
    eprintln!("  Backend: {:?}", info.backend);
    eprintln!("  Max Workgroup Size: ({}, {}, {})",
        limits.max_compute_workgroup_size_x,
        limits.max_compute_workgroup_size_y,
        limits.max_compute_workgroup_size_z);
    eprintln!("  Max Workgroup Memory: {} bytes", 
        limits.max_compute_workgroup_storage_size);
}
```

### Fix 3: Adjust Workgroup Size Dynamically

**File**: `executor/src/wgsl_shader.rs` and `gpu_dispatch.rs`

**Current**: Hard-coded 256 threads

**Proposed**: Query adapter limits
```rust
fn get_optimal_workgroup_size(adapter: &wgpu::Adapter) -> u32 {
    let limits = adapter.limits();
    let max_x = limits.max_compute_workgroup_size_x;
    
    // Use largest power of 2 that fits
    if max_x >= 256 { 256 }
    else if max_x >= 128 { 128 }
    else if max_x >= 64 { 64 }
    else { 32 }
}
```

## Testing the Fix

### Test 1: GPU Detection

```bash
cd data-embed
cargo build --release
cargo run --example test_gpu_info
# Should print GPU information
```

### Test 2: Shader Compilation

```bash
cargo test wgpu_engine::tests
# Should pass if shaders compile
```

### Test 3: Simple Aggregation

```bash
cargo run --release --bin data-run -- \
  -q "SELECT COUNT(*) FROM orders" \
  -f data/orders.parquet \
  --gpu
# Should execute (currently will fall back to CPU)
```

## Environment Variables for Debugging

```bash
# Enable Vulkan validation layers
export VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation
export VK_LAYER_PATH=/usr/share/vulkan/explicit_layer.d

# Specify GPU device (if multiple available)
export VULKAN_DEVICE=0

# Enable debug output
export RUST_LOG=debug

# Run your test
cargo run --release --bin data-run -- -q "SELECT * FROM orders LIMIT 1" -f data/orders.parquet
```

## WGSL Workgroup Size Considerations

### NVIDIA GPU Limits

| Property | Limit |
|----------|-------|
| Max threads per block | 1024 |
| Max blocks per grid | 65535 per dimension |
| Warp size | 32 threads |
| Shared memory per block | 96 KB (configurable up to 192 KB) |

### WGSL Workgroup Limits

| Property | Constraint |
|----------|-----------|
| `@workgroup_size(x, y, z)` | x * y * z <= adapter limit (usually 256-1024) |
| Shared memory (`var<workgroup>`) | <= adapter.limits().max_compute_workgroup_storage_size |

### Current Bottleneck

The shader uses `@workgroup_size(256)` which is safe, but the 2D dispatch indexing has a bug:

```wgsl
let idx = global_id.y * 65535u * 256u + global_id.x;
```

This assumes:
1. Workgroup size is always 256
2. Max workgroups in X = 65535

**Fix**: Make this dynamic:
```wgsl
// Pass workgroup_size as a constant
const WORKGROUP_SIZE = 256u;

fn compute_global_idx() -> u32 {
    let max_workgroups_x = 65535u;
    return global_id.y * max_workgroups_x * WORKGROUP_SIZE + global_id.x;
}
```

## Validation Checklist

- [ ] `vulkaninfo` runs successfully
- [ ] `nvidia-smi` shows your GPU
- [ ] `/etc/vulkan/icd.d/nvidia_icd.json` exists
- [ ] `ldd` shows `libnvidia-glvk.so` is available
- [ ] WGPU adapter detection finds a GPU
- [ ] Shader compilation doesn't error
- [ ] Workgroup size is <= adapter limits
- [ ] Shared memory usage is within limits

## Next Steps

1. Run diagnostic test above to identify the specific issue
2. Apply appropriate fix from the "Proposed Fixes" section
3. Validate with test commands
4. Enable GPU execution: change `if false &&` to `if true &&` in `lib.rs`

