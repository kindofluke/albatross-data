// Core modules
pub mod wgsl_shader;
pub mod wgpu_engine;
pub mod executor;

// GPU infrastructure modules
pub mod gpu_types;
pub mod gpu_buffers;
pub mod gpu_pipeline;
pub mod gpu_dispatch;

// GPU operation modules
pub mod aggregations;
pub mod joins;
pub mod window;

use std::ffi::{CStr, c_char};
use std::sync::OnceLock;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use executor::Executor;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}

#[no_mangle]
pub extern "C" fn execute_query_to_arrow(
    query: *const c_char,
    data_path: *const c_char,
    array_out: *mut *const FFI_ArrowArray,
    schema_out: *mut *const FFI_ArrowSchema,
) -> i32 {
    // Safety: caller must ensure query and data_path are valid C strings
    let query_str = unsafe {
        match CStr::from_ptr(query).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        }
    };
    
    let data_path_str = unsafe {
        match CStr::from_ptr(data_path).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        }
    };
    
    // Find parquet files in data_path
    let parquet_files: Vec<PathBuf> = match std::fs::read_dir(data_path_str) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .collect(),
        Err(_) => return -2,
    };
    
    if parquet_files.is_empty() {
        return -3;
    }
    
    // Generate table names from file stems
    let table_names: Vec<String> = parquet_files
        .iter()
        .map(|f| {
            f.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("table")
                .to_string()
        })
        .collect();
    
    // Execute query using async runtime
    let result = get_runtime().block_on(async {
        let executor = Executor::new(false);
        executor.execute_to_arrow(&parquet_files, &table_names, query_str).await
    });
    
    match result {
        Ok(Some((array_ptr, schema_ptr))) => {
            unsafe {
                *array_out = array_ptr;
                *schema_out = schema_ptr;
            }
            0
        }
        Ok(None) => -4, // Empty result
        Err(_) => -5,   // Execution error
    }
}

#[no_mangle]
pub extern "C" fn release_arrow_pointers(
    array: *const FFI_ArrowArray,
    schema: *const FFI_ArrowSchema,
) {
    if !array.is_null() {
        unsafe {
            let _ = Box::from_raw(array as *mut FFI_ArrowArray);
        }
    }
    if !schema.is_null() {
        unsafe {
            let _ = Box::from_raw(schema as *mut FFI_ArrowSchema);
        }
    }
}
