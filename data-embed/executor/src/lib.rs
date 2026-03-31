// Core modules
pub mod wgsl_shader;
pub mod wgpu_engine;
pub mod executor;
pub mod plan_analyzer;

// GPU infrastructure modules
pub mod gpu_types;
pub mod gpu_buffers;
pub mod gpu_pipeline;
pub mod gpu_dispatch;

// GPU operation modules
pub mod aggregations;
pub mod joins;
pub mod window;

use std::ffi::{CStr, CString, c_char};
use std::sync::{OnceLock, Mutex};
use std::path::PathBuf;
use tokio::runtime::Runtime;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use executor::Executor;
use wgpu_engine::{is_gpu_available, get_gpu_info};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}

fn set_last_error(error: String) {
    if let Ok(mut last_error) = LAST_ERROR.lock() {
        *last_error = Some(error);
    }
}

#[no_mangle]
pub extern "C" fn get_last_error_message() -> *mut c_char {
    let error_msg = LAST_ERROR.lock()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| "Unknown error".to_string());

    CString::new(error_msg)
        .unwrap_or_else(|_| CString::new("Error message contains null byte").unwrap())
        .into_raw()
}

#[no_mangle]
pub extern "C" fn free_error_message(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn execute_query_to_arrow(
    query: *const c_char,
    data_path: *const c_char,
    array_out: *mut *const FFI_ArrowArray,
    schema_out: *mut *const FFI_ArrowSchema,
) -> i32 {
    let gpu_available = get_runtime().block_on(async {
        is_gpu_available().await
    });

    // Use plan analyzer to intelligently route queries to GPU or CPU
    // The execute_to_arrow_gpu function will analyze the physical plan
    // and automatically fall back to CPU for unsupported operations
    if gpu_available {
        execute_query_gpu(query, data_path, array_out, schema_out)
    } else {
        execute_query_cpu(query, data_path, array_out, schema_out)
    }
}


#[no_mangle]
pub extern "C" fn execute_query_cpu(
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
        Ok(None) => {
            set_last_error("Query returned no results".to_string());
            -4
        }
        Err(e) => {
            set_last_error(format!("Query execution failed: {}", e));
            -5
        }
    }
}

#[no_mangle]
pub extern "C" fn execute_query_gpu(
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
        let executor = Executor::new(true);  // Enable verbose logging for diagnostics
        executor.execute_to_arrow_gpu(&parquet_files, &table_names, query_str).await
    });

    match result {
        Ok(Some((array_ptr, schema_ptr))) => {
            unsafe {
                *array_out = array_ptr;
                *schema_out = schema_ptr;
            }
            0
        }
        Ok(None) => {
            set_last_error("Query returned no results".to_string());
            -4
        }
        Err(e) => {
            set_last_error(format!("Query execution failed: {}", e));
            -5
        }
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

/// C-compatible struct for GPU information
#[repr(C)]
pub struct CGpuInfo {
    /// Pointer to GPU name string (null-terminated)
    pub name: *mut c_char,
    /// Pointer to backend type string (null-terminated)
    pub backend: *mut c_char,
    /// Pointer to device type string (null-terminated)
    pub device_type: *mut c_char,
    /// Pointer to driver name string (null-terminated)
    pub driver: *mut c_char,
    /// Pointer to driver info string (null-terminated)
    pub driver_info: *mut c_char,
    /// Whether GPU is available
    pub available: i32,
}

/// Check if a GPU is available
///
/// Returns 1 if GPU is available, 0 otherwise
#[no_mangle]
pub extern "C" fn check_gpu_available() -> i32 {
    let result = get_runtime().block_on(async {
        is_gpu_available().await
    });

    if result { 1 } else { 0 }
}

/// Get detailed GPU information
///
/// Returns a pointer to CGpuInfo struct, or null if no GPU available.
/// Caller must free the returned pointer using free_gpu_info().
#[no_mangle]
pub extern "C" fn get_gpu_information() -> *mut CGpuInfo {
    let result = get_runtime().block_on(async {
        get_gpu_info().await
    });

    match result {
        Some(info) => {
            // Convert Rust strings to C strings
            let name = match CString::new(info.name) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };

            let backend = match CString::new(info.backend) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };

            let device_type = match CString::new(info.device_type) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };

            let driver = match CString::new(info.driver) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };

            let driver_info = match CString::new(info.driver_info) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            };

            let c_info = CGpuInfo {
                name,
                backend,
                device_type,
                driver,
                driver_info,
                available: if info.available { 1 } else { 0 },
            };

            Box::into_raw(Box::new(c_info))
        }
        None => std::ptr::null_mut(),
    }
}

/// Free GPU information struct
///
/// Must be called to free memory allocated by get_gpu_information()
#[no_mangle]
pub extern "C" fn free_gpu_info(info: *mut CGpuInfo) {
    if info.is_null() {
        return;
    }

    unsafe {
        // Retake ownership and free strings
        let info_box = Box::from_raw(info);
        if !info_box.name.is_null() {
            let _ = CString::from_raw(info_box.name);
        }
        if !info_box.backend.is_null() {
            let _ = CString::from_raw(info_box.backend);
        }
        if !info_box.device_type.is_null() {
            let _ = CString::from_raw(info_box.device_type);
        }
        if !info_box.driver.is_null() {
            let _ = CString::from_raw(info_box.driver);
        }
        if !info_box.driver_info.is_null() {
            let _ = CString::from_raw(info_box.driver_info);
        }
        // info_box is dropped here, freeing the struct
    }
}
