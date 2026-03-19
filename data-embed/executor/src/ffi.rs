use libc::{c_char, c_void};
use std::ffi::{CStr, CString};
use std::ptr;

// Opaque types
#[repr(C)]
pub struct duckdb_database {
    _private: [u8; 0],
}

#[repr(C)]
pub struct duckdb_connection {
    _private: [u8; 0],
}

// duckdb_result is a struct allocated on the stack, not an opaque pointer
#[repr(C)]
pub struct duckdb_result {
    deprecated_column_count: u64,
    deprecated_row_count: u64,
    deprecated_rows_changed: u64,
    deprecated_columns: *mut c_void,
    deprecated_error_message: *mut c_char,
    internal_data: *mut c_void,
}

#[repr(C)]
#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum duckdb_state {
    DuckDBSuccess = 0,
    DuckDBError = 1,
}

// External C functions
#[link(name = "duckdb")]
extern "C" {
    pub fn duckdb_open(path: *const c_char, out_database: *mut *mut duckdb_database) -> duckdb_state;
    pub fn duckdb_close(database: *mut *mut duckdb_database);
    pub fn duckdb_connect(database: *mut duckdb_database, out_connection: *mut *mut duckdb_connection) -> duckdb_state;
    pub fn duckdb_disconnect(connection: *mut *mut duckdb_connection);
    pub fn duckdb_query(connection: *mut duckdb_connection, query: *const c_char, out_result: *mut duckdb_result) -> duckdb_state;
    pub fn duckdb_destroy_result(result: *mut duckdb_result);
    pub fn duckdb_column_count(result: *mut duckdb_result) -> u64;
    pub fn duckdb_row_count(result: *mut duckdb_result) -> u64;
    pub fn duckdb_column_name(result: *mut duckdb_result, col: u64) -> *const c_char;
    pub fn duckdb_value_varchar(result: *mut duckdb_result, col: u64, row: u64) -> *mut c_char;
    pub fn duckdb_result_error(result: *mut duckdb_result) -> *const c_char;
    pub fn duckdb_free(ptr: *mut c_void);
}

// Safe wrapper for DuckDB connection
pub struct DuckDBConnection {
    db: *mut duckdb_database,
    conn: *mut duckdb_connection,
}

impl DuckDBConnection {
    pub fn open(path: Option<&str>) -> Result<Self, String> {
        unsafe {
            let mut db: *mut duckdb_database = ptr::null_mut();
            let path_cstr = match path {
                Some(p) => CString::new(p).map_err(|e| format!("Invalid path: {}", e))?,
                None => CString::new(":memory:").unwrap(),
            };
            
            let state = duckdb_open(path_cstr.as_ptr(), &mut db);
            if state != duckdb_state::DuckDBSuccess {
                return Err("Failed to open DuckDB database".to_string());
            }
            
            let mut conn: *mut duckdb_connection = ptr::null_mut();
            let state = duckdb_connect(db, &mut conn);
            if state != duckdb_state::DuckDBSuccess {
                duckdb_close(&mut db);
                return Err("Failed to connect to DuckDB database".to_string());
            }
            
            Ok(DuckDBConnection { db, conn })
        }
    }
    
    pub fn execute(&self, query: &str) -> Result<QueryResult, String> {
        unsafe {
            let query_cstr = CString::new(query).map_err(|e| format!("Invalid query: {}", e))?;
            let mut result: duckdb_result = std::mem::zeroed();
            
            let state = duckdb_query(self.conn, query_cstr.as_ptr(), &mut result);
            
            if state != duckdb_state::DuckDBSuccess {
                let error_ptr = duckdb_result_error(&mut result);
                let error_msg = if !error_ptr.is_null() {
                    CStr::from_ptr(error_ptr).to_string_lossy().to_string()
                } else {
                    "Unknown error".to_string()
                };
                duckdb_destroy_result(&mut result);
                return Err(error_msg);
            }
            
            Ok(QueryResult { result })
        }
    }
}

impl Drop for DuckDBConnection {
    fn drop(&mut self) {
        unsafe {
            if !self.conn.is_null() {
                duckdb_disconnect(&mut self.conn);
            }
            if !self.db.is_null() {
                duckdb_close(&mut self.db);
            }
        }
    }
}

// Safe wrapper for query results
pub struct QueryResult {
    result: duckdb_result,
}

impl QueryResult {
    pub fn column_count(&self) -> u64 {
        unsafe { duckdb_column_count(&self.result as *const _ as *mut _) }
    }
    
    pub fn row_count(&self) -> u64 {
        unsafe { duckdb_row_count(&self.result as *const _ as *mut _) }
    }
    
    pub fn column_name(&self, col: u64) -> Option<String> {
        unsafe {
            let name_ptr = duckdb_column_name(&self.result as *const _ as *mut _, col);
            if name_ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(name_ptr).to_string_lossy().to_string())
            }
        }
    }
    
    pub fn get_value(&self, col: u64, row: u64) -> Option<String> {
        unsafe {
            let value_ptr = duckdb_value_varchar(&self.result as *const _ as *mut _, col, row);
            if value_ptr.is_null() {
                None
            } else {
                let value = CStr::from_ptr(value_ptr).to_string_lossy().to_string();
                duckdb_free(value_ptr as *mut c_void);
                Some(value)
            }
        }
    }
    
    pub fn to_string(&self) -> String {
        let mut output = String::new();
        let col_count = self.column_count();
        let row_count = self.row_count();
        
        // Header
        let mut headers = Vec::new();
        for col in 0..col_count {
            headers.push(self.column_name(col).unwrap_or_else(|| format!("col{}", col)));
        }
        output.push_str(&headers.join("\t"));
        output.push('\n');
        
        // Separator
        output.push_str(&"-".repeat(headers.iter().map(|h| h.len()).sum::<usize>() + (col_count as usize - 1) * 1));
        output.push('\n');
        
        // Rows
        for row in 0..row_count {
            let mut values = Vec::new();
            for col in 0..col_count {
                values.push(self.get_value(col, row).unwrap_or_else(|| "NULL".to_string()));
            }
            output.push_str(&values.join("\t"));
            output.push('\n');
        }
        
        output
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_result(&mut self.result);
        }
    }
}
