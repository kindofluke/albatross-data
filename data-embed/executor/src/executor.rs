use anyhow::{Context, Result};
use std::path::PathBuf;
use std::time::Instant;

use datafusion::prelude::*;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use crate::ffi::DuckDBConnection;
use crate::metrics::{GpuMetrics, GpuMonitor};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExecutionMode {
    Cpu,
    Gpu,
}

pub struct ExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub parse_time_ms: u128,
    pub execution_time_ms: u128,
    pub total_time_ms: u128,
    pub gpu_metrics: Option<GpuMetrics>,
}

struct ExecutionResultInternal {
    stdout: String,
    stderr: String,
    execution_time_ms: u128,
}

pub struct Executor {
    sirius_extension_path: PathBuf,
    substrait_extension_path: PathBuf,
    #[allow(dead_code)]
    sirius_config: Option<PathBuf>,
    verbose: bool,
}

impl Executor {
    pub fn new(duckdb_path: PathBuf, sirius_config: Option<PathBuf>, verbose: bool) -> Self {
        // Derive extension paths from duckdb_path - resolve to absolute paths
        let abs_duckdb_path = std::fs::canonicalize(&duckdb_path)
            .unwrap_or_else(|_| duckdb_path.clone());
        let duckdb_dir = abs_duckdb_path.parent().unwrap();
        let sirius_extension_path = duckdb_dir.join("extension/sirius/sirius.duckdb_extension");
        let substrait_extension_path = duckdb_dir.join("extension/substrait/substrait.duckdb_extension");
        
        Self {
            sirius_extension_path,
            substrait_extension_path,
            sirius_config,
            verbose,
        }
    }

    pub fn execute(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
        mode: ExecutionMode,
    ) -> Result<ExecutionResult> {
        let total_start = Instant::now();

        if self.verbose {
            println!("=== Execution Configuration ===");
            println!("Mode: {:?}", mode);
            println!("Files: {} parquet file(s)", parquet_files.len());
            println!("Query: {}", query);
            println!();
        }

        // Start GPU monitoring if in GPU mode
        let gpu_monitor = if mode == ExecutionMode::Gpu {
            let monitor = GpuMonitor::new();
            monitor.start()?;
            Some(monitor)
        } else {
            None
        };

        let parse_start = Instant::now();
        let result = match mode {
            ExecutionMode::Cpu => self.execute_cpu(parquet_files, table_names, query)?,
            ExecutionMode::Gpu => self.execute_gpu(parquet_files, table_names, query)?,
        };
        let parse_time_ms = parse_start.elapsed().as_millis();

        // Stop GPU monitoring
        let gpu_metrics = gpu_monitor.map(|m| m.stop());

        let total_time_ms = total_start.elapsed().as_millis();

        Ok(ExecutionResult {
            stdout: result.stdout,
            stderr: result.stderr,
            parse_time_ms,
            execution_time_ms: result.execution_time_ms,
            total_time_ms,
            gpu_metrics,
        })
    }

    fn execute_cpu(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<ExecutionResultInternal> {
        if self.verbose {
            println!("=== CPU Mode (Direct SQL) ===");
        }

        // Open DuckDB connection
        let conn = DuckDBConnection::open(None)
            .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;

        // Register parquet tables
        for (table_name, parquet_path) in table_names.iter().zip(parquet_files.iter()) {
            let abs_path = std::fs::canonicalize(parquet_path)
                .with_context(|| format!("Failed to resolve path: {:?}", parquet_path))?;
            let create_table = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM parquet_scan('{}');",
                table_name,
                abs_path.display()
            );
            conn.execute(&create_table)
                .map_err(|e| anyhow::anyhow!("Failed to register table {}: {}", table_name, e))?;
        }

        // Execute query directly on CPU
        let exec_start = Instant::now();
        let result = conn.execute(query)
            .map_err(|e| anyhow::anyhow!("Failed to execute query: {}", e))?;

        let execution_time_ms = exec_start.elapsed().as_millis();

        Ok(ExecutionResultInternal {
            stdout: result.to_string(),
            stderr: String::new(),
            execution_time_ms,
        })
    }

    fn execute_gpu(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<ExecutionResultInternal> {
        if self.verbose {
            println!("=== GPU Mode (Sirius) ===");
        }

        // Open DuckDB connection
        let conn = DuckDBConnection::open(None)
            .map_err(|e| anyhow::anyhow!("Failed to open DuckDB: {}", e))?;

        // Sirius extension is built into this DuckDB, no need to load
        // Initialize GPU buffers
        conn.execute("CALL gpu_buffer_init('1 GB', '2 GB', pinned_memory_size := '4 GB');")
            .map_err(|e| anyhow::anyhow!("Failed to initialize GPU buffers: {}", e))?;

        // Register parquet tables
        for (table_name, parquet_path) in table_names.iter().zip(parquet_files.iter()) {
            let abs_path = std::fs::canonicalize(parquet_path)
                .with_context(|| format!("Failed to resolve path: {:?}", parquet_path))?;
            let create_table = format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM parquet_scan('{}');",
                table_name,
                abs_path.display()
            );
            conn.execute(&create_table)
                .map_err(|e| anyhow::anyhow!("Failed to register table {}: {}", table_name, e))?;
        }

        // Execute via gpu_execution
        let exec_start = Instant::now();
        let escaped_query = query.replace('\'', "''");
        let gpu_query = format!("SELECT * FROM gpu_execution('{}');", escaped_query);
        
        let result = conn.execute(&gpu_query)
            .map_err(|e| anyhow::anyhow!("Failed to execute GPU query: {}", e))?;

        let execution_time_ms = exec_start.elapsed().as_millis();

        Ok(ExecutionResultInternal {
            stdout: result.to_string(),
            stderr: String::new(),
            execution_time_ms,
        })
    }

    fn generate_substrait_plan(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<Vec<u8>> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let ctx = SessionContext::new();
            
            // Register parquet files
            for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
                ctx.register_parquet(table_name, path.to_str().unwrap(), ParquetReadOptions::default())
                    .await?;
            }

            // Parse SQL to logical plan
            let df = ctx.sql(query).await?;
            let logical_plan = df.logical_plan().clone();

            // Convert to Substrait
            let substrait_plan = to_substrait_plan(&logical_plan, &ctx)?;
            
            // Serialize to bytes
            use prost::Message;
            let mut buf = Vec::new();
            substrait_plan.encode(&mut buf)?;
            
            Ok(buf)
        })
    }

    pub fn explain(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<String> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let ctx = SessionContext::new();
            for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
                ctx.register_parquet(table_name, path.to_str().unwrap(), ParquetReadOptions::default())
                    .await?;
            }

            let df = ctx.sql(query).await?;
            let plan = df.logical_plan();
            Ok(format!("{:#?}", plan))
        })
    }
}
