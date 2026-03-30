use anyhow::Result;
use std::path::PathBuf;
use std::time::Instant;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan;
use prost::Message;
use arrow::csv::Writer;
use arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};

use crate::wgpu_engine;

pub struct ExecutionResult {
    pub stdout: String,
    pub execution_time_ms: u128,
    pub total_time_ms: u128,
}

pub struct Executor {
    verbose: bool,
}

impl Executor {
    pub fn new(verbose: bool) -> Self {
        Self { verbose }
    }

    pub async fn execute_gpu(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<ExecutionResult> {
        let total_start = Instant::now();

        if self.verbose {
            println!("Executing on GPU...");
        }

        // 1. Create DataFusion context and register tables
        let ctx = SessionContext::new();
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }

        // 2. Get the physical plan
        let df = ctx.sql(query).await?;
        let physical_plan = df.create_physical_plan().await?;

        // 3. (Placeholder) Check if the plan is a simple aggregation
        // In a real implementation, this would be a visitor traversing the plan
        let is_simple_agg = physical_plan.schema().fields().len() == 1;

        if !is_simple_agg {
            anyhow::bail!("GPU execution only supports simple aggregations for now.");
        }

        // 4. Collect the input data
        let batches: Vec<arrow::record_batch::RecordBatch> = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let input_batch = batches.into_iter().next().unwrap();

        // 5. (Placeholder) Extract the first column to send to GPU
        let data_to_process = input_batch.column(0);

        // 6. Offload to GPU
        let exec_start = Instant::now();
        let gpu_result = wgpu_engine::run_sum_aggregation(data_to_process.clone()).await?;
        let execution_time_ms = exec_start.elapsed().as_millis();

        // 7. Format results
        let stdout = format!("[GPU Result] SUM = {}", gpu_result);
        let total_time_ms = total_start.elapsed().as_millis();

        Ok(ExecutionResult {
            stdout,
            execution_time_ms,
            total_time_ms,
        })
    }

    pub async fn execute_to_arrow_gpu(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        let total_start = Instant::now();

        if self.verbose {
            println!("Executing on GPU...");
        }

        // 1. Create DataFusion context and register tables
        let ctx = SessionContext::new();
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
        }

        // 2. Get the physical plan
        let df = ctx.sql(query).await?;
        let physical_plan = df.create_physical_plan().await?;

        // 3. (Placeholder) Check if the plan is a simple aggregation
        // In a real implementation, this would be a visitor traversing the plan
        let is_simple_agg = physical_plan.schema().fields().len() == 1;

        if !is_simple_agg {
            anyhow::bail!("GPU execution only supports simple aggregations for now.");
        }

        // 4. Collect the input data
        let batches: Vec<arrow::record_batch::RecordBatch> = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let input_batch = batches.into_iter().next().unwrap();

        // 5. (Placeholder) Extract the first column to send to GPU
        let data_to_process = input_batch.column(0);

        // 6. Offload to GPU
        let exec_start = Instant::now();
        let gpu_result = wgpu_engine::run_sum_aggregation(data_to_process.clone()).await?;
        let execution_time_ms = exec_start.elapsed().as_millis();

        // 7. Format results
        let stdout = format!("[GPU Result] SUM = {}", gpu_result);
        let total_time_ms = total_start.elapsed().as_millis();

        if self.verbose {
            println!("{}", stdout);
            println!("Execution time: {}ms", execution_time_ms);
            println!("Total time: {}ms", total_time_ms);
        }

        // TODO: This is a placeholder. We need to convert the GPU result to an Arrow array.
        Ok(None)
    }

    pub async fn execute(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
        csv: bool,
    ) -> Result<ExecutionResult> {
        let total_start = Instant::now();

        if self.verbose {
            println!("Registering {} parquet file(s)...", parquet_files.len());
        }

        // Create DataFusion context
        let ctx = SessionContext::new();
        
        // Register parquet files
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            if self.verbose {
                println!("  - {} -> {:?}", table_name, path);
            }
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        if self.verbose {
            println!("
Executing query: {}", query);
        }

        // Execute query
        let exec_start = Instant::now();
        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;
        let execution_time_ms = exec_start.elapsed().as_millis();

        // Format results
        let stdout = if batches.is_empty() {
            "(empty result)".to_string()
        } else {
            if csv {
                let mut bytes = vec![];
                {
                    let mut writer = Writer::new(&mut bytes);
                    for batch in &batches {
                        writer.write(batch)?;
                    }
                }
                String::from_utf8(bytes)?
            } else {
                arrow::util::pretty::pretty_format_batches(&batches)?.to_string()
            }
        };

        let total_time_ms = total_start.elapsed().as_millis();

        Ok(ExecutionResult {
            stdout,
            execution_time_ms,
            total_time_ms,
        })
    }

    pub async fn execute_to_arrow(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        // Create DataFusion context
        let ctx = SessionContext::new();
        
        // Register parquet files
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        // Execute query
        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;

        if let Some(batch) = batches.into_iter().next() {
            // Convert RecordBatch to StructArray to preserve all columns
            // StructArray treats each row as a struct containing values from all columns
            use arrow::array::{Array, StructArray};

            let struct_array = StructArray::from(batch);
            let array_data = struct_array.to_data();

            // Export to FFI using to_ffi
            let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

            let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
            let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

            Ok(Some((array_ptr, schema_ptr)))
        } else {
            Ok(None)
        }
    }

    pub async fn explain(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<String> {
        let ctx = SessionContext::new();
        
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        let df = ctx.sql(query).await?;
        let plan = df.logical_plan();
        Ok(format!("{:#?}", plan))
    }

    pub async fn physical_plan(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<String> {
        let ctx = SessionContext::new();
        
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        let df = ctx.sql(query).await?;
        let physical_plan = df.create_physical_plan().await?;
        
        // Use displayable for pretty formatting
        use datafusion::physical_plan::displayable;
        let result = displayable(physical_plan.as_ref()).indent(true).to_string();
        Ok(result)
    }

    pub async fn to_substrait(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<(Vec<u8>, Box<datafusion_substrait::substrait::proto::Plan>)> {
        let ctx = SessionContext::new();
        
        // Register parquet files
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            if self.verbose {
                println!("  - {} -> {:?}", table_name, path);
            }
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        if self.verbose {
            println!("
Parsing query: {}", query);
        }

        // Parse SQL and get optimized logical plan
        let df = ctx.sql(query).await?;
        let logical_plan = df.into_optimized_plan()?;

        if self.verbose {
            println!("
Logical Plan:");
            println!("{:#?}", logical_plan);
        }

        // Convert to Substrait
        if self.verbose {
            println!("
Converting to Substrait...");
        }
        let substrait_plan = logical_plan::producer::to_substrait_plan(&logical_plan, &ctx)?;

        // Serialize to bytes
        let mut buf = Vec::new();
        substrait_plan.encode(&mut buf)?;

        if self.verbose {
            println!("Substrait plan size: {} bytes", buf.len());
        }

        Ok((buf, substrait_plan))
    }
}
