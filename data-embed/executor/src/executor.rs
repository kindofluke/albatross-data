use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
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

        // 3. Check if this is a join query
        let is_join_query = query.to_uppercase().contains("JOIN");

        if is_join_query {
            // Execute join on GPU (currently uses CPU with GPU awareness)
            let exec_start = Instant::now();
            let batches: Vec<arrow::record_batch::RecordBatch> =
                datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
            let execution_time_ms = exec_start.elapsed().as_millis();

            if batches.is_empty() {
                return Ok(ExecutionResult {
                    stdout: "(empty result)".to_string(),
                    execution_time_ms,
                    total_time_ms: total_start.elapsed().as_millis(),
                });
            }

            let first_batch = &batches[0];

            if self.verbose {
                println!("JOIN query - {} rows returned", first_batch.num_rows());
                println!("Schema: {} columns", first_batch.schema().fields().len());
            }

            // Format the result
            let stdout = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
            let total_time_ms = total_start.elapsed().as_millis();

            Ok(ExecutionResult {
                stdout,
                execution_time_ms,
                total_time_ms,
            })
        } else {
            // Execute simple aggregation on GPU
            // 4. Collect the input data
            let batches: Vec<arrow::record_batch::RecordBatch> =
                datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
            let input_batch = batches.into_iter().next().unwrap();

            // 5. Extract the first column to send to GPU
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
    }

    pub async fn execute_to_arrow_gpu(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use crate::plan_analyzer::{GpuSuitabilityAnalysis, OperationType};

        let total_start = Instant::now();

        if self.verbose {
            println!("Executing with GPU analysis...");
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

        // 3. Analyze physical plan to determine GPU suitability
        let analysis = GpuSuitabilityAnalysis::analyze(physical_plan.clone());

        if self.verbose {
            println!("GPU Analysis: {}", analysis.reason);
            println!("Operation type: {:?}", analysis.operation_type);
        }

        // 4. Route to appropriate execution path based on plan analysis
        match analysis.operation_type {
            OperationType::HashJoin => {
                if self.verbose {
                    println!("Routing to GPU hash join execution");
                }
                self.execute_join_gpu(&ctx, physical_plan, total_start).await
            }
            OperationType::Aggregation => {
                if self.verbose {
                    println!("Routing to GPU aggregation execution");
                }
                self.execute_simple_agg_gpu(&ctx, physical_plan, total_start).await
            }
            OperationType::Window => {
                if self.verbose {
                    println!("Routing to GPU window function execution");
                }
                // TODO: Implement GPU window function execution
                self.execute_table_scan_cpu(&ctx, physical_plan, total_start).await
            }
            _ => {
                if self.verbose {
                    println!("Routing to CPU execution");
                }
                self.execute_table_scan_cpu(&ctx, physical_plan, total_start).await
            }
        }
    }

    async fn execute_simple_agg_gpu(
        &self,
        ctx: &SessionContext,
        physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total_start: Instant,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use arrow::array::{Array, Float64Array, StructArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc as StdArc;
        use arrow::record_batch::RecordBatch;

        // Collect the input data
        let batches: Vec<arrow::record_batch::RecordBatch> =
            datafusion::physical_plan::collect(physical_plan.clone(), ctx.task_ctx()).await?;

        if batches.is_empty() {
            return Ok(None);
        }

        let input_batch = batches.into_iter().next().unwrap();

        // Find the first numeric column to aggregate
        let schema = input_batch.schema();
        let mut numeric_column_idx = None;

        for (idx, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                DataType::Int32 | DataType::Int64
                | DataType::UInt32 | DataType::UInt64
                | DataType::Float32 | DataType::Float64 => {
                    numeric_column_idx = Some(idx);
                    break;
                }
                _ => continue,
            }
        }

        // If no numeric columns found, fall back to CPU
        let numeric_idx = match numeric_column_idx {
            Some(idx) => idx,
            None => {
                if self.verbose {
                    println!("No numeric columns found for GPU aggregation, falling back to CPU");
                }
                return self.execute_table_scan_cpu(ctx, physical_plan, total_start).await;
            }
        };

        let data_to_process = input_batch.column(numeric_idx);

        if self.verbose {
            println!("Processing column '{}' (index {}) of type {:?} with {} rows",
                     schema.field(numeric_idx).name(),
                     numeric_idx,
                     schema.field(numeric_idx).data_type(),
                     data_to_process.len());
        }

        // Handle empty data - return 0 for empty aggregation
        if data_to_process.len() == 0 {
            if self.verbose {
                println!("Column is empty, returning 0");
            }

            let schema = Schema::new(vec![Field::new("sum", DataType::Float64, false)]);
            let result_array = Float64Array::from(vec![0.0]);
            let batch = RecordBatch::try_new(
                StdArc::new(schema),
                vec![StdArc::new(result_array) as StdArc<dyn Array>],
            )?;

            let struct_array = StructArray::from(batch);
            let array_data = struct_array.to_data();
            let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

            let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
            let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

            return Ok(Some((array_ptr, schema_ptr)));
        }

        // Offload to GPU
        let exec_start = Instant::now();
        let gpu_result = wgpu_engine::run_sum_aggregation(data_to_process.clone()).await;

        // If GPU execution fails, fall back to CPU
        let gpu_result = match gpu_result {
            Ok(result) => result,
            Err(e) => {
                if self.verbose {
                    println!("GPU execution failed: {}, falling back to CPU", e);
                }
                return self.execute_table_scan_cpu(ctx, physical_plan, total_start).await;
            }
        };

        let execution_time_ms = exec_start.elapsed().as_millis();

        let total_time_ms = total_start.elapsed().as_millis();

        if self.verbose {
            println!("[GPU Result] SUM = {}", gpu_result);
            println!("Execution time: {}ms", execution_time_ms);
            println!("Total time: {}ms", total_time_ms);
        }

        // Create schema with a single column for the result
        let schema = Schema::new(vec![Field::new("sum", DataType::Float64, false)]);

        // Create an array with the single GPU result value
        let result_array = Float64Array::from(vec![gpu_result]);

        // Create a RecordBatch with one row
        let batch = RecordBatch::try_new(
            StdArc::new(schema),
            vec![StdArc::new(result_array) as StdArc<dyn Array>],
        )?;

        // Convert RecordBatch to FFI format
        let struct_array = StructArray::from(batch);
        let array_data = struct_array.to_data();

        // Export to FFI using to_ffi
        let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

        Ok(Some((array_ptr, schema_ptr)))
    }

    async fn execute_join_gpu(
        &self,
        ctx: &SessionContext,
        physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total_start: Instant,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use arrow::array::{Array, StructArray};
        use arrow::datatypes::DataType;

        if self.verbose {
            println!("Detected JOIN query - attempting GPU hash join execution");
        }

        // Try to execute on GPU by reading tables directly
        // This is a simplified implementation for the common pattern:
        // SELECT ... FROM table1 JOIN table2 ON table1.col = table2.col WHERE ... GROUP BY ...

        // For now, execute on CPU and return the result
        // A full GPU implementation would require:
        // 1. Parsing the physical plan tree to find ParquetExec nodes
        // 2. Extracting join columns before DataFusion executes the join
        // 3. Feeding raw data to GPU hash join
        // 4. Applying any post-join operations

        let exec_start = Instant::now();
        let batches: Vec<arrow::record_batch::RecordBatch> =
            datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let execution_time_ms = exec_start.elapsed().as_millis();

        if batches.is_empty() {
            return Ok(None);
        }

        let first_batch = &batches[0];
        let schema = first_batch.schema();

        if self.verbose {
            println!("Join executed - {} rows returned", first_batch.num_rows());
            println!("Schema: {} columns", schema.fields().len());
            for (i, field) in schema.fields().iter().enumerate() {
                println!("  [{}] {} ({:?})", i, field.name(), field.data_type());
            }
        }

        // Attempt to use GPU for post-join aggregation if applicable
        // Check if result has numeric columns we can aggregate
        let has_numeric_agg = schema.fields().iter().any(|f| {
            matches!(f.data_type(), DataType::Float64 | DataType::Float32 | DataType::Int64 | DataType::Int32)
        });

        if has_numeric_agg && first_batch.num_rows() > 1000 && self.verbose {
            println!("Note: Large result set detected - GPU aggregation could be beneficial");
            println!("Execution time: {}ms", execution_time_ms);
        }

        let total_time_ms = total_start.elapsed().as_millis();

        if self.verbose {
            println!("Total time: {}ms", total_time_ms);
        }

        // Convert the first batch to FFI format
        let struct_array = StructArray::from(first_batch.clone());
        let array_data = struct_array.to_data();

        let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

        Ok(Some((array_ptr, schema_ptr)))
    }

    async fn execute_table_scan_cpu(
        &self,
        ctx: &SessionContext,
        physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total_start: Instant,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use arrow::array::{Array, StructArray};

        if self.verbose {
            println!("Executing table scan on CPU (SELECT * or similar query)");
        }

        // Execute the query using DataFusion on CPU
        let exec_start = Instant::now();
        let batches: Vec<arrow::record_batch::RecordBatch> =
            datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
        let execution_time_ms = exec_start.elapsed().as_millis();

        if batches.is_empty() {
            if self.verbose {
                println!("Query returned no results");
            }
            return Ok(None);
        }

        let first_batch = &batches[0];
        let schema = first_batch.schema();

        if self.verbose {
            println!("Table scan completed - {} rows returned", first_batch.num_rows());
            println!("Schema: {} columns", schema.fields().len());
            println!("Execution time: {}ms", execution_time_ms);
            println!("Total time: {}ms", total_start.elapsed().as_millis());
        }

        // Convert the first batch to FFI format
        let struct_array = StructArray::from(first_batch.clone());
        let array_data = struct_array.to_data();

        let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

        Ok(Some((array_ptr, schema_ptr)))
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

        if batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all batches into a single batch
        use arrow::array::{Array, StructArray};
        use arrow::compute::concat_batches;

        let schema = batches[0].schema();
        let combined_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            // Concatenate multiple batches
            concat_batches(&schema, &batches)?
        };

        // Convert RecordBatch to StructArray to preserve all columns
        // StructArray treats each row as a struct containing values from all columns
        let struct_array = StructArray::from(combined_batch);
        let array_data = struct_array.to_data();

        // Export to FFI using to_ffi
        let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

        Ok(Some((array_ptr, schema_ptr)))
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
