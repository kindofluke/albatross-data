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
            OperationType::GroupBy => {
                if self.verbose {
                    println!("Routing GROUP BY aggregation to GPU execution");
                }
                self.execute_group_by_gpu(&ctx, physical_plan, total_start).await
            }
            OperationType::Window => {
                if self.verbose {
                    println!("Routing window function to CPU execution");
                }
                // Window functions not yet implemented on GPU - execute full query on CPU
                self.execute_full_query_cpu(&ctx, physical_plan, total_start).await
            }
            _ => {
                if self.verbose {
                    println!("Routing to CPU execution");
                }
                self.execute_full_query_cpu(&ctx, physical_plan, total_start).await
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

        // Extract the table scan from the aggregation plan
        // Typical plan structure: AggregateExec -> ProjectionExec -> ParquetExec
        let table_scan_plan = self.find_table_scan(physical_plan.clone())?;

        if self.verbose {
            println!("Found table scan: {}", table_scan_plan.name());
        }

        // Execute ONLY the table scan to get raw data (not the aggregation)
        let batches: Vec<arrow::record_batch::RecordBatch> =
            datafusion::physical_plan::collect(table_scan_plan, ctx.task_ctx()).await?;

        if batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all batches into one for GPU processing
        let input_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches)?
        };

        if self.verbose {
            println!("Loaded {} rows from table scan for GPU aggregation", input_batch.num_rows());
        }

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

    async fn execute_group_by_gpu(
        &self,
        ctx: &SessionContext,
        physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total_start: Instant,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use arrow::record_batch::RecordBatch;
        use datafusion::physical_plan::aggregates::AggregateExec;

        if self.verbose {
            println!("Attempting GPU GROUP BY execution");
        }

        // Try to downcast to AggregateExec to extract GROUP BY and aggregation info
        let agg_exec = match physical_plan.as_any().downcast_ref::<AggregateExec>() {
            Some(exec) => exec,
            None => {
                if self.verbose {
                    println!("Could not downcast to AggregateExec, falling back to CPU");
                }
                return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
            }
        };

        // Extract GROUP BY columns and aggregation expressions
        let group_expr = agg_exec.group_expr();
        let group_by_cols = group_expr.expr();
        let aggr_exprs = agg_exec.aggr_expr();

        if group_by_cols.is_empty() {
            if self.verbose {
                println!("No GROUP BY columns found");
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        if self.verbose {
            println!("Found {} GROUP BY column(s) and {} aggregation(s)",
                group_by_cols.len(), aggr_exprs.len());
            for (i, expr) in group_by_cols.iter().enumerate() {
                println!("  GROUP BY [{}]: {:?}", i, expr);
            }
            for (i, expr) in aggr_exprs.iter().enumerate() {
                println!("  AGGREGATE [{}]: {:?}", i, expr);
            }
        }

        // Extract table scan and get raw data
        let table_scan_plan = match self.find_table_scan(physical_plan.clone()) {
            Ok(scan) => scan,
            Err(e) => {
                if self.verbose {
                    println!("Could not find table scan: {}, falling back to CPU", e);
                }
                return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
            }
        };

        if self.verbose {
            println!("Found table scan: {}", table_scan_plan.name());
        }

        // Execute table scan to get raw data
        let batches: Vec<RecordBatch> =
            datafusion::physical_plan::collect(table_scan_plan, ctx.task_ctx()).await?;

        if batches.is_empty() {
            return Ok(None);
        }

        // Concatenate all batches
        let input_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches)?
        };

        let num_rows = input_batch.num_rows();

        if self.verbose {
            println!("Loaded {} rows from table scan", num_rows);
            println!("Schema: {:?}", input_batch.schema());
        }

        // VALIDATION: Check if dataset is suitable for GPU
        const MAX_ROWS_FOR_GPU: usize = 10_000_000; // 10M rows
        const MIN_ROWS_FOR_GPU: usize = 10_000; // Below this, CPU is faster

        if num_rows > MAX_ROWS_FOR_GPU {
            if self.verbose {
                println!("Dataset too large for GPU ({} > {} rows), falling back to CPU",
                    num_rows, MAX_ROWS_FOR_GPU);
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        if num_rows < MIN_ROWS_FOR_GPU {
            if self.verbose {
                println!("Dataset too small for GPU benefit ({} < {} rows), falling back to CPU",
                    num_rows, MIN_ROWS_FOR_GPU);
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        // Validate: Only support single GROUP BY column for now
        if group_by_cols.len() > 1 {
            if self.verbose {
                println!("Multiple GROUP BY columns not yet supported on GPU, falling back to CPU");
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        // Step 1: Extract GROUP BY column name
        let group_col_name = group_by_cols[0].0.to_string();

        if self.verbose {
            println!("GROUP BY column: {}", group_col_name);
        }

        // Find GROUP BY column in the input batch
        let group_col_idx = match input_batch.schema().index_of(&group_col_name) {
            Ok(idx) => idx,
            Err(_) => {
                if self.verbose {
                    println!("Could not find GROUP BY column '{}' in schema, falling back to CPU", group_col_name);
                }
                return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
            }
        };

        let group_col = input_batch.column(group_col_idx);

        // Step 2: Build mapping from group values to group IDs
        // For now, support Int32, Int64, UInt32, UInt64 group keys
        use arrow::array::{Int32Array, Int64Array, UInt32Array, UInt64Array};
        use arrow::datatypes::DataType;
        use std::collections::HashMap;

        let (group_keys, _group_id_map, num_groups) = match group_col.data_type() {
            DataType::Int32 => {
                let arr = group_col.as_any().downcast_ref::<Int32Array>().unwrap();
                let mut id_map: HashMap<i32, u32> = HashMap::new();
                let mut next_id = 0_u32;
                let keys: Vec<u32> = arr.values().iter().map(|&val| {
                    *id_map.entry(val).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    })
                }).collect();
                (keys, id_map.len(), next_id as usize)
            }
            DataType::Int64 => {
                let arr = group_col.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut id_map: HashMap<i64, u32> = HashMap::new();
                let mut next_id = 0_u32;
                let keys: Vec<u32> = arr.values().iter().map(|&val| {
                    *id_map.entry(val).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    })
                }).collect();
                (keys, id_map.len(), next_id as usize)
            }
            DataType::UInt32 => {
                let arr = group_col.as_any().downcast_ref::<UInt32Array>().unwrap();
                let mut id_map: HashMap<u32, u32> = HashMap::new();
                let mut next_id = 0_u32;
                let keys: Vec<u32> = arr.values().iter().map(|&val| {
                    *id_map.entry(val).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    })
                }).collect();
                (keys, id_map.len(), next_id as usize)
            }
            DataType::UInt64 => {
                let arr = group_col.as_any().downcast_ref::<UInt64Array>().unwrap();
                let mut id_map: HashMap<u64, u32> = HashMap::new();
                let mut next_id = 0_u32;
                let keys: Vec<u32> = arr.values().iter().map(|&val| {
                    *id_map.entry(val).or_insert_with(|| {
                        let id = next_id;
                        next_id += 1;
                        id
                    })
                }).collect();
                (keys, id_map.len(), next_id as usize)
            }
            _ => {
                if self.verbose {
                    println!("Unsupported GROUP BY column type: {:?}, falling back to CPU", group_col.data_type());
                }
                return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
            }
        };

        if self.verbose {
            println!("Found {} unique groups from {} rows", num_groups, num_rows);
        }

        // Validate group cardinality
        const MAX_GROUPS_FOR_GPU: usize = 1_000_000;
        if num_groups > MAX_GROUPS_FOR_GPU {
            if self.verbose {
                println!("Too many groups ({} > {}), falling back to CPU", num_groups, MAX_GROUPS_FOR_GPU);
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        // For very small cardinality, CPU is faster
        if num_groups < 10 {
            if self.verbose {
                println!("Very low cardinality ({} groups), CPU more efficient", num_groups);
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        // Step 3: Parse aggregation expressions
        // AggregateExpr provides name and args for each aggregation
        use arrow::array::{Array, Float64Array, UInt64Array as ArrowUInt64Array};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc as StdArc;

        struct AggInfo {
            agg_type: String,  // COUNT, SUM, AVG, MIN, MAX
            column_name: String,
            result_name: String,
        }

        let mut agg_infos: Vec<AggInfo> = Vec::new();

        for agg_expr in aggr_exprs.iter() {
            let agg_name = agg_expr.name().to_string();
            let fun_name = agg_expr.fun().to_string();

            if self.verbose {
                println!("Parsing aggregation: {} ({})", agg_name, fun_name);
            }

            // Extract the source column from the aggregation expression
            // For simple cases like COUNT(id) or SUM(quantity), the column is in the args
            let args = agg_expr.expressions();

            if args.is_empty() {
                if fun_name.to_uppercase() == "COUNT" {
                    // COUNT(*) - no specific column, count all rows
                    agg_infos.push(AggInfo {
                        agg_type: "COUNT".to_string(),
                        column_name: "*".to_string(),
                        result_name: agg_name,
                    });
                    continue;
                }
            }

            if args.len() == 1 {
                // Extract column name from the argument
                // The arg is typically a Column expression
                let arg_str = format!("{:?}", args[0]);

                // Try to extract column name from debug string
                // Format is usually: Column { name: "column_name", index: X }
                let column_name = if arg_str.contains("Column") {
                    // Parse out the column name
                    if let Some(start) = arg_str.find("name: \"") {
                        let start = start + 7;
                        if let Some(end) = arg_str[start..].find('"') {
                            arg_str[start..start + end].to_string()
                        } else {
                            continue;  // Skip this aggregation
                        }
                    } else {
                        continue;  // Skip this aggregation
                    }
                } else {
                    continue;  // Skip this aggregation
                };

                agg_infos.push(AggInfo {
                    agg_type: fun_name.to_uppercase(),
                    column_name,
                    result_name: agg_name,
                });
            }
        }

        if agg_infos.is_empty() {
            if self.verbose {
                println!("Could not parse any aggregation expressions, falling back to CPU");
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        }

        if self.verbose {
            println!("Parsed {} aggregations:", agg_infos.len());
            for info in &agg_infos {
                println!("  {}({}) AS {}", info.agg_type, info.column_name, info.result_name);
            }
        }

        // Step 4: Extract columns and call GPU for each aggregation
        // Initialize GPU engine
        let exec_start = Instant::now();
        let engine = match wgpu_engine::WgpuEngine::new().await {
            Ok(eng) => eng,
            Err(e) => {
                if self.verbose {
                    println!("Failed to initialize GPU engine: {}, falling back to CPU", e);
                }
                return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
            }
        };

        // For each aggregation, extract column and call GPU
        use arrow::array::{Float32Array, Float64Array as ArrowFloat64Array};

        let mut gpu_results: Vec<(String, Vec<wgpu_engine::GroupResult>)> = Vec::new();

        for agg_info in &agg_infos {
            if self.verbose {
                println!("Processing aggregation: {}({})", agg_info.agg_type, agg_info.column_name);
            }

            // Extract the column data
            let col_data = if agg_info.column_name == "*" {
                // COUNT(*) - create a dummy column of ones
                let ones: Vec<f32> = vec![1.0; num_rows];
                ones
            } else {
                // Find the column in the batch
                let col_idx = match input_batch.schema().index_of(&agg_info.column_name) {
                    Ok(idx) => idx,
                    Err(_) => {
                        if self.verbose {
                            println!("Column '{}' not found, falling back to CPU", agg_info.column_name);
                        }
                        return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
                    }
                };

                let col = input_batch.column(col_idx);

                // Convert to f32 for GPU
                match col.data_type() {
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        arr.values().iter().map(|&v| v as f32).collect()
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        arr.values().iter().map(|&v| v as f32).collect()
                    }
                    DataType::UInt32 => {
                        let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                        arr.values().iter().map(|&v| v as f32).collect()
                    }
                    DataType::UInt64 => {
                        let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                        arr.values().iter().map(|&v| v as f32).collect()
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        arr.values().to_vec()
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<ArrowFloat64Array>().unwrap();
                        arr.values().iter().map(|&v| v as f32).collect()
                    }
                    _ => {
                        if self.verbose {
                            println!("Unsupported column type: {:?}, falling back to CPU", col.data_type());
                        }
                        return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
                    }
                }
            };

            // Call GPU GROUP BY aggregation
            if self.verbose {
                println!("Calling GPU GROUP BY aggregation with {} rows, {} groups", col_data.len(), num_groups);
            }

            let gpu_result = match engine.execute_group_by_aggregation(&col_data, &group_keys, num_groups).await {
                Ok(result) => result,
                Err(e) => {
                    if self.verbose {
                        println!("GPU aggregation failed: {}, falling back to CPU", e);
                    }
                    return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
                }
            };

            gpu_results.push((agg_info.result_name.clone(), gpu_result));
        }

        let execution_time_ms = exec_start.elapsed().as_millis();
        if self.verbose {
            println!("GPU aggregation completed in {}ms", execution_time_ms);
        }

        // Step 5: Format results as Arrow RecordBatch
        // Need to combine: group values + all aggregation results

        // Build reverse mapping: group_id -> group_value
        let mut id_to_value: Vec<Option<i64>> = vec![None; num_groups];

        match group_col.data_type() {
            DataType::Int32 => {
                let arr = group_col.as_any().downcast_ref::<Int32Array>().unwrap();
                let mut seen = HashMap::new();
                for (i, &val) in arr.values().iter().enumerate() {
                    let group_id = group_keys[i] as usize;
                    if !seen.contains_key(&group_id) {
                        id_to_value[group_id] = Some(val as i64);
                        seen.insert(group_id, true);
                    }
                }
            }
            DataType::Int64 => {
                let arr = group_col.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut seen = HashMap::new();
                for (i, &val) in arr.values().iter().enumerate() {
                    let group_id = group_keys[i] as usize;
                    if !seen.contains_key(&group_id) {
                        id_to_value[group_id] = Some(val);
                        seen.insert(group_id, true);
                    }
                }
            }
            DataType::UInt32 => {
                let arr = group_col.as_any().downcast_ref::<UInt32Array>().unwrap();
                let mut seen = HashMap::new();
                for (i, &val) in arr.values().iter().enumerate() {
                    let group_id = group_keys[i] as usize;
                    if !seen.contains_key(&group_id) {
                        id_to_value[group_id] = Some(val as i64);
                        seen.insert(group_id, true);
                    }
                }
            }
            DataType::UInt64 => {
                let arr = group_col.as_any().downcast_ref::<UInt64Array>().unwrap();
                let mut seen = HashMap::new();
                for (i, &val) in arr.values().iter().enumerate() {
                    let group_id = group_keys[i] as usize;
                    if !seen.contains_key(&group_id) {
                        id_to_value[group_id] = Some(val as i64);
                        seen.insert(group_id, true);
                    }
                }
            }
            _ => unreachable!(),
        }

        // Build Arrow arrays for results
        let group_values: Vec<i64> = id_to_value.iter().map(|v| v.unwrap()).collect();
        let group_array = Int64Array::from(group_values.clone());

        // Build schema fields
        let mut fields = vec![Field::new(&group_col_name, DataType::Int64, false)];
        let mut arrays: Vec<StdArc<dyn Array>> = vec![StdArc::new(group_array)];

        // For each aggregation, extract the appropriate values from GroupResult
        for (result_name, gpu_result) in &gpu_results {
            // Find the aggregation info to determine which field to use
            let agg_info = agg_infos.iter().find(|a| &a.result_name == result_name).unwrap();

            match agg_info.agg_type.as_str() {
                "COUNT" => {
                    let counts: Vec<u64> = gpu_result.iter().map(|r| r.count as u64).collect();
                    fields.push(Field::new(result_name, DataType::UInt64, false));
                    arrays.push(StdArc::new(ArrowUInt64Array::from(counts)));
                }
                "SUM" => {
                    let sums: Vec<f64> = gpu_result.iter().map(|r| r.sum_f32() as f64).collect();
                    fields.push(Field::new(result_name, DataType::Float64, false));
                    arrays.push(StdArc::new(Float64Array::from(sums)));
                }
                "AVG" => {
                    let avgs: Vec<f64> = gpu_result.iter().map(|r| r.avg() as f64).collect();
                    fields.push(Field::new(result_name, DataType::Float64, false));
                    arrays.push(StdArc::new(Float64Array::from(avgs)));
                }
                "MIN" => {
                    let mins: Vec<f64> = gpu_result.iter().map(|r| r.min_f32() as f64).collect();
                    fields.push(Field::new(result_name, DataType::Float64, false));
                    arrays.push(StdArc::new(Float64Array::from(mins)));
                }
                "MAX" => {
                    let maxs: Vec<f64> = gpu_result.iter().map(|r| r.max_f32() as f64).collect();
                    fields.push(Field::new(result_name, DataType::Float64, false));
                    arrays.push(StdArc::new(Float64Array::from(maxs)));
                }
                _ => {
                    // Unknown aggregation type, use SUM as default
                    let sums: Vec<f64> = gpu_result.iter().map(|r| r.sum_f32() as f64).collect();
                    fields.push(Field::new(result_name, DataType::Float64, false));
                    arrays.push(StdArc::new(Float64Array::from(sums)));
                }
            }
        }

        let schema = StdArc::new(Schema::new(fields));
        let result_batch = RecordBatch::try_new(schema.clone(), arrays)?;

        if self.verbose {
            println!("GPU GROUP BY aggregation successful!");
            println!("  Execution time: {}ms", execution_time_ms);
            println!("  Computed {} aggregations for {} groups", gpu_results.len(), num_groups);
            println!("  Result schema: {:?}", result_batch.schema());
        }

        // Step 6: Handle ORDER BY with CPU sort
        // Check if there's a SortExec in the plan
        let has_order_by = {
            fn check_sort(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> bool {
                if plan.name() == "SortExec" {
                    return true;
                }
                for child in plan.children() {
                    if check_sort(child) {
                        return true;
                    }
                }
                false
            }
            check_sort(&physical_plan)
        };

        let final_batch = if has_order_by {
            if self.verbose {
                println!("ORDER BY detected, applying CPU sort to GPU results");
            }

            // For now, fall back to CPU for the full query to handle ORDER BY properly
            // TODO: Implement sorting on the GPU results
            if self.verbose {
                println!("Note: ORDER BY requires CPU execution for proper sorting");
                println!("Falling back to CPU for final result with ORDER BY");
            }
            return self.execute_full_query_cpu(ctx, physical_plan, total_start).await;
        } else {
            result_batch
        };

        // Convert to FFI format
        use arrow::array::StructArray;
        let struct_array = StructArray::from(final_batch);
        let array_data = struct_array.to_data();

        let (ffi_array, ffi_schema) = ffi::to_ffi(&array_data)?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *const FFI_ArrowArray;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *const FFI_ArrowSchema;

        let total_time_ms = total_start.elapsed().as_millis();

        if self.verbose {
            println!("Total time: {}ms (GPU exec: {}ms)", total_time_ms, execution_time_ms);
            println!("✓ GPU GROUP BY execution complete!");
        }

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

    async fn execute_full_query_cpu(
        &self,
        ctx: &SessionContext,
        physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        total_start: Instant,
    ) -> Result<Option<(*const ffi::FFI_ArrowArray, *const ffi::FFI_ArrowSchema)>> {
        use arrow::array::{Array, StructArray};

        if self.verbose {
            println!("Executing full query on CPU");
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

        // Concatenate all batches into a single batch
        let schema = batches[0].schema();
        let combined_batch = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&schema, &batches)?
        };

        if self.verbose {
            println!("Query completed - {} rows returned", combined_batch.num_rows());
            println!("Schema: {} columns", schema.fields().len());
            println!("Execution time: {}ms", execution_time_ms);
            println!("Total time: {}ms", total_start.elapsed().as_millis());
        }

        // Convert the combined batch to FFI format
        let struct_array = StructArray::from(combined_batch);
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

    /// Recursively find the table scan (ParquetExec) in the physical plan
    fn find_table_scan(
        &self,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let plan_name = plan.name();

        // If this is a table scan, return it
        if plan_name == "ParquetExec" || plan_name == "CsvExec" || plan_name == "MemoryExec" {
            return Ok(plan);
        }

        // Otherwise, recurse into children
        let children = plan.children();
        if children.is_empty() {
            return Err(anyhow::anyhow!("No table scan found in physical plan"));
        }

        // For aggregations, the table scan is typically the last child
        // Try last child first, then first child
        if let Some(child) = children.last() {
            if let Ok(scan) = self.find_table_scan(Arc::clone(child)) {
                return Ok(scan);
            }
        }

        if let Some(child) = children.first() {
            return self.find_table_scan(Arc::clone(child));
        }

        Err(anyhow::anyhow!("No table scan found in physical plan"))
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

    pub async fn get_tables_metadata(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
    ) -> Result<String> {
        use serde_json::json;
        use std::fs;

        if self.verbose {
            println!("Collecting metadata for {} table(s)...", parquet_files.len());
        }

        // Create DataFusion context
        let ctx = SessionContext::new();

        // Register parquet files
        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            if self.verbose {
                println!("  - Registering {} -> {:?}", table_name, path);
            }
            ctx.register_parquet(
                table_name,
                path.to_str().unwrap(),
                ParquetReadOptions::default()
            ).await?;
        }

        // Collect metadata for each table
        let mut tables_json = Vec::new();

        for (table_name, path) in table_names.iter().zip(parquet_files.iter()) {
            if self.verbose {
                println!("  - Getting metadata for {}", table_name);
            }

            // Get table from catalog
            let catalog = ctx.catalog("datafusion").unwrap();
            let schema = catalog.schema("public").unwrap();
            let table = schema.table(table_name).await.unwrap().unwrap();
            let table_schema = table.schema();

            // Get row count by executing COUNT(*)
            let count_query = format!("SELECT COUNT(*) as count FROM {}", table_name);
            let df = ctx.sql(&count_query).await?;
            let batches = df.collect().await?;

            let num_rows = if !batches.is_empty() && batches[0].num_rows() > 0 {
                use arrow::array::*;
                let count_array = batches[0].column(0);
                if let Some(int64_array) = count_array.as_any().downcast_ref::<Int64Array>() {
                    int64_array.value(0) as u64
                } else if let Some(uint64_array) = count_array.as_any().downcast_ref::<UInt64Array>() {
                    uint64_array.value(0)
                } else {
                    0
                }
            } else {
                0
            };

            // Get file size
            let file_size_bytes = match fs::metadata(path) {
                Ok(metadata) => metadata.len(),
                Err(_) => 0,
            };

            // Build columns metadata
            let mut columns_json = Vec::new();
            for field in table_schema.fields() {
                columns_json.push(json!({
                    "name": field.name(),
                    "data_type": format!("{:?}", field.data_type()),
                    "nullable": field.is_nullable(),
                }));
            }

            // Build table metadata
            tables_json.push(json!({
                "name": table_name,
                "file_path": path.to_str().unwrap(),
                "num_rows": num_rows,
                "file_size_bytes": file_size_bytes,
                "columns": columns_json,
            }));
        }

        let result = json!({
            "tables": tables_json
        });

        if self.verbose {
            println!("Metadata collection complete");
        }

        Ok(serde_json::to_string_pretty(&result)?)
    }
}
