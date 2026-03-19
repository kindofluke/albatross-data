use anyhow::Result;
use std::path::PathBuf;
use std::time::Instant;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan;
use prost::Message;

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

    pub async fn execute(
        &self,
        parquet_files: &[PathBuf],
        table_names: &[String],
        query: &str,
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
            println!("\nExecuting query: {}", query);
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
            arrow::util::pretty::pretty_format_batches(&batches)?.to_string()
        };

        let total_time_ms = total_start.elapsed().as_millis();

        Ok(ExecutionResult {
            stdout,
            execution_time_ms,
            total_time_ms,
        })
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
            println!("\nParsing query: {}", query);
        }

        // Parse SQL and get optimized logical plan
        let df = ctx.sql(query).await?;
        let logical_plan = df.into_optimized_plan()?;

        if self.verbose {
            println!("\nLogical Plan:");
            println!("{:#?}", logical_plan);
        }

        // Convert to Substrait
        if self.verbose {
            println!("\nConverting to Substrait...");
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
