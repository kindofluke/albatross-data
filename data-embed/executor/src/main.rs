use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

mod executor;
mod wgsl_shader;
pub mod wgpu_engine;

use executor::Executor;

#[derive(Parser, Debug)]
#[command(name = "executor")]
#[command(about = "Execute SQL queries on Parquet files using DataFusion")]
struct Args {
    /// SQL query to execute
    #[arg(short, long)]
    query: String,

    /// Parquet file paths (can be specified multiple times)
    #[arg(short = 'f', long = "file", required = true)]
    files: Vec<PathBuf>,

    /// Table names corresponding to files (defaults to file stems)
    #[arg(short = 't', long = "table")]
    tables: Vec<String>,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Only show the logical plan; do not execute
    #[arg(long)]
    explain_only: bool,

    /// Show the physical plan; do not execute
    #[arg(long)]
    physical_plan: bool,

    /// Output Substrait plan instead of executing query
    #[arg(long)]
    substrait: bool,

    /// Output Substrait plan as debug text instead of binary
    #[arg(long)]
    substrait_text: bool,

    /// Output file for Substrait plan (when --substrait is used)
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Generate table names if not provided
    let table_names = if args.tables.is_empty() {
        args.files
            .iter()
            .map(|f| {
                f.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("table")
                    .to_string()
            })
            .collect()
    } else {
        if args.tables.len() != args.files.len() {
            anyhow::bail!(
                "Number of table names ({}) must match number of files ({})",
                args.tables.len(),
                args.files.len()
            );
        }
        args.tables.clone()
    };

    // Verify files exist
    for file in &args.files {
        if !file.exists() {
            anyhow::bail!("File not found: {:?}", file);
        }
    }

    let executor = Executor::new(args.verbose);

    if args.substrait || args.substrait_text {
        let (substrait_bytes, substrait_plan) = executor
            .to_substrait(&args.files, &table_names, &args.query)
            .await
            .context("Failed to generate Substrait plan")?;

        if args.substrait_text {
            // Output as debug text
            let text = format!("{:#?}", substrait_plan);
            if let Some(output_path) = args.output {
                std::fs::write(&output_path, &text)
                    .context("Failed to write Substrait plan to file")?;
                println!("✓ Substrait plan (text) written to: {:?}", output_path);
            } else {
                println!("--- Substrait Plan (Debug Format) ---");
                println!("{}", text);
            }
        } else {
            // Output as binary
            if let Some(output_path) = args.output {
                std::fs::write(&output_path, &substrait_bytes)
                    .context("Failed to write Substrait plan to file")?;
                println!("✓ Substrait plan written to: {:?} ({} bytes)", output_path, substrait_bytes.len());
            } else {
                // Print as hex dump if no output file specified
                println!("--- Substrait Plan ({} bytes) ---", substrait_bytes.len());
                for (i, chunk) in substrait_bytes.chunks(16).enumerate() {
                    print!("{:08x}  ", i * 16);
                    for byte in chunk {
                        print!("{:02x} ", byte);
                    }
                    println!();
                }
            }
        }
        return Ok(());
    }

    if args.physical_plan {
        println!("--- Physical Plan ---");
        let plan = executor.physical_plan(&args.files, &table_names, &args.query).await?;
        println!("{}", plan);
        return Ok(());
    }

    if args.explain_only {
        println!("--- Logical Plan ---");
        let plan = executor.explain(&args.files, &table_names, &args.query).await?;
        println!("{}", plan);
        return Ok(());
    }

    // Execute query
    let result = executor
        .execute(&args.files, &table_names, &args.query)
        .await
        .context("Failed to execute query")?;

    // Print results
    println!("{}", result.stdout);

    // Print timing
    if args.verbose {
        println!("\n--- Timing ---");
        println!("Execution time: {}ms", result.execution_time_ms);
        println!("Total time:     {}ms", result.total_time_ms);
    }

    Ok(())
}
