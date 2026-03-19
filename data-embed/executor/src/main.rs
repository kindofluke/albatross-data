use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

mod executor;
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
