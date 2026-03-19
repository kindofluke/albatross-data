use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

mod executor;
mod metrics;
mod ffi;
mod velox;

use executor::{ExecutionMode, Executor};

#[derive(Parser, Debug)]
#[command(name = "executor")]
#[command(about = "Execute SQL queries on Parquet files with CPU or GPU acceleration", long_about = None)]
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

    /// Execution mode: cpu, gpu, or both
    #[arg(short, long, default_value = "cpu")]
    mode: String,

    /// Path to DuckDB binary
    #[arg(long, default_value = "lib/sirius/build/release/duckdb")]
    duckdb: PathBuf,

    /// Path to Sirius config file (for GPU mode)
    #[arg(long, default_value = "../lib/sirius/sirius_config.cfg")]
    sirius_config: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Only build and print the DataFusion/Substrait plan; do not execute
    #[arg(long)]
    explain_only: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Parse execution mode
    let modes: Vec<ExecutionMode> = match args.mode.to_lowercase().as_str() {
        "cpu" => vec![ExecutionMode::Cpu],
        "gpu" => vec![ExecutionMode::Gpu],
        "both" => vec![ExecutionMode::Cpu, ExecutionMode::Gpu],
        _ => anyhow::bail!("Invalid mode: {}. Use 'cpu', 'gpu', or 'both'", args.mode),
    };

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

    // Create executor
    let sirius_config = if args.sirius_config.exists() {
        Some(args.sirius_config)
    } else {
        if modes.contains(&ExecutionMode::Gpu) {
            eprintln!("Warning: Sirius config not found at {:?}", args.sirius_config);
        }
        None
    };

    let executor = Executor::new(args.duckdb.clone(), sirius_config, args.verbose);

    if args.explain_only {
        println!("--- DataFusion Logical Plan ---");
        let plan = executor.explain(&args.files, &table_names, &args.query)?;
        println!("{}", plan);
        return Ok(());
    }

    // Execute for each mode
    for mode in modes {
        println!("\n{}", "=".repeat(60));
        println!("Mode: {:?}", mode);
        println!("{}\n", "=".repeat(60));

        let result = executor
            .execute(&args.files, &table_names, &args.query, mode)
            .context(format!("Failed to execute in {:?} mode", mode))?;

        // Print results
        println!("{}", result.stdout);

        // Print timing
        println!("\n--- Timing ---");
        println!("Parse time:     {}ms", result.parse_time_ms);
        println!("Execution time: {}ms", result.execution_time_ms);
        println!("Total time:     {}ms", result.total_time_ms);

        // Print GPU metrics if available
        if let Some(gpu_metrics) = result.gpu_metrics {
            println!("\n--- GPU Metrics ---");
            println!("Peak utilization: {}%", gpu_metrics.peak_utilization);
            println!("Peak memory:      {} MB", gpu_metrics.peak_memory_mb);
            println!("Avg utilization:  {:.1}%", gpu_metrics.avg_utilization);
            println!("Avg memory:       {:.1} MB", gpu_metrics.avg_memory_mb);
            println!("Samples:          {}", gpu_metrics.samples);
        }

        if args.verbose && !result.stderr.is_empty() {
            println!("\n--- Stderr ---");
            println!("{}", result.stderr);
        }
    }

    Ok(())
}
