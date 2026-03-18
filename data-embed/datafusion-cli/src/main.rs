use anyhow::{Context, Result};
use clap::Parser;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan;
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "datafusion-cli")]
#[command(about = "DataFusion SQL to Substrait converter", long_about = None)]
struct Args {
    /// SQL query to execute
    #[arg(short, long)]
    query: String,

    /// Path to Parquet file
    #[arg(short, long)]
    parquet: PathBuf,

    /// Table name to register the Parquet file as
    #[arg(short, long, default_value = "orders")]
    table: String,

    /// Output path for Substrait plan (.pb file)
    #[arg(short, long)]
    output: PathBuf,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.verbose {
        println!("Initializing DataFusion context...");
    }

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Register Parquet file as a table
    if args.verbose {
        println!("Registering Parquet file: {:?} as table '{}'", args.parquet, args.table);
    }
    ctx.register_parquet(&args.table, args.parquet.to_str().unwrap(), ParquetReadOptions::default())
        .await
        .context("Failed to register Parquet file")?;

    // Parse SQL query
    if args.verbose {
        println!("Parsing SQL query: {}", args.query);
    }
    let df = ctx.sql(&args.query)
        .await
        .context("Failed to parse SQL query")?;

    // Get the optimized logical plan
    let logical_plan = df.into_optimized_plan()
        .context("Failed to optimize logical plan")?;

    if args.verbose {
        println!("\nLogical Plan:");
        println!("{:?}", logical_plan);
    }

    // Convert to Substrait
    if args.verbose {
        println!("\nConverting to Substrait...");
    }
    let substrait_plan = logical_plan::producer::to_substrait_plan(&logical_plan, &ctx)
        .context("Failed to serialize to Substrait")?;

    // Serialize to bytes
    use prost::Message;
    let mut buf = Vec::new();
    substrait_plan.encode(&mut buf)
        .context("Failed to encode Substrait plan")?;

    // Write to file
    fs::write(&args.output, &buf)
        .context("Failed to write Substrait plan to file")?;

    println!("✓ Substrait plan written to: {:?} ({} bytes)", args.output, buf.len());

    Ok(())
}
