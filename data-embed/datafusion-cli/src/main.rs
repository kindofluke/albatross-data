use anyhow::{Context, Result};
use clap::Parser;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan;
use std::fs;
use std::path::PathBuf;

mod manifest;
use manifest::ExecutionManifest;

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

    /// Generate execution manifest (.json) instead of raw Substrait
    #[arg(short, long)]
    manifest: bool,

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

    if args.manifest {
        // Generate execution manifest (JSON with Substrait + file paths)
        if args.verbose {
            println!("\nGenerating execution manifest...");
        }
        
        let mut manifest = ExecutionManifest::new(args.query.clone(), buf.clone());
        
        // Add the Parquet file mapping
        let absolute_path = std::fs::canonicalize(&args.parquet)
            .context("Failed to resolve absolute path for Parquet file")?;
        manifest.add_table(args.table.clone(), absolute_path);
        
        // Serialize manifest to JSON
        let json = serde_json::to_string_pretty(&manifest)
            .context("Failed to serialize manifest to JSON")?;
        
        // Write JSON file
        let json_path = args.output.with_extension("json");
        fs::write(&json_path, json)
            .context("Failed to write manifest to file")?;
        
        println!("✓ Execution manifest written to: {:?}", json_path);
        println!("  - SQL: {}", args.query);
        println!("  - Tables: {}", manifest.tables.len());
        println!("  - Substrait size: {} bytes", buf.len());
    } else {
        // Write raw Substrait bytes
        fs::write(&args.output, &buf)
            .context("Failed to write Substrait plan to file")?;
        
        println!("✓ Substrait plan written to: {:?} ({} bytes)", args.output, buf.len());
    }

    Ok(())
}
