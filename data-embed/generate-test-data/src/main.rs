use anyhow::Result;
use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "generate-test-data")]
#[command(about = "Generate test Parquet data for benchmarking", long_about = None)]
struct Args {
    /// Number of rows to generate
    #[arg(short, long, default_value = "10000")]
    rows: usize,

    /// Output path for Parquet file
    #[arg(short, long, default_value = "data/orders.parquet")]
    output: PathBuf,

    /// Batch size for writing (rows per batch)
    #[arg(short, long, default_value = "1000000")]
    batch_size: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Generating {} rows of test data...", args.rows);
    println!("Output: {:?}", args.output);
    println!("Batch size: {} rows", args.batch_size);

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    // Ensure output directory exists
    if let Some(parent) = args.output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write to Parquet with batching
    let file = File::create(&args.output)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows_written = 0;
    let mut rng = rand::thread_rng();
    let statuses = ["pending", "shipped", "delivered", "cancelled"];

    while rows_written < args.rows {
        let batch_rows = std::cmp::min(args.batch_size, args.rows - rows_written);
        
        // Generate batch data
        let ids: Vec<i64> = (rows_written as i64..(rows_written + batch_rows) as i64).collect();
        let customer_ids: Vec<i64> = (0..batch_rows).map(|_| rng.gen_range(1..1000)).collect();
        let amounts: Vec<f64> = (0..batch_rows).map(|_| rng.gen_range(10.0..1000.0)).collect();
        let quantities: Vec<i32> = (0..batch_rows).map(|_| rng.gen_range(1..20)).collect();
        let status_data: Vec<&str> = (0..batch_rows)
            .map(|_| statuses[rng.gen_range(0..statuses.len())])
            .collect();

        // Create arrays
        let id_array = Int64Array::from(ids);
        let customer_id_array = Int64Array::from(customer_ids);
        let amount_array = Float64Array::from(amounts);
        let quantity_array = Int32Array::from(quantities);
        let status_array = StringArray::from(status_data);

        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(customer_id_array),
                Arc::new(amount_array),
                Arc::new(quantity_array),
                Arc::new(status_array),
            ],
        )?;

        writer.write(&batch)?;
        rows_written += batch_rows;

        if rows_written % 1_000_000 == 0 || rows_written == args.rows {
            println!("  Progress: {}/{} rows ({:.1}%)", 
                rows_written, args.rows, 
                (rows_written as f64 / args.rows as f64) * 100.0);
        }
    }

    writer.close()?;

    println!("✓ Generated {} rows", args.rows);
    println!("✓ Written to: {:?}", args.output);
    
    Ok(())
}
