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
    /// Table to generate: "orders" or "order_items"
    #[arg(short, long, default_value = "orders")]
    table: String,

    /// Number of rows to generate (for orders) or number of orders (for order_items)
    #[arg(short = 'n', long, default_value = "10000")]
    rows: usize,

    /// Output path for Parquet file
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Batch size for writing (rows per batch)
    #[arg(short, long, default_value = "1000000")]
    batch_size: usize,
}

fn generate_orders(output: &PathBuf, num_orders: usize, batch_size: usize) -> Result<()> {
    println!("Generating {} orders...", num_orders);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(output)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut rows_written = 0;
    let mut rng = rand::thread_rng();
    let statuses = ["pending", "shipped", "delivered", "cancelled"];

    while rows_written < num_orders {
        let batch_rows = std::cmp::min(batch_size, num_orders - rows_written);
        
        let ids: Vec<i64> = (rows_written as i64..(rows_written + batch_rows) as i64).collect();
        let customer_ids: Vec<i64> = (0..batch_rows).map(|_| rng.gen_range(1..1000)).collect();
        let amounts: Vec<f64> = (0..batch_rows).map(|_| rng.gen_range(10.0..1000.0)).collect();
        let quantities: Vec<i32> = (0..batch_rows).map(|_| rng.gen_range(1..20)).collect();
        let status_data: Vec<&str> = (0..batch_rows)
            .map(|_| statuses[rng.gen_range(0..statuses.len())])
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(customer_ids)),
                Arc::new(Float64Array::from(amounts)),
                Arc::new(Int32Array::from(quantities)),
                Arc::new(StringArray::from(status_data)),
            ],
        )?;

        writer.write(&batch)?;
        rows_written += batch_rows;

        if rows_written % 1_000_000 == 0 || rows_written == num_orders {
            println!("  Progress: {}/{} rows ({:.1}%)", 
                rows_written, num_orders, 
                (rows_written as f64 / num_orders as f64) * 100.0);
        }
    }

    writer.close()?;
    println!("✓ Generated {} orders", num_orders);
    Ok(())
}

fn generate_order_items(output: &PathBuf, num_orders: usize, batch_size: usize) -> Result<()> {
    println!("Generating order_items for {} orders (1-10 items per order)...", num_orders);
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("price", DataType::Float64, false),
    ]));

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(output)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut item_id = 0i64;
    let mut total_items = 0usize;
    let mut rng = rand::thread_rng();
    
    let mut current_batch_ids = Vec::new();
    let mut current_batch_order_ids = Vec::new();
    let mut current_batch_product_ids = Vec::new();
    let mut current_batch_quantities = Vec::new();
    let mut current_batch_prices = Vec::new();

    for order_id in 0..num_orders as i64 {
        let items_per_order = rng.gen_range(1..=10);
        
        for _ in 0..items_per_order {
            current_batch_ids.push(item_id);
            current_batch_order_ids.push(order_id);
            current_batch_product_ids.push(rng.gen_range(1..10000));
            current_batch_quantities.push(rng.gen_range(1..=10));
            current_batch_prices.push(rng.gen_range(5.0..500.0));
            
            item_id += 1;
            total_items += 1;
            
            if current_batch_ids.len() >= batch_size {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(current_batch_ids.clone())),
                        Arc::new(Int64Array::from(current_batch_order_ids.clone())),
                        Arc::new(Int64Array::from(current_batch_product_ids.clone())),
                        Arc::new(Int32Array::from(current_batch_quantities.clone())),
                        Arc::new(Float64Array::from(current_batch_prices.clone())),
                    ],
                )?;
                writer.write(&batch)?;
                
                current_batch_ids.clear();
                current_batch_order_ids.clear();
                current_batch_product_ids.clear();
                current_batch_quantities.clear();
                current_batch_prices.clear();
                
                if total_items % 1_000_000 == 0 {
                    println!("  Progress: {} items generated", total_items);
                }
            }
        }
    }
    
    // Write remaining items
    if !current_batch_ids.is_empty() {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(current_batch_ids)),
                Arc::new(Int64Array::from(current_batch_order_ids)),
                Arc::new(Int64Array::from(current_batch_product_ids)),
                Arc::new(Int32Array::from(current_batch_quantities)),
                Arc::new(Float64Array::from(current_batch_prices)),
            ],
        )?;
        writer.write(&batch)?;
    }

    writer.close()?;
    println!("✓ Generated {} order_items", total_items);
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let output = args.output.unwrap_or_else(|| {
        PathBuf::from(format!("data/{}.parquet", args.table))
    });

    println!("Table: {}", args.table);
    println!("Output: {:?}", output);
    println!("Batch size: {} rows", args.batch_size);
    println!();

    match args.table.as_str() {
        "orders" => generate_orders(&output, args.rows, args.batch_size)?,
        "order_items" => generate_order_items(&output, args.rows, args.batch_size)?,
        _ => {
            eprintln!("Error: Unknown table '{}'. Use 'orders' or 'order_items'", args.table);
            std::process::exit(1);
        }
    }

    println!("✓ Written to: {:?}", output);
    Ok(())
}
