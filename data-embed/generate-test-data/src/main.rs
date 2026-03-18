use anyhow::Result;
use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::fs::File;
use std::sync::Arc;

fn main() -> Result<()> {
    let num_rows = 10_000;
    let output_path = "data/orders.parquet";

    println!("Generating {} rows of test data...", num_rows);

    // Create schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("status", DataType::Utf8, false),
    ]);

    let mut rng = rand::thread_rng();

    // Generate data
    let ids: Vec<i64> = (0..num_rows).collect();
    let customer_ids: Vec<i64> = (0..num_rows).map(|_| rng.gen_range(1..1000)).collect();
    let amounts: Vec<f64> = (0..num_rows).map(|_| rng.gen_range(10.0..1000.0)).collect();
    let quantities: Vec<i32> = (0..num_rows).map(|_| rng.gen_range(1..20)).collect();
    
    let statuses = ["pending", "shipped", "delivered", "cancelled"];
    let status_data: Vec<&str> = (0..num_rows)
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
        Arc::new(schema.clone()),
        vec![
            Arc::new(id_array),
            Arc::new(customer_id_array),
            Arc::new(amount_array),
            Arc::new(quantity_array),
            Arc::new(status_array),
        ],
    )?;

    // Ensure data directory exists
    std::fs::create_dir_all("data")?;

    // Write to Parquet
    let file = File::create(output_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
    
    writer.write(&batch)?;
    writer.close()?;

    println!("✓ Generated {} rows", num_rows);
    println!("✓ Written to: {}", output_path);
    
    Ok(())
}
