use anyhow::Result;
use std::time::Instant;
use std::collections::{HashMap, HashSet};
use rand::Rng;

// Import from executor crate
use executor::wgpu_engine::WgpuEngine;

fn format_duration(ms: f64) -> String {
    if ms < 1000.0 {
        format!("{:.2}ms", ms)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== GPU Aggregation Benchmark (100M rows) ===\n");

    // Initialize GPU engine
    println!("Initializing GPU...");
    let engine = WgpuEngine::new().await?;
    println!("GPU initialized successfully\n");

    let n = 30_000_000;  // 30M rows = 120MB (within 128MB GPU binding limit)
    println!("Dataset: {} rows\n", n);

    // Test 1: Simple SUM aggregation
    println!("=== Test 1: Simple SUM (SELECT SUM(amount) FROM orders) ===");
    let values: Vec<f32> = (0..n).map(|i| ((i % 1000) as f32) * 0.5 + 10.0).collect();

    let start = Instant::now();
    let cpu_sum: f32 = values.iter().sum();
    let cpu_time = start.elapsed();
    println!("CPU: sum={:.2}, time={}", cpu_sum, format_duration(cpu_time.as_secs_f64() * 1000.0));

    let start = Instant::now();
    let gpu_result = engine.execute_global_aggregation(&values).await?;
    let gpu_time = start.elapsed();
    println!("GPU: sum={:.2}, time={}", gpu_result.sum_f32(), format_duration(gpu_time.as_secs_f64() * 1000.0));
    
    let sum_match = (cpu_sum - gpu_result.sum_f32()).abs() / cpu_sum < 0.01;
    println!("Match: {}", if sum_match { "✓" } else { "✗" });
    println!();

    // Test 2: Multiple aggregates (SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount))
    println!("=== Test 2: Multiple Aggregates (COUNT, SUM, AVG, MIN, MAX) ===");
    
    let start = Instant::now();
    let cpu_count = values.len() as u32;
    let cpu_sum: f32 = values.iter().sum();
    let cpu_min = values.iter().copied().fold(f32::INFINITY, f32::min);
    let cpu_max = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let cpu_avg = cpu_sum / cpu_count as f32;
    let cpu_time = start.elapsed();
    
    println!("CPU Results:");
    println!("  COUNT: {}", cpu_count);
    println!("  SUM:   {:.2}", cpu_sum);
    println!("  AVG:   {:.2}", cpu_avg);
    println!("  MIN:   {:.2}", cpu_min);
    println!("  MAX:   {:.2}", cpu_max);
    println!("  Time:  {}", format_duration(cpu_time.as_secs_f64() * 1000.0));

    let start = Instant::now();
    let gpu_result = engine.execute_global_aggregation(&values).await?;
    let gpu_time = start.elapsed();
    
    println!("\nGPU Results:");
    println!("  COUNT: {}", gpu_result.count);
    println!("  SUM:   {:.2}", gpu_result.sum_f32());
    println!("  AVG:   {:.2}", gpu_result.avg());
    println!("  MIN:   {:.2}", gpu_result.min_f32());
    println!("  MAX:   {:.2}", gpu_result.max_f32());
    println!("  Time:  {}", format_duration(gpu_time.as_secs_f64() * 1000.0));
    println!();

    // Test 3: GROUP BY with few groups (SELECT status, COUNT(*), SUM(amount), AVG(amount) GROUP BY status)
    println!("=== Test 3: GROUP BY with 4 groups (status) ===");
    let num_groups: usize = 4;
    let group_keys: Vec<u32> = (0..n).map(|i| (i % num_groups as u32) as u32).collect();

    let start = Instant::now();
    let mut cpu_groups: HashMap<u32, (f32, u32, f32, f32)> = HashMap::new();
    for (i, &val) in values.iter().enumerate() {
        let group_id = group_keys[i];
        let entry = cpu_groups.entry(group_id).or_insert((0.0, 0, f32::INFINITY, f32::NEG_INFINITY));
        entry.0 += val;
        entry.1 += 1;
        entry.2 = entry.2.min(val);
        entry.3 = entry.3.max(val);
    }
    let cpu_time = start.elapsed();
    
    println!("CPU GROUP BY time: {}", format_duration(cpu_time.as_secs_f64() * 1000.0));
    for i in 0..num_groups {
        if let Some(&(sum, count, min, max)) = cpu_groups.get(&(i as u32)) {
            println!("  Group {}: count={}, sum={:.2}, avg={:.2}, min={:.2}, max={:.2}",
                i, count, sum, sum / count as f32, min, max);
        }
    }

    let start = Instant::now();
    let gpu_results = engine.execute_group_by_aggregation(&values, &group_keys, num_groups).await?;
    let gpu_time = start.elapsed();
    
    println!("\nGPU GROUP BY time: {}", format_duration(gpu_time.as_secs_f64() * 1000.0));
    for i in 0..num_groups {
        let r = &gpu_results[i as usize];
        println!("  Group {}: count={}, sum={:.2}, avg={:.2}, min={:.2}, max={:.2}",
            i, r.count, r.sum_f32(), r.avg(), r.min_f32(), r.max_f32());
    }
    
    let speedup = cpu_time.as_secs_f64() / gpu_time.as_secs_f64();
    println!("\nSpeedup: {:.2}x", speedup);
    println!();

    // Test 4: GROUP BY with moderate groups (SELECT customer_id % 100, ... GROUP BY customer_id % 100)
    println!("=== Test 4: GROUP BY with 100 groups (customer buckets) ===");
    let num_groups: usize = 100;
    let group_keys: Vec<u32> = (0..n).map(|i| (i % num_groups as u32) as u32).collect();

    let start = Instant::now();
    let mut cpu_groups: HashMap<u32, (f32, u32, f32, f32)> = HashMap::new();
    for (i, &val) in values.iter().enumerate() {
        let group_id = group_keys[i];
        let entry = cpu_groups.entry(group_id).or_insert((0.0, 0, f32::INFINITY, f32::NEG_INFINITY));
        entry.0 += val;
        entry.1 += 1;
        entry.2 = entry.2.min(val);
        entry.3 = entry.3.max(val);
    }
    let cpu_time = start.elapsed();
    
    println!("CPU GROUP BY time: {}", format_duration(cpu_time.as_secs_f64() * 1000.0));
    println!("Sample groups (first 3):");
    for i in 0..3 {
        if let Some(&(sum, count, min, max)) = cpu_groups.get(&(i as u32)) {
            println!("  Group {}: count={}, sum={:.2}, avg={:.2}, min={:.2}, max={:.2}",
                i, count, sum, sum / count as f32, min, max);
        }
    }

    let start = Instant::now();
    let gpu_results = engine.execute_group_by_aggregation(&values, &group_keys, num_groups).await?;
    let gpu_time = start.elapsed();
    
    println!("\nGPU GROUP BY time: {}", format_duration(gpu_time.as_secs_f64() * 1000.0));
    println!("Sample groups (first 3):");
    for i in 0..3 {
        let r = &gpu_results[i];
        println!("  Group {}: count={}, sum={:.2}, avg={:.2}, min={:.2}, max={:.2}",
            i, r.count, r.sum_f32(), r.avg(), r.min_f32(), r.max_f32());
    }
    
    // Verify accuracy
    let mut max_error = 0.0f32;
    for i in 0..num_groups.min(10) {
        if let Some(&(cpu_sum, _, _, _)) = cpu_groups.get(&(i as u32)) {
            let gpu_sum = gpu_results[i].sum_f32();
            let error = (cpu_sum - gpu_sum).abs() / cpu_sum * 100.0;
            max_error = max_error.max(error);
        }
    }
    println!("\nMax error in first 10 groups: {:.4}%", max_error);
    
    let speedup = cpu_time.as_secs_f64() / gpu_time.as_secs_f64();
    println!("Speedup: {:.2}x", speedup);
    println!();

    // Test 5: GROUP BY with many groups (1000 groups)
    println!("=== Test 5: GROUP BY with 1000 groups (high cardinality) ===");
    let num_groups: usize = 1000;
    let group_keys: Vec<u32> = (0..n).map(|i| (i % num_groups as u32) as u32).collect();

    let start = Instant::now();
    let mut cpu_groups: HashMap<u32, (f32, u32, f32, f32)> = HashMap::new();
    for (i, &val) in values.iter().enumerate() {
        let group_id = group_keys[i];
        let entry = cpu_groups.entry(group_id).or_insert((0.0, 0, f32::INFINITY, f32::NEG_INFINITY));
        entry.0 += val;
        entry.1 += 1;
        entry.2 = entry.2.min(val);
        entry.3 = entry.3.max(val);
    }
    let cpu_time = start.elapsed();
    
    println!("CPU GROUP BY time: {}", format_duration(cpu_time.as_secs_f64() * 1000.0));

    let start = Instant::now();
    let gpu_results = engine.execute_group_by_aggregation(&values, &group_keys, num_groups).await?;
    let gpu_time = start.elapsed();
    
    println!("GPU GROUP BY time: {}", format_duration(gpu_time.as_secs_f64() * 1000.0));
    
    // Verify accuracy
    let mut max_error = 0.0f32;
    for i in 0..num_groups.min(100) {
        if let Some(&(cpu_sum, _, _, _)) = cpu_groups.get(&(i as u32)) {
            let gpu_sum = gpu_results[i].sum_f32();
            let error = (cpu_sum - gpu_sum).abs() / cpu_sum * 100.0;
            max_error = max_error.max(error);
        }
    }
    println!("Max error in first 100 groups: {:.4}%", max_error);
    
    let speedup = cpu_time.as_secs_f64() / gpu_time.as_secs_f64();
    println!("Speedup: {:.2}x", speedup);
    println!();

    // Test 6: Hash Join with Aggregation
    println!("=== Test 6: Hash Join + Aggregation (orders JOIN order_items) ===");
    
    // Simulate: SELECT SUM(oi.price) FROM orders o JOIN order_items oi ON o.id = oi.order_id
    // Build side: 1M orders (smaller)
    // Probe side: 5M order_items (larger, ~5 items per order on average)
    let num_orders = 1_000_000;
    let num_items = 5_000_000;
    
    println!("Build side: {} orders", num_orders);
    println!("Probe side: {} order_items", num_items);
    
    // Generate order IDs (build side)
    let order_ids: Vec<i32> = (0..num_orders as i32).collect();
    
    // Generate order_items (probe side) - each references a random order
    let mut rng = rand::thread_rng();
    let item_order_ids: Vec<i32> = (0..num_items)
        .map(|_| rng.gen_range(0..num_orders as i32))
        .collect();
    let item_prices: Vec<f32> = (0..num_items)
        .map(|_| rng.gen_range(5.0..500.0))
        .collect();
    
    // CPU: Hash join with aggregation
    let start = Instant::now();
    let mut order_set = HashSet::new();
    for &order_id in &order_ids {
        order_set.insert(order_id);
    }
    
    let mut cpu_sum = 0.0f32;
    let mut cpu_count = 0u32;
    let mut cpu_min = f32::MAX;
    let mut cpu_max = f32::MIN;
    
    for i in 0..num_items {
        if order_set.contains(&item_order_ids[i]) {
            let price = item_prices[i];
            cpu_sum += price;
            cpu_count += 1;
            cpu_min = cpu_min.min(price);
            cpu_max = cpu_max.max(price);
        }
    }
    let cpu_time = start.elapsed();
    
    println!("CPU: {} matches, sum={:.2}, avg={:.2}, min={:.2}, max={:.2} in {}",
             cpu_count, cpu_sum, cpu_sum / cpu_count as f32, cpu_min, cpu_max,
             format_duration(cpu_time.as_secs_f64() * 1000.0));
    
    // GPU: Hash join with aggregation
    let start = Instant::now();
    let gpu_result = engine.execute_hash_join_aggregate(&order_ids, &item_order_ids, &item_prices).await?;
    let gpu_time = start.elapsed();
    
    let gpu_sum = gpu_result.sum_f32();
    let gpu_count = gpu_result.count;
    let gpu_min = gpu_result.min_f32();
    let gpu_max = gpu_result.max_f32();
    
    println!("GPU: {} matches, sum={:.2}, avg={:.2}, min={:.2}, max={:.2} in {}",
             gpu_count, gpu_sum, gpu_sum / gpu_count as f32, gpu_min, gpu_max,
             format_duration(gpu_time.as_secs_f64() * 1000.0));
    
    // Verify accuracy
    let count_match = cpu_count == gpu_count;
    let sum_error = (cpu_sum - gpu_sum).abs() / cpu_sum * 100.0;
    let avg_error = ((cpu_sum / cpu_count as f32) - (gpu_sum / gpu_count as f32)).abs() / (cpu_sum / cpu_count as f32) * 100.0;
    
    println!("Accuracy: count_match={}, sum_error={:.4}%, avg_error={:.4}%",
             count_match, sum_error, avg_error);
    
    let speedup = cpu_time.as_secs_f64() / gpu_time.as_secs_f64();
    println!("Speedup: {:.2}x", speedup);
    println!();

    println!("=== Benchmark Complete ===");
    println!("\nSummary:");
    println!("- Dataset: 30M rows (GPU binding limit: 128MB)");
    println!("- GPU: Functional for GROUP BY aggregations and hash joins");
    println!("- Best use case: GROUP BY with moderate cardinality (100-1000 groups)");
    println!("- Hash joins: Efficient for inner joins with single key");
    println!("- Limitation: Global aggregation has atomic contention issues");
    println!("- Note: For 100M+ rows, chunked processing would be needed");

    Ok(())
}
