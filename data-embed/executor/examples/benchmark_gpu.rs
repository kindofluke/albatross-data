use anyhow::Result;
use std::time::Instant;
use std::collections::HashMap;

// Import from executor crate
use executor::wgpu_engine::WgpuEngine;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== GPU Aggregation Benchmark ===\n");

    // Initialize GPU engine
    println!("Initializing GPU...");
    let engine = WgpuEngine::new().await?;
    println!("GPU initialized successfully\n");

    // Test 1: Global aggregation
    println!("=== Test 1: Global Aggregation (10M elements) ===");
    let n = 10_000_000;
    let values: Vec<f32> = (0..n).map(|i| (i % 1000) as f32 + 0.5).collect();

    // CPU baseline
    let start = Instant::now();
    let cpu_sum: f32 = values.iter().sum();
    let cpu_count = values.len() as u32;
    let cpu_min = values.iter().copied().fold(f32::INFINITY, f32::min);
    let cpu_max = values.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let cpu_avg = cpu_sum / cpu_count as f32;
    let cpu_time = start.elapsed();

    println!("CPU Results:");
    println!("  SUM:   {:.2}", cpu_sum);
    println!("  COUNT: {}", cpu_count);
    println!("  AVG:   {:.2}", cpu_avg);
    println!("  MIN:   {:.2}", cpu_min);
    println!("  MAX:   {:.2}", cpu_max);
    println!("  Time:  {:?}", cpu_time);

    // GPU execution
    let start = Instant::now();
    let gpu_result = engine.execute_global_aggregation(&values).await?;
    let gpu_time = start.elapsed();

    println!("\nGPU Results:");
    println!("  SUM:   {:.2}", gpu_result.sum_f32());
    println!("  COUNT: {}", gpu_result.count);
    println!("  AVG:   {:.2}", gpu_result.avg());
    println!("  MIN:   {:.2}", gpu_result.min_f32());
    println!("  MAX:   {:.2}", gpu_result.max_f32());
    println!("  Time:  {:?}", gpu_time);

    // Verify results match (with tolerance for floating point)
    let sum_match = (cpu_sum - gpu_result.sum_f32()).abs() < 1.0;
    let count_match = cpu_count == gpu_result.count;
    let min_match = (cpu_min - gpu_result.min_f32()).abs() < 0.01;
    let max_match = (cpu_max - gpu_result.max_f32()).abs() < 0.01;

    println!("\nVerification:");
    println!("  SUM match:   {}", if sum_match { "✓" } else { "✗" });
    println!("  COUNT match: {}", if count_match { "✓" } else { "✗" });
    println!("  MIN match:   {}", if min_match { "✓" } else { "✗" });
    println!("  MAX match:   {}", if max_match { "✓" } else { "✗" });

    let speedup = cpu_time.as_secs_f64() / gpu_time.as_secs_f64();
    println!("\nSpeedup: {:.2}x", speedup);

    // Test 2: GROUP BY aggregation
    println!("\n=== Test 2: GROUP BY Aggregation (10M rows, 100 groups) ===");
    let num_groups = 100;
    let group_keys: Vec<u32> = (0..n).map(|i| (i % num_groups) as u32).collect();

    // CPU baseline
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
    let cpu_group_time = start.elapsed();

    println!("CPU GROUP BY time: {:?}", cpu_group_time);
    println!("Sample groups (first 3):");
    for i in 0..3 {
        if let Some(&(sum, count, min, max)) = cpu_groups.get(&i) {
            println!("  Group {}: sum={:.2}, count={}, avg={:.2}, min={:.2}, max={:.2}",
                i, sum, count, sum / count as f32, min, max);
        }
    }

    // GPU execution
    let start = Instant::now();
    let gpu_results = engine.execute_group_by_aggregation(&values, &group_keys, num_groups).await?;
    let gpu_group_time = start.elapsed();

    println!("\nGPU GROUP BY time: {:?}", gpu_group_time);
    println!("Sample groups (first 3):");
    for i in 0..3 {
        let r = &gpu_results[i];
        println!("  Group {}: sum={:.2}, count={}, avg={:.2}, min={:.2}, max={:.2}",
            i, r.sum_f32(), r.count, r.avg(), r.min_f32(), r.max_f32());
    }

    // Verify a few groups
    let mut all_match = true;
    for i in 0..10 {
        if let Some(&(cpu_sum, cpu_count, cpu_min, cpu_max)) = cpu_groups.get(&i) {
            let gpu = &gpu_results[i as usize];
            let sum_ok = (cpu_sum - gpu.sum_f32()).abs() < 1.0;
            let count_ok = cpu_count == gpu.count;
            let min_ok = (cpu_min - gpu.min_f32()).abs() < 0.01;
            let max_ok = (cpu_max - gpu.max_f32()).abs() < 0.01;
            
            if !sum_ok || !count_ok || !min_ok || !max_ok {
                all_match = false;
                println!("Group {} mismatch!", i);
            }
        }
    }

    println!("\nVerification: {}", if all_match { "✓ All groups match" } else { "✗ Some groups don't match" });

    let group_speedup = cpu_group_time.as_secs_f64() / gpu_group_time.as_secs_f64();
    println!("Speedup: {:.2}x", group_speedup);

    println!("\n=== Benchmark Complete ===");

    Ok(())
}
