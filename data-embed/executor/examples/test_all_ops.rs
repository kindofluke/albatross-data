use executor::wgpu_engine::WgpuEngine;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Testing All GPU Operations ===\n");

    let engine = WgpuEngine::new().await?;
    println!("✓ GPU initialized\n");

    // Test 1: ROW_NUMBER
    println!("Test 1: ROW_NUMBER (1000 rows)");
    let row_nums = engine.execute_window_row_number(1000).await?;
    let correct = row_nums.len() == 1000
        && row_nums[0] == 1
        && row_nums[999] == 1000;
    println!("  Result: {}", if correct { "✓ PASS" } else { "✗ FAIL" });
    println!("  First: {}, Last: {}\n", row_nums[0], row_nums[999]);

    // Test 2: RANK detection
    println!("Test 2: RANK detection");
    let sorted_keys = vec![1, 1, 2, 2, 2, 3];
    let rank_result = engine.execute_window_rank_detection(&sorted_keys).await?;
    println!("  Input:  {:?}", sorted_keys);
    println!("  Output: {:?}", rank_result);
    let expected = vec![1, 0, 1, 0, 0, 1];
    let correct = rank_result == expected;
    println!("  Result: {}\n", if correct { "✓ PASS" } else { "✗ FAIL" });

    // Test 3: Hash Join Aggregate
    println!("Test 3: Hash Join Aggregate");
    let build_keys = vec![1, 2, 3];
    let probe_keys = vec![2, 3, 3, 5];
    let probe_values = vec![10.0, 20.0, 30.0, 40.0];
    let result = engine.execute_hash_join_aggregate(&build_keys, &probe_keys, &probe_values).await?;
    println!("  Build keys: {:?}", build_keys);
    println!("  Probe keys: {:?}, values: {:?}", probe_keys, probe_values);
    println!("  Matches: keys 2,3,3 -> values 10.0, 20.0, 30.0");
    println!("  Expected: sum=60.0, count=3");
    println!("  GPU Result: sum={:.1}, count={}", result.sum_f32(), result.count);
    let correct = (result.sum_f32() - 60.0).abs() < 0.1 && result.count == 3;
    println!("  Result: {}\n", if correct { "✓ PASS" } else { "✗ FAIL" });

    // Test 4: Small aggregation test
    println!("Test 4: Global Aggregation (10,000 elements)");
    let values: Vec<f32> = (0..10000).map(|i| (i % 100) as f32).collect();
    let result = engine.execute_global_aggregation(&values).await?;
    println!("  COUNT: {}", result.count);
    println!("  SUM:   {:.1}", result.sum_f32());
    println!("  AVG:   {:.1}", result.avg());
    println!("  MIN:   {:.1}", result.min_f32());
    println!("  MAX:   {:.1}", result.max_f32());
    let correct = result.count == 10000
        && result.min_f32() == 0.0
        && result.max_f32() == 99.0;
    println!("  Result: {}\n", if correct { "✓ PASS" } else { "✗ FAIL" });

    // Test 5: GROUP BY with 10 groups
    println!("Test 5: GROUP BY Aggregation (10,000 rows, 10 groups)");
    let values: Vec<f32> = (0..10000).map(|i| (i % 100) as f32).collect();
    let group_keys: Vec<u32> = (0..10000).map(|i| (i % 10) as u32).collect();
    let results = engine.execute_group_by_aggregation(&values, &group_keys, 10).await?;
    println!("  Sample Group 0: count={}, sum={:.1}, avg={:.1}",
        results[0].count, results[0].sum_f32(), results[0].avg());
    let correct = results.len() == 10 && results[0].count == 1000;
    println!("  Result: {}\n", if correct { "✓ PASS" } else { "✗ FAIL" });

    println!("=== All Tests Complete ===");

    Ok(())
}
