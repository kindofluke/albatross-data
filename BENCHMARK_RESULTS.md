# Benchmark Results: DuckDB CPU vs data_kernel GPU

**Date**: 2026-03-31 15:12:59
**Hardware**:
- **CPU**: arm
- **GPU**: Apple M1 Pro (Metal backend, IntegratedGpu)
- **DuckDB**: 1.5.1
- **data_kernel**: Available

## Datasets
- **orders_5m.parquet**: 5M rows (89.2MB)
- **order_items_5m.parquet**: ~27.5M rows (619.1MB)

## Queries

### Orders Table
1. **Q1 - Aggregations**: `SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders`
2. **Q2 - GROUP BY**: `SELECT status, COUNT(*) as cnt, SUM(amount) as total FROM orders GROUP BY status ORDER BY cnt DESC`
3. **Q3 - Filter**: `SELECT * FROM orders WHERE amount > 500 AND quantity > 5 LIMIT 1000`
4. **Q4 - Complex**: `SELECT status, AVG(amount), SUM(quantity) FROM orders WHERE amount > 100 GROUP BY status`

### Order Items Table
5. **Q5 - Aggregations**: `SELECT COUNT(*), SUM(price * quantity), AVG(price) FROM order_items`
6. **Q6 - Top Products**: `SELECT product_id, COUNT(*) as cnt, SUM(quantity) as qty FROM order_items GROUP BY product_id ORDER BY cnt DESC LIMIT 20`
7. **Q7 - Filter**: `SELECT * FROM order_items WHERE price > 100 AND quantity > 5 LIMIT 1000`
8. **Q8 - Revenue**: `SELECT product_id, SUM(price * quantity) as revenue FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 20`

### JOIN Queries
9. **Q9 - JOIN with GROUP BY**: `SELECT o.customer_id, COUNT(*) as order_count, SUM(oi.price * oi.quantity) as total_revenue FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id ORDER BY total_revenue DESC LIMIT 20`
10. **Q10 - JOIN with Aggregations**: `SELECT o.status, COUNT(DISTINCT o.id) as order_count, SUM(oi.quantity) as total_items, AVG(oi.price) as avg_price FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status`

### Window Function Queries
11. **Q11 - Window Rank**: `SELECT customer_id, id, amount, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM orders`
12. **Q12 - Window Row Number**: `SELECT product_id, order_id, price, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY price DESC) as row_num FROM order_items`

## Results

| Query | Dataset | Rows | CPU Time (ms) | GPU Time (ms) | Speedup | Notes |
|-------|---------|------|---------------|---------------|---------|-------|
| Q1 | orders | 5M | 32.31 | 121.23 | 0.27x | CPU faster |
| Q2 | orders | 5M | 15.62 | 42.28 | 0.37x | CPU faster |
| Q3 | orders | 5M | 4.12 | 42.89 | 0.10x | CPU faster |
| Q4 | orders | 5M | 25.16 | 50.93 | 0.49x | CPU faster |
| Q5 | order_items | 27.5M | 122.19 | 46.78 | 2.61x | GPU faster |
| Q6 | order_items | 27.5M | 78.13 | 89.92 | 0.87x | CPU faster |
| Q7 | order_items | 27.5M | 3.73 | 94.17 | 0.04x | CPU faster |
| Q8 | order_items | 27.5M | 44.04 | 85.02 | 0.52x | CPU faster |
| Q9 | orders+items | 5M+27.5M | 242.58 | 425.68 | 0.57x | CPU faster |
| Q10 | orders+items | 5M+27.5M | 381.17 | 623.69 | 0.61x | CPU faster |
| Q11 | orders | 5M | 1967.87 | 6224.69 | 0.32x | CPU faster |
| Q12 | order_items | 27.5M | 16261.60 | 37828.65 | 0.43x | CPU faster |

## Summary

### Performance Analysis
- GPU (via data_kernel with WGPU) vs CPU (DuckDB) speedup varies by query type
- Aggregation-heavy queries may benefit more from GPU acceleration
- Small result sets (LIMIT queries) may have GPU transfer overhead
- The GPU backend is using Metal for compute

### Next Steps
1. Analyze which query patterns benefit most from GPU acceleration
2. Test with larger datasets (10M+ rows) to see GPU benefits scale
3. Profile GPU utilization with Metal/Vulkan tools
