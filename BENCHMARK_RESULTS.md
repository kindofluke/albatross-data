# Benchmark Results: DuckDB CPU vs data_kernel GPU

**Date**: 2026-03-31 15:47:45
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
| Q1 | orders | 5M | 55.48 | 87.52 | 0.63x | CPU faster |
| Q2 | orders | 5M | 51.09 | 54.79 | 0.93x | CPU faster |
| Q3 | orders | 5M | 4.50 | 80.56 | 0.06x | CPU faster |
| Q4 | orders | 5M | 30.94 | 139.38 | 0.22x | CPU faster |
| Q5 | order_items | 27.5M | 277.45 | 65.75 | 4.22x | GPU faster |
| Q6 | order_items | 27.5M | 98.12 | 76.33 | 1.29x | GPU faster |
| Q7 | order_items | 27.5M | 3.91 | 193.28 | 0.02x | CPU faster |
| Q8 | order_items | 27.5M | 52.62 | 107.74 | 0.49x | CPU faster |
| Q9 | orders+items | 5M+27.5M | 316.99 | 471.01 | 0.67x | CPU faster |
| Q10 | orders+items | 5M+27.5M | 664.03 | 918.76 | 0.72x | CPU faster |
| Q11 | orders | 5M | 2099.82 | 451.05 | 4.66x | GPU faster |
| Q12 | order_items | 27.5M | 12355.80 | 1495.05 | 8.26x | GPU faster |

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
