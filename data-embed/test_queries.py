"""
10 diverse test queries for GPU vs CPU benchmarking.
DuckDB uses 'table.parquet', data_kernel uses 'table' (no extension).
"""

queries = [
    {
        "name": "Q1: Multi-Aggregation",
        "description": "Multiple aggregations on large table",
        "duckdb_sql": "SELECT COUNT(*), SUM(price * quantity), AVG(price), MIN(quantity), MAX(quantity) FROM 'order_items.parquet'",
        "data_kernel_sql": "SELECT COUNT(*), SUM(price * quantity), AVG(price), MIN(quantity), MAX(quantity) FROM order_items"
    },
    {
        "name": "Q2: Complex Filter + Aggregation",
        "description": "Filter with multiple conditions then aggregate",
        "duckdb_sql": "SELECT status, COUNT(*) as cnt, AVG(amount) as avg_amt FROM 'orders.parquet' WHERE amount > 100 AND quantity >= 3 GROUP BY status",
        "data_kernel_sql": "SELECT status, COUNT(*) as cnt, AVG(amount) as avg_amt FROM orders WHERE amount > 100 AND quantity >= 3 GROUP BY status"
    },
    {
        "name": "Q3: Top-K with GROUP BY",
        "description": "Group by with ORDER BY and LIMIT",
        "duckdb_sql": "SELECT product_id, SUM(price * quantity) as revenue, COUNT(*) as order_count FROM 'order_items.parquet' GROUP BY product_id ORDER BY revenue DESC LIMIT 100",
        "data_kernel_sql": "SELECT product_id, SUM(price * quantity) as revenue, COUNT(*) as order_count FROM order_items GROUP BY product_id ORDER BY revenue DESC LIMIT 100"
    },
    {
        "name": "Q4: Multi-Column GROUP BY",
        "description": "Group by multiple columns with aggregations",
        "duckdb_sql": "SELECT customer_id, status, COUNT(*) as orders, SUM(amount) as total, AVG(quantity) as avg_qty FROM 'orders.parquet' GROUP BY customer_id, status",
        "data_kernel_sql": "SELECT customer_id, status, COUNT(*) as orders, SUM(amount) as total, AVG(quantity) as avg_qty FROM orders GROUP BY customer_id, status"
    },
    {
        "name": "Q5: INNER JOIN with Aggregation",
        "description": "Join two tables and aggregate results",
        "duckdb_sql": "SELECT o.customer_id, COUNT(DISTINCT o.id) as order_count, SUM(oi.price * oi.quantity) as total_spent FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id GROUP BY o.customer_id",
        "data_kernel_sql": "SELECT o.customer_id, COUNT(DISTINCT o.id) as order_count, SUM(oi.price * oi.quantity) as total_spent FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.customer_id"
    },
    {
        "name": "Q6: JOIN with Filter + GROUP BY",
        "description": "Filtered join with grouping",
        "duckdb_sql": "SELECT oi.product_id, o.status, COUNT(*) as cnt, AVG(oi.price) as avg_price FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id WHERE o.amount > 200 GROUP BY oi.product_id, o.status",
        "data_kernel_sql": "SELECT oi.product_id, o.status, COUNT(*) as cnt, AVG(oi.price) as avg_price FROM orders o JOIN order_items oi ON o.id = oi.order_id WHERE o.amount > 200 GROUP BY oi.product_id, o.status"
    },
    {
        "name": "Q7: Window RANK Function",
        "description": "Rank customers by order amount",
        "duckdb_sql": "SELECT customer_id, id, amount, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM 'orders.parquet'",
        "data_kernel_sql": "SELECT customer_id, id, amount, RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank FROM orders"
    },
    {
        "name": "Q8: Window ROW_NUMBER Function",
        "description": "Number products by price within each order",
        "duckdb_sql": "SELECT order_id, product_id, price, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY price DESC) as row_num FROM 'order_items.parquet'",
        "data_kernel_sql": "SELECT order_id, product_id, price, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY price DESC) as row_num FROM order_items"
    },
    {
        "name": "Q9: JOIN with Window Function",
        "description": "Join tables then apply window function",
        "duckdb_sql": "SELECT o.customer_id, oi.product_id, oi.price, RANK() OVER (PARTITION BY o.customer_id ORDER BY oi.price DESC) as price_rank FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id",
        "data_kernel_sql": "SELECT o.customer_id, oi.product_id, oi.price, RANK() OVER (PARTITION BY o.customer_id ORDER BY oi.price DESC) as price_rank FROM orders o JOIN order_items oi ON o.id = oi.order_id"
    },
    {
        "name": "Q10: Complex Multi-Join Aggregation",
        "description": "Join with multiple aggregations and ordering",
        "duckdb_sql": "SELECT o.status, COUNT(DISTINCT o.customer_id) as unique_customers, COUNT(DISTINCT oi.product_id) as unique_products, SUM(oi.quantity) as total_items, AVG(o.amount) as avg_order_value FROM 'orders.parquet' o JOIN 'order_items.parquet' oi ON o.id = oi.order_id GROUP BY o.status ORDER BY total_items DESC",
        "data_kernel_sql": "SELECT o.status, COUNT(DISTINCT o.customer_id) as unique_customers, COUNT(DISTINCT oi.product_id) as unique_products, SUM(oi.quantity) as total_items, AVG(o.amount) as avg_order_value FROM orders o JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status ORDER BY total_items DESC"
    }
]

if __name__ == "__main__":
    print(f"Defined {len(queries)} query pairs for benchmarking.\n")
    for i, q in enumerate(queries, 1):
        print(f"{i}. {q['name']}")
        print(f"   {q['description']}")
        print(f"   DuckDB: {q['duckdb_sql'][:80]}...")
        print()
