#!/bin/bash
set -e

# Generate 10M orders and associated order items
cargo run --release --manifest-path data-embed/generate-test-data/Cargo.toml -- \
  --table orders \
  --rows 10000000 \
  --output data/orders.parquet && \
cargo run --release --manifest-path data-embed/generate-test-data/Cargo.toml -- \
  --table order_items \
  --rows 10000000 \
  --output data/order_items.parquet
