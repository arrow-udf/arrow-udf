# All-in-one Benchmark for Arrow UDFs

## How to run

1. Build the WebAssembly UDF example:

```bash
rustup target add wasm32-wasip1
cargo build --release -p arrow-udf-example --target wasm32-wasip1
```

2. Run the benchmarks:

```bash
# Run all benchmarks
cargo bench --bench bench

# Run a specific benchmark
cargo bench --bench bench js
```