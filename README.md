# Arrow User-Defined Functions Framework on WebAssembly

## Example

Build the WebAssembly module:

```sh
cargo build --release -p arrow-udf-example --target wasm32-wasi
```

Run the example:

```sh
cargo run --example wasm -- target/wasm32-wasi/release/arrow_udf_example.wasm
```

Run the Python example:

```sh
cargo run --example python
```

Build the Python WebAssembly module:

```sh
PYO3_NO_PYTHON=1 cargo build --release -p arrow-udf-python --target wasm32-wasi
mkdir -p arrow-udf-python/target/wasm32-wasi/wasi-deps/bin
cp target/wasm32-wasi/release/arrow_udf_python.wasm arrow-udf-python/target/wasm32-wasi/wasi-deps/bin/python.wasm
```

Run the Python WebAssembly example:

```sh
cargo run --release --example python_wasm
```

Run microbenchmark:

```sh
cargo bench --bench wasm
```

Performance comparison of calling `gcd` on a chunk of 1024 rows:

```
gcd/native          1.4020 µs   x1
gcd/wasm            18.352 µs   x13
gcd/python          126.22 µs   x90
gcd/python-wasm     3.9261 ms   x2800
```
