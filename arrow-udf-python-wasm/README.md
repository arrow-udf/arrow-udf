# Python UDF on WebAssembly for Apache Arrow

We don't have a ready-to-use library yet, but you can refer to the following steps to run a simple example.

```sh
# Build the Python WebAssembly module
PYO3_NO_PYTHON=1 cargo build --release -p arrow-udf-python --target wasm32-wasi
mkdir -p arrow-udf-python/target/wasm32-wasi/wasi-deps/bin
cp target/wasm32-wasi/release/arrow_udf_python.wasm arrow-udf-python/target/wasm32-wasi/wasi-deps/bin/python.wasm

# Run the Python WebAssembly example
cargo run --release --example python_wasm
```
