# Arrow User-Defined Functions Framework on WebAssembly

## Example

Build the WebAssembly module:

```sh
cargo build --release -p arrow-udf-wasm-example --target wasm32-wasi
```

Run the example:

```sh
cargo run --example wasm -- target/wasm32-wasi/release/arrow_udf_wasm_example.wasm
```

Run the Python example:

```sh
cargo run --example python
```

Build the Python WebAssembly module:

```sh
PYO3_NO_PYTHON=1 cargo build --release -p arrow-udf-python --target wasm32-wasi
mkdir arrow-udf-python/target/wasm32-wasi/wasi-deps/bin
cp target/wasm32-wasi/release/arrow_udf_python.wasm arrow-udf-python/target/wasm32-wasi/wasi-deps/bin/python.wasm
```

Run the Python WebAssembly example:

```sh
cargo run --release --example python_wasm
```
