# Arrow User-Defined Functions Framework on WebAssembly

## Example

Build the WebAssembly module:

```sh
cd arrow-udf-wasm-example
cargo build --release
cd ..
```

Run the example:

```sh
cargo run --example gcd -- target/wasm32-wasi/release/arrow_udf_wasm_example.wasm
```
