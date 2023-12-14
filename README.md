# Arrow User-Defined Functions Framework on WebAssembly

## Example

Build the WebAssembly module:

```sh
cargo build --release -p arrow-udf-wasm-example --target wasm32-wasi
```

Run the example:

```sh
cargo run --example gcd -- target/wasm32-wasi/release/arrow_udf_wasm_example.wasm
```
