# WebAssembly UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf-wasm.svg)](https://crates.io/crates/arrow-udf-wasm)
[![Docs](https://docs.rs/arrow-udf-wasm/badge.svg)](https://docs.rs/arrow-udf-wasm)

For untrusted user-defined functions, you can compile them into WebAssembly and run them in a sandboxed environment.

## Build UDF in WebAssembly

Create a project and add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.1"
```

Define your functions with the `#[function]` macro:

```rust,ignore
use arrow_udf::function;

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
```

Then compile the project into WebAssembly:

```sh
cargo build --release --target wasm32-wasi
```

You can find the generated WebAssembly module in `target/wasm32-wasi/release/*.wasm`.

## Run UDF in WebAssembly

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-wasm = "0.1"
```

You can then load the WebAssembly module and call the functions:

```rust,ignore
use arrow_udf_wasm::Runtime;

// load the WebAssembly module
let binary = std::fs::read("udf.wasm").unwrap();
// create a runtime from the module
let runtime = Runtime::new(&binary).unwrap();
// list available functions in the module:
for name in runtime.functions() {
    println!("{}", name);
}
// call the function with a RecordBatch
let input: RecordBatch = ...;
let output = runtime.call("gcd(int4,int4)->int4", &input).unwrap();
```

The WebAssembly runtime is powered by [Wasmtime](https://wasmtime.dev/). 
Notice that each WebAssembly instance can only run single-threaded, we maintain an instance pool internally to support parallel calls from multiple threads.

See the [example](./examples/wasm.rs) for more details. To run the example:

```sh
cargo build --release -p arrow-udf-example --target wasm32-wasi
cargo run --example wasm -- target/wasm32-wasi/release/arrow_udf_example.wasm
```

## Build WASM UDF at Runtime

Enable the `build` feature to build the wasm binary from source:

```toml
[dependencies]
arrow-udf-wasm = { version = "0.1", features = ["build"] }
```

You can then build the WebAssembly module at runtime:

```rust,ignore
let manifest = r#"
[dependencies]
chrono = "0.4"
"#;

let script = r#"
use arrow_udf::function;

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
"#;
let binary = arrow_udf_wasm::build::build(manifest, script).unwrap();
```

See the [`build`] module for more details.
