# Arrow User-Defined Functions Framework

[![Crate](https://img.shields.io/crates/v/arrow-udf.svg)](https://crates.io/crates/arrow-udf)
[![Docs](https://docs.rs/arrow-udf/badge.svg)](https://docs.rs/arrow-udf)

Easily create and run user-defined functions (UDF) on Apache Arrow.
You can define functions in Rust or Python, run natively or on WebAssembly.

## Usage

### Define Rust Functions and Run Locally

You can integrate this library into your Rust project to quickly define and use custom functions.

#### Basic Usage

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.1"
```

Define your functions with the `#[function]` macro:

```rust
use arrow_udf::function;

#[function("gcd(int, int) -> int", output = "eval_gcd")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
```

The macro will generate a function that takes a `RecordBatch` as input and returns a `RecordBatch` as output.
The function can be named with the optional `output` parameter.
If not specified, it will be named arbitrarily like `gcd_int4_int4_int4_eval`.

You can then call the generated function on a `RecordBatch`:

```rust
let input: RecordBatch = ...;
let output: RecordBatch = eval_gcd(&input).unwrap();
```

If you print the input and output batch, it will be like this:

```text
 input     output
+----+----+-----+
| a  | b  | gcd |
+----+----+-----+
| 15 | 25 | 5   |
|    | 1  |     |
+----+----+-----+
```

#### Fallible Functions

If your function returns a `Result`:

```rust
#[function("div(int, int) -> int", output = "eval_div")]
fn div(x: i32, y: i32) -> Result<i32, &'static str> {
    x.checked_div(y).ok_or("division by zero")
}
```

The output batch will contain a column of errors if any, and the error rows will be filled with NULL:

```text
 input     output
+----+----+-----+------------------+
| a  | b  | div | error            |
+----+----+-----+------------------+
| 15 | 25 | 0   |                  |
| 5  | 0  |     | division by zero |
+----+----+-----+------------------+
```

#### Function Registry

If you want to lookup functions by signature, you can enable the `global_registry` feature:

```toml
[dependencies]
arrow-udf = { version = "0.1", features = ["global_registry"] }
```

Each function will be registered in a global registry when it is defined.
Then you can lookup functions from the `REGISTRY`:

```rust
use arrow_schema::DataType::Int32;
use arrow_udf::sig::REGISTRY;

let sig = REGISTRY.get("gcd", &[Int32, Int32], &Int32).expect("gcd function");
let output = (sig.function)(&input).unwrap();
```

See the [example](./arrow-udf/examples/rust.rs) for more details.

### Define Python Functions and Run Locally

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-python = "0.1"
```

Define your Python function in a string and create a `Runtime` for each function:

```rust
use arrow_udf_python::Runtime;

let python_code = r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#;
let return_type = arrow_schema::DataType::Int32;
let runtime = Runtime::new("gcd", return_type, python_code).unwrap();
```

You can then call the python function on a `RecordBatch`:

```rust
let input: RecordBatch = ...;
let output = runtime.call(&input).unwrap();
```

The python code will be run in an embedded CPython 3.11 interpreter, powered by [PyO3](pyo3.rs).
Please note that due to the limitation of GIL, only one Python function can be running in a process at the same time.

See the [example](./arrow-udf-python/examples/python.rs) for more details.

### Define Rust Functions and Run on WebAssembly

For untrusted user-defined functions, you can compile them into WebAssembly and run them in a sandboxed environment.

First, create a project and define functions as described in [the above section](#define-rust-functions-and-run-locally).
Then compile the project into WebAssembly:

```sh
cargo build --release --target wasm32-wasi
```

You can find the generated WebAssembly module in `target/wasm32-wasi/release/*.wasm`.

Next, add the following lines to your project:

```toml
[dependencies]
arrow-udf-wasm = "0.1"
```

You can then load the WebAssembly module and call the functions:

```rust
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

The WebAssembly runtime is powered by [wasmtime](https://wasmtime.dev/). 
Notice that each WebAssembly instance can only run single-threaded, we maintain an instance pool internally to support parallel calls from multiple threads.

See the [example](./arrow-udf-wasm/examples/wasm.rs) for more details. To run the example:

```sh
cargo build --release -p arrow-udf-example --target wasm32-wasi
cargo run --example wasm -- target/wasm32-wasi/release/arrow_udf_example.wasm
```

### Define Python Functions and Run on WebAssembly

Similarly, you can run Python functions on WebAssembly.

We don't have a ready-to-use library yet, but you can refer to the following steps to run a simple example.

```sh
# Build the Python WebAssembly module
PYO3_NO_PYTHON=1 cargo build --release -p arrow-udf-python --target wasm32-wasi
mkdir -p arrow-udf-python/target/wasm32-wasi/wasi-deps/bin
cp target/wasm32-wasi/release/arrow_udf_python.wasm arrow-udf-python/target/wasm32-wasi/wasi-deps/bin/python.wasm

# Run the Python WebAssembly example
cargo run --release --example python_wasm
```

## Benchmarks

We have benchmarked the performance of function calls in different environments.
You can run the benchmarks with the following command:

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
