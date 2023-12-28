# Arrow User-Defined Functions Framework

Easily create and run user-defined functions (UDF) on Apache Arrow.
You can define functions in Rust or Python, run natively or on WebAssembly.

## Usage

### Define Rust Functions and Run Locally

You can integrate this library into your Rust project to quickly define and use custom functions.

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.1"
```

Define your functions with the `#[function]` macro:

```rust
use arrow_udf::function;

#[function("gcd(int, int) -> int", output = "gcd_batch")]
fn gcd(x: i32, y: i32) -> i32 {
    while y != 0 {
        let t = y;
        y = x % y;
        x = t;
    }
    x
}
```

The macro will generate a function that takes a `RecordBatch` and returns an `ArrayRef`.
It can be named with the optional `output` parameter, or if not specified, it will be named arbitrarily.

You can then call the generated function on a `RecordBatch`:

```rust
let input: RecordBatch = ...;
let output = gcd_batch(&input).unwrap();
```

Meanwhile, the macro will register each function into a global registry.
You can then lookup the function by name and types:

```rust
use arrow_schema::DataType::Int32;
use arrow_udf::sig::REGISTRY;

// lookup the function from the global registry
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

### Define Python Functions and Run on WebAssembly





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
