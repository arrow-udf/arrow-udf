# Arrow User-Defined Functions Framework

[![Crate](https://img.shields.io/crates/v/arrow-udf.svg)](https://crates.io/crates/arrow-udf)
[![Docs](https://docs.rs/arrow-udf/badge.svg)](https://docs.rs/arrow-udf)

Easily create and run user-defined functions (UDF) on Apache Arrow.
You can define functions in Rust, Python or JavaScript, run natively or on WebAssembly.

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

The output batch will contain a column of errors. Error rows will be filled with NULL in the output column,
and the error message will be stored in the error column.

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
let output = sig.function.as_scalar().unwrap()(&input).unwrap();
```

See the [example](./arrow-udf/examples/rust.rs) for more details.

### Define Python Functions and Run Locally

See [documents](./arrow-udf-python/README.md) in `arrow-udf-python`.

### Define JavaScript Functions and Run Locally

See [documents](./arrow-udf-js/README.md) in `arrow-udf-js`.

### Define Rust Functions and Run on WebAssembly

See [documents](./arrow-udf-wasm/README.md) in `arrow-udf-wasm`.

### Define Python Functions and Run on WebAssembly

See [documents](./arrow-udf-python-wasm/README.md) in `arrow-udf-python-wasm`.

## Benchmarks

We have benchmarked the performance of function calls in different environments.
You can run the benchmarks with the following command:

```sh
cargo bench --bench wasm
```

Performance comparison of calling `gcd` on a chunk of 1024 rows:

```
gcd/native          1.4476 µs   x1
gcd/wasm            16.006 µs   x11
gcd/js              82.103 µs   x57
gcd/python          122.52 µs   x85
gcd/python-wasm     2.2475 ms   x1553
```
