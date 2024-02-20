# Arrow User-Defined Functions Framework

Easily create and run user-defined functions (UDF) on Apache Arrow.
You can define functions in Rust, Python or JavaScript, run natively or on WebAssembly.

| Language   | Native             | WebAssembly             |
| ---------- | ------------------ | ----------------------- |
| Rust       | [arrow-udf]        | [arrow-udf-wasm]        |
| Python     | [arrow-udf-python] | N/A                     |
| JavaScript | [arrow-udf-js]     | N/A                     |

[arrow-udf]: ./arrow-udf
[arrow-udf-python]: ./arrow-udf-python
[arrow-udf-js]: ./arrow-udf-js
[arrow-udf-wasm]: ./arrow-udf-wasm

## Usage

You can integrate this library into your Rust project to quickly define and use custom functions.

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

See [`arrow-udf`](./arrow-udf/README.md) for more details.

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
```
