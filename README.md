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
[arrow-udf-js-deno]: ./arrow-udf-js-deno
[arrow-udf-wasm]: ./arrow-udf-wasm

## Usage

You can integrate this library into your Rust project to quickly define and use custom functions.

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.2"
```

Define your functions with the `#[function]` macro:

```rust
use arrow_udf::function;

#[function("gcd(int32, int32) -> int32", output = "eval_gcd")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}
```

The macro will generate a function that takes a `RecordBatch` as input and returns a `RecordBatch` as output.
The function can be named with the optional `output` parameter.
If not specified, it will be named arbitrarily like `gcd_int32_int32_int32_eval`.

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

## Extension Types

In addition to the standard types defined by Arrow, these crates also support the following data types through Arrow's [extension type](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types). When using extension types, you need to add the `ARROW:extension:name` key to the field's metadata.

| Extension Type | Physical Type | Metadata                                    |
| -------------- | ------------- | ------------------------------------------- |
| JSON           | Utf8          | `ARROW:extension:name` = `arrowudf.json`    |
| Decimal        | Utf8          | `ARROW:extension:name` = `arrowudf.decimal` |

### JSON Type

JSON type is stored in string array in text form.

```rust
let json_field = Field::new(name, DataType::Utf8, true)
    .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into());
let json_array = StringArray::from(vec![r#"{"key": "value"}"#]);
```

### Decimal Type

Different from the fixed-point decimal type built into Arrow, this decimal type represents floating-point numbers with arbitrary precision or scale, that is, the [unconstrained numeric](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL) in Postgres. The decimal type is stored in a string array in text form.

```rust
let decimal_field = Field::new(name, DataType::Utf8, true)
    .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into());
let decimal_array = StringArray::from(vec!["0.0001", "-1.23", "0"]);
```

## Benchmarks

We have benchmarked the performance of function calls in different environments.
You can run the benchmarks with the following command:

```sh
cargo bench --bench wasm
```

Performance comparison of calling `gcd` on a chunk of 1024 rows:

```
gcd/native          1.5237 µs   x1
gcd/wasm            15.547 µs   x10
gcd/js(quickjs)     85.007 µs   x55
gcd/js(deno)        93.584 µs   x62
gcd/python          175.29 µs   x115
```
