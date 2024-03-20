# Deno UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf-js.svg)](https://crates.io/crates/arrow-udf-js-deno)
[![Docs](https://docs.rs/arrow-udf-js/badge.svg)](https://docs.rs/arrow-udf-js-deno)

## Usage

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-js-deno = "0.0.1"
```

Create a `Runtime` and define your JS functions in string form.
Note that the function must be exported and its name must match the one you pass to `add_function`.

```rust
use arrow_udf_js_deno::{Runtime, CallMode};

let mut runtime = Runtime::new();
runtime
    .add_function(
        "gcd",
        arrow_schema::DataType::Int32,
        CallMode::ReturnNullOnNullInput,
        r#"
        export function gcd(a, b) {
            while (b != 0) {
                let t = b;
                b = a % b;
                a = t;
            }
            return a;
        }
        "#,
    )
    .await
    .unwrap();
```

You can then call the JS function on a `RecordBatch`:

```rust,ignore
let input: RecordBatch = ...;
let output: RecordBatch = runtime.call("gcd", input).await.unwrap();
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

For set-returning functions (or so-called table functions), define the function as a generator:

```rust
use arrow_udf_js_deno::{Runtime, CallMode};

let mut runtime = Runtime::new();
runtime
    .add_function(
        "range",
        arrow_schema::DataType::Int32,
        CallMode::ReturnNullOnNullInput,
        r#"
        export function* range(n) {
            for (let i = 0; i < n; i++) {
                yield i;
            }
        }
        "#,
    )
    .await
    .unwrap();
```

You can then call the table function via `call_table_function`:

```rust,ignore
let chunk_size = 1024;
let input: RecordBatch = ...;
let mut outputs = runtime.call_table_function("range", input, chunk_size).await.unwrap();
while let Some(result) = outputs.next().await {
    let output: RecordBatch = result?;
    // do something with the output
}
```

If you print the output batch, it will be like this:

```text
+-----+-------+
| row | range |
+-----+-------+
| 0   | 0     |
| 2   | 0     |
| 2   | 1     |
| 2   | 2     |
+-----+-------+
```

The JS code will be run deno runtime.

See the [example](examples/js.rs) for more details.

## Using external libraries

You can use external libraries in your JS code. By using a bundler as esbuild, you can bundle the external libraries into a single file and then pass the bundled file to `add_function`. See the [example](examples/sse) for more details.

To run the example, first you need to compile the bundle using esbuild:

```sh
cd examples/sse
npm install
npm run build
```

Then you can run the sse mock server by running:

```sh
cargo run -p sse-server
```

And finally, you can run the example by running:

```sh
cargo run --example sse --package arrow-udf-js-deno

```

## Type Mapping

The following table shows the type mapping between Arrow and JavaScript:

| Arrow Type            | JS Type       | Note                  |
| --------------------- | ------------- | --------------------- |
| Null                  | null          |                       |
| Boolean               | boolean       |                       |
| Int8                  | number        |                       |
| Int16                 | number        |                       |
| Int32                 | number        |                       |
| Int64                 | number        |                       |
| UInt8                 | number        |                       |
| UInt16                | number        |                       |
| UInt32                | number        |                       |
| UInt64                | number        |                       |
| Float32               | number        |                       |
| Float64               | number        |                       |
| Utf8                  | string        |                       |
| Binary                | Uint8Array    |                       |
| LargeString (json)    | null, boolean, number, string, array or object | `JSON.parse(string)`  |
| LargeBinary (decimal) | BigDecimal    |                       |
| List(Int8)            | Int8Array     |                       |
| List(Int16)           | Int16Array    |                       |
| List(Int32)           | Int32Array    |                       |
| List(Int64)           | BigInt64Array |                       |
| List(UInt8)           | Uint8Array    |                       |
| List(UInt16)          | Uint16Array   |                       |
| List(UInt32)          | Uint32Array   |                       |
| List(UInt64)          | BigUint64Array|                       |
| List(Float32)         | Float32Array  |                       |
| List(Float64)         | Float64Array  |                       |
| List(others)          | Array         |                       |
| Date32                | Date/Dayjs    |                       |
| Date64                | Date/Dayjs    |                       |
| Duration(Second)      | Dayjs.duration|                       |
| Duration(Milisecond)  | Dayjs.duration|                       |
| Interval(YearMonth)   | Dayjs.duration|                       |
| Interval(DayTime)     | Dayjs.duration|                       |
| Interval(MonthDayNano)| Dayjs.duration|  Truncated to milliseconds                     |
| Struct                | object        |                       |