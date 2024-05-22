# Arrow User-Defined Functions Framework

Easily create and run user-defined functions (UDF) on Apache Arrow.
You can define functions in Rust, Python, Java or JavaScript.
The functions can be executed natively, or in WebAssembly, or in a [remote server].

| Language   | Native             | WebAssembly             | Remote                    |
| ---------- | ------------------ | ----------------------- | ------------------------- |
| Rust       | [arrow-udf]        | [arrow-udf-wasm]        |                           |
| Python     | [arrow-udf-python] |                         | [arrow-udf-flight/python] |
| JavaScript | [arrow-udf-js] or [arrow-udf-js-deno] |      |                           |
| Java       |                    |                         | [arrow-udf-flight/java]   |

[arrow-udf]: ./arrow-udf
[arrow-udf-python]: ./arrow-udf-python
[arrow-udf-js]: ./arrow-udf-js
[arrow-udf-js-deno]: ./arrow-udf-js-deno
[arrow-udf-wasm]: ./arrow-udf-wasm
[remote server]: ./arrow-udf-flight
[arrow-udf-flight/python]: ./arrow-udf-flight/python
[arrow-udf-flight/java]: ./arrow-udf-flight/java

## Extension Types

In addition to the standard types defined by Arrow, these crates also support the following data types through Arrow's [extension type](https://arrow.apache.org/docs/format/Columnar.html#format-metadata-extension-types). When using extension types, you need to add the `ARROW:extension:name` key to the field's metadata.

| Extension Type | Physical Type     | Metadata                                    |
| -------------- | ----------------- | ------------------------------------------- |
| JSON           | Utf8, LargeBinary | `ARROW:extension:name` = `arrowudf.json`    |
| Decimal        | Utf8              | `ARROW:extension:name` = `arrowudf.decimal` |

Alternatively, you can configure the extension metadata key and values to look for when converting between Arrow and extension types:

```rust
let mut js_runtime = arrow_udf_js::Runtime::new().unwrap();
let converter = js_runtime.converter_mut();
converter.set_arrow_extension_key("Extension");
converter.set_json_extension_name("Variant");
converter.set_decimal_extension_name("Decimal");
```

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

## Who is using this library?

- [RisingWave]: A Distributed SQL Database for Stream Processing.

[RisingWave]: https://github.com/risingwavelabs/risingwave
