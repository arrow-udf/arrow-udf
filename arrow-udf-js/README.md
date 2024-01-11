# JavaScript UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf-js.svg)](https://crates.io/crates/arrow-udf-js)
[![Docs](https://docs.rs/arrow-udf-js/badge.svg)](https://docs.rs/arrow-udf-js)

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-js = "0.1"
```

Create a `Runtime` and define your JS functions in string form.
Note that the function must be exported and its name must match the one you pass to `add_function`.

```rust
use arrow_udf_js::{Runtime, CallMode};

let mut runtime = Runtime::new().unwrap();
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
    .unwrap();
```

You can then call the JS function on a `RecordBatch`:

```rust
let input: RecordBatch = ...;
let output: RecordBatch = runtime.call("gcd", &input).unwrap();
```

The JS code will be run in an embedded QuickJS interpreter.

See the [example](examples/js.rs) for more details.
