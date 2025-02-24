# Rust UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf.svg)](https://crates.io/crates/arrow-udf)
[![Docs](https://docs.rs/arrow-udf/badge.svg)](https://docs.rs/arrow-udf)

Generate `RecordBatch` functions from scalar functions painlessly with the [#[function] macro](https://docs.rs/arrow-udf/latest/arrow_udf/attr.function.html).

## Usage

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.6"
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

The generated function can be named with the optional `output` parameter.
If not specified, it will be named arbitrarily like `gcd_int32_int32_int32_eval`.

You can then call the generated function on a `RecordBatch`:

```rust,ignore
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

### Fallible Functions

If your function returns a `Result`:

```rust
use arrow_udf::function;

#[function("div(int32, int32) -> int32", output = "eval_div")]
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

### Struct Types

You can define a struct type with the `StructType` trait:

```rust
use arrow_udf::types::StructType;

#[derive(StructType)]
struct Point {
    x: f64,
    y: f64,
}
```

Then you can use the struct type in function signatures:

```rust,ignore
use arrow_udf::function;

#[function("point(float64, float64) -> struct Point", output = "eval_point")]
fn point(x: f64, y: f64) -> Point {
    Point { x, y }
}
```

Currently struct types are only supported as return types.

### Function Registry

If you want to lookup functions by signature, you can enable the `global_registry` feature:

```toml
[dependencies]
arrow-udf = { version = "0.3", features = ["global_registry"] }
```

Each function will be registered in a global registry when it is defined.
Then you can lookup functions from the `REGISTRY`:

```rust,ignore
use arrow_schema::{DataType, Field};
use arrow_udf::sig::REGISTRY;

let int32 = Field::new("int32", DataType::Int32, true);
let sig = REGISTRY.get("gcd", &[int32.clone(), int32.clone()], &int32).expect("gcd function");
let output = sig.function.as_scalar().unwrap()(&input).unwrap();
```

See the [example](https://github.com/risingwavelabs/arrow-udf/blob/main/arrow-udf/examples/rust.rs) and the [documentation for the #[function] macro](https://docs.rs/arrow-udf/latest/arrow_udf/attr.function.html) for more details.

See also the blog post: [Simplifying SQL Function Implementation with Rust Procedural Macro](https://risingwave.com/blog/simplifying-sql-function-implementation-with-rust-procedural-macro/).
