# Rust UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf.svg)](https://crates.io/crates/arrow-udf)
[![Docs](https://docs.rs/arrow-udf/badge.svg)](https://docs.rs/arrow-udf)

## Usage

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf = "0.2"
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

#[function("point(float8, float8) -> struct Point", output = "eval_point")]
fn point(x: f64, y: f64) -> Point {
    Point { x, y }
}
```

Currently struct types are only supported as return types.

### Function Registry

If you want to lookup functions by signature, you can enable the `global_registry` feature:

```toml
[dependencies]
arrow-udf = { version = "0.1", features = ["global_registry"] }
```

Each function will be registered in a global registry when it is defined.
Then you can lookup functions from the `REGISTRY`:

```rust,ignore
use arrow_schema::DataType::Int32;
use arrow_udf::sig::REGISTRY;

let sig = REGISTRY.get("gcd", &[Int32, Int32], &Int32).expect("gcd function");
let output = sig.function.as_scalar().unwrap()(&input).unwrap();
```

See the [example](./examples/rust.rs) for more details.
