# Python UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf-python.svg)](https://crates.io/crates/arrow-udf-python)
[![Docs](https://docs.rs/arrow-udf-python/badge.svg)](https://docs.rs/arrow-udf-python)

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-python = "0.1"
```

Define your Python function in a string and create a `Runtime` for each function:

```rust
use arrow_udf_python::{CallMode, Function};

let python_code = r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#;
let return_type = arrow_schema::DataType::Int32;
let mode = CallMode::ReturnNullOnNullInput;
let function = Function::new("gcd", return_type, mode, python_code).unwrap();
```

You can then call the python function on a `RecordBatch`:

```rust
let input: RecordBatch = ...;
let output: RecordBatch = function.call(&input).unwrap();
```

The python code will be run in an embedded CPython 3.11 interpreter, powered by [PyO3](pyo3.rs).
Please note that due to the limitation of GIL, only one Python function can be running in a process at the same time.

See the [example](examples/python.rs) for more details.
