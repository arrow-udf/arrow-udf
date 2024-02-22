# Python UDF for Apache Arrow

[![Crate](https://img.shields.io/crates/v/arrow-udf-python.svg)](https://crates.io/crates/arrow-udf-python)
[![Docs](https://docs.rs/arrow-udf-python/badge.svg)](https://docs.rs/arrow-udf-python)

Notice: Python 3.12 is required to run this library.
If `python3` is not 3.12, please set the environment variable `PYO3_PYTHON=python3.12`.

Add the following lines to your `Cargo.toml`:

```toml
[dependencies]
arrow-udf-python = "0.1"
```

Create a `Runtime` and define your Python functions in string form.
Note that the function name must match the one you pass to `add_function`.

```rust
use arrow_udf_python::{CallMode, Runtime};

let mut runtime = Runtime::new().unwrap();
let python_code = r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#;
let return_type = arrow_schema::DataType::Int32;
let mode = CallMode::ReturnNullOnNullInput;
runtime.add_function("gcd", return_type, mode, python_code).unwrap();
```

You can then call the python function on a `RecordBatch`:

```rust,ignore
let input: RecordBatch = ...;
let output: RecordBatch = runtime.call("gcd", &input).unwrap();
```

The python code will be run in an embedded CPython 3.12 interpreter, powered by [PyO3](pyo3.rs).

See the [example](examples/python.rs) for more details.
