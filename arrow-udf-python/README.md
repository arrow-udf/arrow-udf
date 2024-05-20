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

## Struct Type

If the function returns a struct type, you can return a class instance or a dictionary.

```rust
use arrow_schema::{DataType, Field};
use arrow_udf_python::{CallMode, Runtime};

let mut runtime = Runtime::new().unwrap();
let python_code = r#"
class KeyValue:
    def __init__(self, key, value):
        self.key = key
        self.value = value

def key_value(s: str):
    key, value = s.split('=')

    ## return a class instance
    return KeyValue(key, value)

    ## or return a dict
    return {"key": key, "value": value}
"#;
let return_type = DataType::Struct(
    vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]
    .into(),
);
let mode = CallMode::ReturnNullOnNullInput;
runtime.add_function("key_value", return_type, mode, python_code).unwrap();
```

## Pickle Type

This crate supports one more extension type, the pickle type.
When a field is pickle type, the data is stored in a binary array in serialized form.

| Extension Type | Physical Type | Metadata                                    |
| -------------- | ------------- | ------------------------------------------- |
| Pickle         | Binary        | `ARROW:extension:name` = `arrowudf.pickle`  |

```rust,ignore
let pickle_field = Field::new(name, DataType::Binary, true)
    .with_metadata([("ARROW:extension:name".into(), "arrowudf.pickle".into())].into());
let pickle_array = BinaryArray::from(vec![b"xxxxx"]);
```

Pickle type is useful for the state of aggregation functions when the state is complex.
