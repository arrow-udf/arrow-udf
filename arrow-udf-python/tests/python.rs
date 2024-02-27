// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::*;
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_python::{CallMode, Runtime};

#[test]
fn test_gcd() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(25), None]);
    let arg1 = Int32Array::from(vec![Some(15), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("gcd", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-----+
| gcd |
+-----+
| 5   |
|     |
+-----+
"#
        .trim()
    );

    runtime.del_function("gcd").unwrap();
}

#[test]
fn test_max_with_custom_handler() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function_with_handler(
            "max_py",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def max_handler(a: int, b: int) -> int:
    if a > b:
         return a
    else:
         return b
"#,
            "max_handler",
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(25), None]);
    let arg1 = Int32Array::from(vec![Some(15), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("max_py", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------+
| max_py |
+--------+
| 25     |
|        |
+--------+
"#
        .trim()
    );

    runtime.del_function("max_py").unwrap();
}

#[test]
fn test_fib() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "fib",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def fib(n: int) -> int:
    if n <= 1:
        return n
    else:
        return fib(n - 1) + fib(n - 2)
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![30]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("fib", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------+
| fib    |
+--------+
| 832040 |
+--------+
"#
        .trim()
    );
}

#[test]
fn test_decimal_add() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "decimal_add",
            DataType::LargeBinary,
            CallMode::ReturnNullOnNullInput,
            r#"
def decimal_add(a, b):
    return a + b
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::LargeBinary, true),
        Field::new("b", DataType::LargeBinary, true),
    ]);
    let arg0 = LargeBinaryArray::from(vec![b"0.0001".as_ref()]);
    let arg1 = LargeBinaryArray::from(vec![b"0.0002".as_ref()]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal_add", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------------+
| decimal_add  |
+--------------+
| 302e30303033 |
+--------------+
"#
        .trim()
    );
}

#[test]
fn test_json_array_access() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "json_array_access",
            DataType::LargeUtf8,
            CallMode::ReturnNullOnNullInput,
            r#"
def json_array_access(array, i):
    return array[i]
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("array", DataType::LargeUtf8, true),
        Field::new("i", DataType::Int32, true),
    ]);
    let arg0 = LargeStringArray::from(vec![r#"[1, null, ""]"#]);
    let arg1 = Int32Array::from(vec![0]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("json_array_access", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-------------------+
| json_array_access |
+-------------------+
| 1                 |
+-------------------+
"#
        .trim()
    );
}

#[test]
fn test_return_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "to_array",
            DataType::new_list(DataType::Int32, true),
            CallMode::CalledOnNullInput,
            r#"
def to_array(x):
    if x is None:
        return None
    else:
        return [x]
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_array", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+----------+
| to_array |
+----------+
| [1]      |
|          |
| [3]      |
+----------+
    "#
        .trim()
    );
}

#[test]
fn test_key_value() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "key_value",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            CallMode::ReturnNullOnNullInput,
            r#"
class KeyValue:
    def __init__(self, key, value):
        self.key = key
        self.value = value

def key_value(s: str):
    key, value = s.split('=')
    return KeyValue(key, value)
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a=b"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("key_value", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------------------+
| key_value          |
+--------------------+
| {key: a, value: b} |
+--------------------+
"#
        .trim()
    );
}

#[test]
fn test_struct_to_json() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "to_json",
            DataType::LargeUtf8,
            CallMode::ReturnNullOnNullInput,
            r#"
def to_json(object):
    return object.__dict__
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    )]);
    let arg0 = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("b"), None])),
        ),
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_json", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+------------------------------+
| to_json                      |
+------------------------------+
| {"key": "a", "value": "b"}   |
| {"key": null, "value": null} |
+------------------------------+
"#
        .trim()
    );
}

#[test]
fn test_range() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "range1",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def range1(n: int):
    for i in range(n):
        yield i
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range1", &input, 2).unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range1");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.next().unwrap().unwrap();
    let o2 = outputs.next().unwrap().unwrap();
    assert_eq!(o1.num_rows(), 2);
    assert_eq!(o2.num_rows(), 2);
    assert!(outputs.next().is_none());

    assert_eq!(
        pretty_format_batches(&[o1, o2]).unwrap().to_string(),
        r#"
+-----+--------+
| row | range1 |
+-----+--------+
| 0   | 0      |
| 2   | 0      |
| 2   | 1      |
| 2   | 2      |
+-----+--------+
"#
        .trim()
    );
}

/// Test there is no GIL contention across threads.
#[test]
fn test_no_gil() {
    use std::time::Duration;

    fn timeit(f: impl FnOnce()) -> Duration {
        let start = std::time::Instant::now();
        f();
        start.elapsed()
    }

    let t0 = timeit(test_fib);
    let t1 = timeit(|| {
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(test_fib);
            }
        })
    });
    assert!(
        t1 < t0 + Duration::from_millis(10),
        "multi-threaded execution is slower than single-threaded. there is GIL contention."
    )
}

#[test]
fn test_import() {
    let mut runtime = Runtime::builder().sandboxed(true).build().unwrap();
    runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
import decimal
import json
import math
import re
import numbers
import datetime

def gcd(a: int, b: int) -> int:
    while b:
        a, b = b, a % b
    return a
"#,
        )
        .unwrap();
}

#[test]
fn test_forbid() {
    assert_err("", "AttributeError: module '' has no attribute 'gcd'");
    assert_err("import os", "ImportError: import os is not allowed");
    assert_err(
        "breakpoint()",
        "NameError: name 'breakpoint' is not defined",
    );
    assert_err("exit()", "NameError: name 'exit' is not defined");
    assert_err("eval('exit()')", "NameError: name 'eval' is not defined");
    assert_err("help()", "NameError: name 'help' is not defined");
    assert_err("input()", "NameError: name 'input' is not defined");
    assert_err("open('foo', 'w')", "NameError: name 'open' is not defined");
    assert_err("print()", "NameError: name 'print' is not defined");
}

#[track_caller]
fn assert_err(code: &str, err: &str) {
    let mut runtime = Runtime::builder().sandboxed(true).build().unwrap();
    let error = runtime
        .add_function(
            "gcd",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            code,
        )
        .unwrap_err();
    assert_eq!(error.to_string(), err);
}
