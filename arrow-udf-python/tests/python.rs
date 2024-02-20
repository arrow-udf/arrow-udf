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

use arrow_array::{Int32Array, RecordBatch};
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

/// Test there is no GIL contention across threads.
#[test]
// #[cfg(Py_3_12)]
fn test_multi_threads() {
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
fn test_forbid() {
    assert_err("", "AttributeError: module 'gcd' has no attribute 'gcd'");
    assert_err("import os", "ImportError: __import__ not found");
    assert_err(
        "breakpoint()",
        "NameError: name 'breakpoint' is not defined",
    );
    assert_err("exit()", "NameError: name 'exit' is not defined");
    assert_err("eval('exit()')", "NameError: name 'eval' is not defined");
    assert_err("exec('exit()')", "NameError: name 'exec' is not defined");
    assert_err("help()", "NameError: name 'help' is not defined");
    assert_err("input()", "NameError: name 'input' is not defined");
    assert_err("open('foo', 'w')", "NameError: name 'open' is not defined");
    assert_err("print()", "NameError: name 'print' is not defined");
}

#[track_caller]
fn assert_err(code: &str, err: &str) {
    let mut runtime = Runtime::new().unwrap();
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
