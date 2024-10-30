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
use arrow_cast::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_python::{CallMode, Runtime};
use expect_test::{expect, Expect};

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
    check(
        &[output],
        expect![[r#"
        +-----+
        | gcd |
        +-----+
        | 5   |
        |     |
        +-----+"#]],
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
    check(
        &[output],
        expect![[r#"
        +--------+
        | max_py |
        +--------+
        | 25     |
        |        |
        +--------+"#]],
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
    check(
        &[output],
        expect![[r#"
        +--------+
        | fib    |
        +--------+
        | 832040 |
        +--------+"#]],
    );
}

#[test]
fn test_decimal_add() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "decimal_add",
            decimal_field("add"),
            CallMode::ReturnNullOnNullInput,
            r#"
def decimal_add(a, b):
    return a + b
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![decimal_field("a"), decimal_field("b")]);
    let arg0 = StringArray::from(vec!["0.0001"]);
    let arg1 = StringArray::from(vec!["0.0002"]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal_add", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------+
        | add    |
        +--------+
        | 0.0003 |
        +--------+"#]],
    );
}

#[test]
fn test_json_array_access() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "json_array_access",
            json_field("json"),
            CallMode::ReturnNullOnNullInput,
            r#"
def json_array_access(array, i):
    return array[i]
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        json_field("array"),
        Field::new("i", DataType::Int32, true),
    ]);
    let arg0 = StringArray::from(vec![r#"[1, null, ""]"#]);
    let arg1 = Int32Array::from(vec![0]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("json_array_access", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +------+
        | json |
        +------+
        | 1    |
        +------+"#]],
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
    check(
        &[output],
        expect![[r#"
        +----------+
        | to_array |
        +----------+
        | [1]      |
        |          |
        | [3]      |
        +----------+"#]],
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
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | key_value          |
        +--------------------+
        | {key: a, value: b} |
        +--------------------+"#]],
    );

    runtime
        .add_function(
            "key_value2",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            CallMode::ReturnNullOnNullInput,
            r#"
def key_value2(s: str):
    key, value = s.split('=')
    return {"key": key, "value": value}
"#,
        )
        .unwrap();

    let output = runtime.call("key_value2", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | key_value2         |
        +--------------------+
        | {key: a, value: b} |
        +--------------------+"#]],
    );
}

#[test]
fn test_runtime() {
    let runtime = Runtime::new().unwrap();
    drop(runtime);
    let runtime = Runtime::new().unwrap();
    drop(runtime);
}

#[test]
fn test_struct_to_json() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "to_json",
            json_field("to_json"),
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
    check(
        &[output],
        expect![[r#"
        +------------------------------+
        | to_json                      |
        +------------------------------+
        | {"key": "a", "value": "b"}   |
        | {"key": null, "value": null} |
        +------------------------------+"#]],
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

    check(
        &[o1, o2],
        expect![[r#"
        +-----+--------+
        | row | range1 |
        +-----+--------+
        | 0   | 0      |
        | 2   | 0      |
        | 2   | 1      |
        | 2   | 2      |
        +-----+--------+"#]],
    );
}

#[test]
fn test_sum() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_aggregate(
            "sum",
            DataType::Int32,
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def create_state():
    return 0

def accumulate(state, value):
    return state + value

def retract(state, value):
    return state - value

def merge(state1, state2):
    return state1 + state2
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    let ops = BooleanArray::from(vec![false, false, true, false]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let state = runtime.create_state("sum").unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 0     |
            +-------+"#]],
    );

    let state = runtime.accumulate("sum", &state, &input).unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 9     |
            +-------+"#]],
    );

    let state = runtime
        .accumulate_or_retract("sum", &state, &ops, &input)
        .unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 12    |
            +-------+"#]],
    );

    let output = runtime.finish("sum", &state).unwrap();
    check_array(
        &[output],
        expect![[r#"
            +-------+
            | array |
            +-------+
            | 12    |
            +-------+"#]],
    );
}

#[test]
fn test_weighted_avg() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_aggregate(
            "weighted_avg",
            DataType::Struct(
                vec![
                    Field::new("sum", DataType::Int32, false),
                    Field::new("weight", DataType::Int32, false),
                ]
                .into(),
            ),
            DataType::Float32,
            CallMode::ReturnNullOnNullInput,
            r#"
class State:
    def __init__(self):
        self.sum = 0
        self.weight = 0

def create_state():
    return State()

def accumulate(state, value, weight):
    state.sum += value * weight
    state.weight += weight
    return state

def retract(state, value, weight):
    state.sum -= value * weight
    state.weight -= weight
    return state

def merge(state1, state2):
    state1.sum += state2.sum
    state1.weight += state2.weight
    return state1

def finish(state):
    if state.weight == 0:
        return None
    else:
        return state.sum / state.weight
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("weight", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    let arg1 = Int32Array::from(vec![Some(2), None, Some(4), Some(6)]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let state = runtime.create_state("weighted_avg").unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +---------------------+
            | array               |
            +---------------------+
            | {sum: 0, weight: 0} |
            +---------------------+"#]],
    );

    let state = runtime.accumulate("weighted_avg", &state, &input).unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-----------------------+
            | array                 |
            +-----------------------+
            | {sum: 44, weight: 12} |
            +-----------------------+"#]],
    );

    let states = arrow_select::concat::concat(&[&state, &state]).unwrap();
    let state = runtime.merge("weighted_avg", &states).unwrap();
    check_array(
        std::slice::from_ref(&state),
        expect![[r#"
            +-----------------------+
            | array                 |
            +-----------------------+
            | {sum: 88, weight: 24} |
            +-----------------------+"#]],
    );

    let output = runtime.finish("weighted_avg", &state).unwrap();
    check_array(
        &[output],
        expect![[r#"
            +-----------+
            | array     |
            +-----------+
            | 3.6666667 |
            +-----------+"#]],
    );
}

#[test]
fn test_weighted_avg_pickle_state() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_aggregate(
            "weighted_avg",
            pickle_field("state"),
            DataType::Float32,
            CallMode::ReturnNullOnNullInput,
            r#"
class State:
    def __init__(self):
        self.sum = 0
        self.weight = 0

def create_state():
    return State()

def accumulate(state, value, weight):
    state.sum += value * weight
    state.weight += weight
    return state

def retract(state, value, weight):
    state.sum -= value * weight
    state.weight -= weight
    return state

def merge(state1, state2):
    state1.sum += state2.sum
    state1.weight += state2.weight
    return state1

def finish(state):
    if state.weight == 0:
        return None
    else:
        return state.sum / state.weight
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("value", DataType::Int32, true),
        Field::new("weight", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    let arg1 = Int32Array::from(vec![Some(2), None, Some(4), Some(6)]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let state = runtime.create_state("weighted_avg").unwrap();
    let state = runtime.accumulate("weighted_avg", &state, &input).unwrap();
    // we don't check the state because it is pickled
    let output = runtime.finish("weighted_avg", &state).unwrap();
    check_array(
        &[output],
        expect![[r#"
            +-----------+
            | array     |
            +-----------+
            | 3.6666667 |
            +-----------+"#]],
    );
}

#[test]
fn test_output_type_mismatch() {
    let mut runtime = Runtime::new().unwrap();
    let err = runtime
        .add_aggregate(
            "sum",
            DataType::Int32,
            DataType::Int64,
            CallMode::ReturnNullOnNullInput,
            r#"
def create_state():
    return 0

def accumulate(state, value):
    return state + value
"#,
        )
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "`output_type` must be the same as `state_type` when `finish` is not defined"
    );
}

#[test]
fn test_error() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "div",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def div(a: int, b: int) -> int:
    return a // b
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![1, 2]);
    let arg1 = Int32Array::from(vec![0, 1]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("div", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+-------------------------------------------------------+
        | div | error                                                 |
        +-----+-------------------------------------------------------+
        |     | ZeroDivisionError: integer division or modulo by zero |
        | 2   |                                                       |
        +-----+-------------------------------------------------------+"#]],
    );

    runtime
        .add_function(
            "range1",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def range1(n: int):
    for i in range(n):
        if i == 1:
            raise ValueError("i is 1")
        yield i
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![0, 2, 1]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range1", &input, 10).unwrap();
    let output = outputs.next().unwrap().unwrap();

    check(
        &[output],
        expect![[r#"
        +-----+--------+--------------------+
        | row | range1 | error              |
        +-----+--------+--------------------+
        | 1   | 0      |                    |
        | 1   |        | ValueError: i is 1 |
        | 2   | 0      |                    |
        +-----+--------+--------------------+"#]],
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
    assert_err("", "AttributeError: module 'gcd' has no attribute 'gcd'");
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

#[test]
fn test_type_mismatch() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "neg",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def neg(x):
    return -x
"#,
        )
        .unwrap();

    // case1: return type mismatch
    let schema = Schema::new(vec![Field::new("x", DataType::Float32, true)]);
    let arg0 = Float32Array::from(vec![1.0]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let err = runtime.call("neg", &input).unwrap_err();
    assert_eq!(
        err.to_string(),
        "TypeError: 'float' object cannot be interpreted as an integer"
    );
    // drop the error here and switch to a new runtime
    // this is to ensure that error does not contain `PyErr`,
    // otherwise the next call will cause SIGABRT `pointer being freed was not allocated`.
    drop(err);

    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "neg",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
def neg(x):
    return -x
    "#,
        )
        .unwrap();

    // case2: arguments mismatch
    let input = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )
    .unwrap();

    let output = runtime.call("neg", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+--------------------------------------------------------------+
        | neg | error                                                        |
        +-----+--------------------------------------------------------------+
        |     | TypeError: neg() missing 1 required positional argument: 'x' |
        +-----+--------------------------------------------------------------+"#]],
    );

    // case3: arguments mismatch
    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![1]);
    let arg1 = Int32Array::from(vec![2]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("neg", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+---------------------------------------------------------------+
        | neg | error                                                         |
        +-----+---------------------------------------------------------------+
        |     | TypeError: neg() takes 1 positional argument but 2 were given |
        +-----+---------------------------------------------------------------+"#]],
    );
}

#[test]
fn test_send() {
    let mut runtime = Runtime::new().unwrap();

    std::thread::spawn(move || {
        runtime
            .add_function(
                "neg",
                DataType::Int32,
                CallMode::ReturnNullOnNullInput,
                r#"
def neg(x):
    return -x
"#,
            )
            .unwrap();
    })
    .join()
    .unwrap();
}

#[test]
fn test_view_array() {
    let mut runtime = Runtime::new().unwrap();
    runtime
        .add_function(
            "echo",
            DataType::Utf8View,
            CallMode::ReturnNullOnNullInput,
            r#"
def echo(x):
    return x + "!"
"#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8View, true)]);
    let arg0 = StringViewArray::from(vec!["hello", "world"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("echo", &input).unwrap();

    check(
        &[output],
        expect![[r#"
        +--------+
        | echo   |
        +--------+
        | hello! |
        | world! |
        +--------+"#]],
    );
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check(actual: &[RecordBatch], expect: Expect) {
    expect.assert_eq(&pretty_format_batches(actual).unwrap().to_string());
}

/// Compare the actual output with the expected output.
#[track_caller]
fn check_array(actual: &[ArrayRef], expect: Expect) {
    expect.assert_eq(&pretty_format_columns("array", actual).unwrap().to_string());
}

/// Returns a field with JSON type.
fn json_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with decimal type.
fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
}

/// Returns a field with pickle type.
fn pickle_field(name: &str) -> Field {
    Field::new(name, DataType::Binary, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.pickle".into())].into())
}
