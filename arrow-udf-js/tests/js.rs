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

use arrow_array::{BinaryArray, Int32Array, LargeStringArray, RecordBatch};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js::{CallMode, Runtime};

#[test]
fn test_gcd() {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function gcd(a, b) {
            while (b != 0) {
                let t = b;
                b = a % b;
                a = t;
            }
            return a;
        }
    "#;
    runtime
        .add_function(
            "gcd",
            arrow_schema::DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            js_code,
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
fn test_to_string() {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function to_string(a) {
            if (a == null) {
                return "null";
            }
            return a.toString();
        }
    "#;
    runtime
        .add_function(
            "to_string",
            arrow_schema::DataType::Utf8,
            CallMode::CalledOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(5), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_string", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-----------+
| to_string |
+-----------+
| 5         |
| null      |
+-----------+
"#
        .trim()
    );
}

#[test]
fn test_concat() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "concat",
            arrow_schema::DataType::Binary,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function concat(a, b) {
                return a.concat(b);
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Binary, true),
        Field::new("b", DataType::Binary, true),
    ]);
    let arg0 = BinaryArray::from(vec![&b"hello"[..]]);
    let arg1 = BinaryArray::from(vec![&b"world"[..]]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("concat", &input).unwrap();
    assert_eq!(
        pretty_format_batches(std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+----------------------+
| concat               |
+----------------------+
| 68656c6c6f776f726c64 |
+----------------------+
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
            arrow_schema::DataType::LargeUtf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function json_array_access(array, i) {
                return array[i];
            }
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