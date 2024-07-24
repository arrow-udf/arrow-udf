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

use crate::common::check;
use arrow_array::{Float32Array, Float64Array, Int32Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf::function;
use expect_test::expect;

// test visibility
#[function("maybe_visible(int) -> int", output = "maybe_visible_udf")]
#[function(
    "maybe_visible(uint32) -> uint32",
    output = "maybe_visible_pub_udf",
    visibility = "pub"
)]
#[function(
    "maybe_visible(float32) -> float32",
    output = "maybe_visible_pub_crate_udf",
    visibility = "pub(crate)"
)]
#[function(
    "maybe_visible(float64) -> float64",
    output = "maybe_visible_pub_self_udf",
    visibility = "pub(self)"
)]
#[function(
    "maybe_visible(string) -> string",
    output = "maybe_visible_pub_super_udf",
    visibility = "pub(super)"
)]
fn maybe_visible<T>(x: T) -> T {
    x
}

#[test]
fn test_default() {
    let schema = Schema::new(vec![Field::new("int", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = maybe_visible_udf(&input).unwrap();
    check(
        &[output],
        expect![[r#"
    +---------------+
    | maybe_visible |
    +---------------+
    | 1             |
    |               |
    +---------------+"#]],
    );
}

#[test]
fn test_pub() {
    let schema = Schema::new(vec![Field::new("uint32", DataType::UInt32, true)]);
    let arg0 = UInt32Array::from(vec![Some(1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = maybe_visible_pub_udf(&input).unwrap();
    check(
        &[output],
        expect![[r#"
    +---------------+
    | maybe_visible |
    +---------------+
    | 1             |
    |               |
    +---------------+"#]],
    );
}

#[test]
fn test_pub_crate() {
    let schema = Schema::new(vec![Field::new("float32", DataType::Float32, true)]);
    let arg0 = Float32Array::from(vec![Some(1.0), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = maybe_visible_pub_crate_udf(&input).unwrap();
    check(
        &[output],
        expect![[r#"
    +---------------+
    | maybe_visible |
    +---------------+
    | 1.0           |
    |               |
    +---------------+"#]],
    );
}

#[test]
fn test_pub_self() {
    let schema = Schema::new(vec![Field::new("float64", DataType::Float64, true)]);
    let arg0 = Float64Array::from(vec![Some(1.0), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = maybe_visible_pub_self_udf(&input).unwrap();
    check(
        &[output],
        expect![[r#"
    +---------------+
    | maybe_visible |
    +---------------+
    | 1.0           |
    |               |
    +---------------+"#]],
    );
}

#[test]
fn test_pub_super() {
    let schema = Schema::new(vec![Field::new("string", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec![Some("1.0"), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = maybe_visible_pub_super_udf(&input).unwrap();
    check(
        &[output],
        expect![[r#"
    +---------------+
    | maybe_visible |
    +---------------+
    | 1.0           |
    |               |
    +---------------+"#]],
    );
}
