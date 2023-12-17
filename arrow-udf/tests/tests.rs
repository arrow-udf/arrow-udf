// Copyright 2023 RisingWave Labs
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

use std::{ops::Neg, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf::function;

// test no return value
#[function("null()")]
fn null() {}

// test simd with no arguments
#[function("zero() -> int")]
fn zero() -> i32 {
    0
}

// test simd with 1 arguments
#[function("neg(int2) -> int2")]
#[function("neg(int4) -> int4")]
#[function("neg(int8) -> int8")]
#[function("neg(float4) -> float4")]
#[function("neg(float8) -> float8")]
fn neg<T: Neg<Output = T>>(x: T) -> T {
    x.neg()
}

// test simd with 2 arguments
#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

#[function("identity(boolean) -> boolean")]
#[function("identity(int2) -> int2")]
#[function("identity(int4) -> int4")]
#[function("identity(int8) -> int8")]
#[function("identity(float4) -> float4")]
#[function("identity(float8) -> float8")]
#[function("identity(varchar) -> varchar")]
#[function("identity(bytea) -> bytea")]
fn identity<T>(x: T) -> T {
    x
}

#[function("option_add(int, int) -> int")]
fn option_add(x: i32, y: Option<i32>) -> i32 {
    x + y.unwrap_or(0)
}

#[function("length(varchar) -> int")]
#[function("length(bytea) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}

#[function("substring(varchar, int) -> varchar")]
fn substring_varchar(s: &str, start: i32) -> &str {
    s.char_indices()
        .nth(start.max(0) as usize)
        .map_or("", |(i, _)| &s[i..])
}

#[function("substring(bytea, int) -> bytea")]
fn substring_bytea(s: &[u8], start: i32) -> &[u8] {
    let start = start.max(0).min(s.len() as i32) as usize;
    &s[start..]
}

#[function("to_string1(int) -> varchar")]
fn to_string1(x: i32) -> String {
    x.to_string()
}

#[function("to_string2(int) -> varchar")]
fn to_string2(x: i32) -> Box<str> {
    x.to_string().into()
}

#[function("to_string3(int) -> varchar")]
fn to_string3(x: i32, output: &mut impl std::fmt::Write) {
    write!(output, "{}", x).unwrap();
}

#[function("to_string4(int) -> varchar")]
fn to_string4(x: i32, output: &mut impl std::fmt::Write) -> Option<()> {
    let x = usize::try_from(x).ok()?;
    write!(output, "{}", x).unwrap();
    Some(())
}

#[function("bytes1(int) -> bytea")]
fn bytes1(x: i32) -> Vec<u8> {
    vec![0; x as usize]
}

#[function("bytes2(int) -> bytea")]
fn bytes2(x: i32) -> Box<[u8]> {
    vec![0; x as usize].into()
}

#[function("bytes3(int) -> bytea")]
fn bytes3(x: i32) -> [u8; 10] {
    [x as u8; 10]
}

#[function("bytes4(int) -> bytea")]
fn bytes4(x: i32, output: &mut impl std::io::Write) {
    for _ in 0..x {
        output.write_all(&[0]).unwrap();
    }
}

#[function("split(varchar) -> varchar[]")]
fn split(s: &str) -> impl Iterator<Item = &str> {
    s.split(',')
}

#[function("key_value(varchar) -> struct<key:varchar,value:varchar>")]
fn key_value(kv: &str) -> Option<(&str, &str)> {
    kv.split_once('=')
}

#[function("nested_struct() -> struct<a:int, b:struct<c:int, d:varchar>>")]
fn nested_struct() -> (i32, (i32, &'static str)) {
    (1, (2, "g"))
}

#[test]
fn test_neg() {
    let sig = neg_int4_int4_sig();
    assert_eq!(sig.name, "neg");
    assert_eq!(sig.arg_types, vec![DataType::Int32.into()]);
    assert_eq!(sig.variadic, false);
    assert_eq!(sig.return_type, DataType::Int32.into());

    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None]);
    let expected = Int32Array::from(vec![Some(-1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = (sig.function)(&input).unwrap();
    assert_eq!(output.as_primitive::<Int32Type>(), &expected);
}

#[test]
fn test_key_value() {
    let sig = key_value_varchar_struct_key_varchar_value_varchar__sig();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a=b", "??"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = (sig.function)(&input).unwrap();
    assert_eq!(
        arrow_cast::pretty::pretty_format_columns("result", std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------------------+
| result             |
+--------------------+
| {key: a, value: b} |
|                    |
+--------------------+
"#
        .trim()
    );
}

#[test]
fn test_nested_struct() {
    let sig = nested_struct_struct_a_int4_b_struct_c_int4_d_varchar__sig();

    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![1]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = (sig.function)(&input).unwrap();
    assert_eq!(
        arrow_cast::pretty::pretty_format_columns("result", std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+-------------------------+
| result                  |
+-------------------------+
| {a: 1, b: {c: 2, d: g}} |
+-------------------------+
"#
        .trim()
    );
}

#[test]
fn test_split() {
    let sig = split_varchar_varchararray_sig();

    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a,b"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = (sig.function)(&input).unwrap();
    assert_eq!(
        arrow_cast::pretty::pretty_format_columns("result", std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------+
| result |
+--------+
| [a, b] |
+--------+
"#
        .trim()
    );
}

#[test]
fn test_option_add() {
    let sig = option_add_int4_int4_int4_sig();

    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), Some(1), None, None]);
    let arg1 = Int32Array::from(vec![Some(1), None, Some(1), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = (sig.function)(&input).unwrap();
    assert_eq!(
        arrow_cast::pretty::pretty_format_columns("result", std::slice::from_ref(&output))
            .unwrap()
            .to_string(),
        r#"
+--------+
| result |
+--------+
| 2      |
| 1      |
|        |
|        |
+--------+
"#
        .trim()
    );
}
