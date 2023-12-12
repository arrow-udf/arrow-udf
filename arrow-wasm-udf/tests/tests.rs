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

use std::sync::Arc;

use arrow::{
    array::{AsArray, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Int32Type, Schema},
};
use arrow_wasm_udf::function;

// test no return value
#[function("null()")]
fn null() {}

// test simd with no arguments
#[function("zero() -> int")]
fn zero() -> i32 {
    0
}

// test simd with 1 arguments
#[function("double(int) -> int")]
fn double(x: i32) -> i32 {
    x * 2
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

#[function("count(varchar) -> int")]
fn count_varchar(s: &str) -> i32 {
    s.len() as i32
}

#[function("count(bytea) -> int")]
fn count_bytea(s: &[u8]) -> i32 {
    s.len() as i32
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

#[test]
fn test_double() {
    let sig = double_sig();
    assert_eq!(sig.name, "double");
    assert_eq!(sig.arg_types, vec![DataType::Int32.into()]);
    assert_eq!(sig.variadic, false);
    assert_eq!(sig.return_type, DataType::Int32.into());

    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let expected = Int32Array::from(vec![Some(2), None, Some(6)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = sig.function.eval(&input).unwrap();
    assert_eq!(output.as_primitive::<Int32Type>(), &expected);
}
