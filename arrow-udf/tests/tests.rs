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

use std::iter::Sum;
use std::ops::{Add, Neg};
use std::sync::Arc;

use arrow_array::temporal_conversions::time_to_time64us;
use arrow_array::types::{Date32Type, Int32Type};
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_udf::function;
use arrow_udf::types::*;
use cases::visibility_tests::{maybe_visible_pub_crate_udf, maybe_visible_pub_udf};
use common::check;
use expect_test::expect;

mod cases;
mod common;

// test no return value
#[function("null()")]
fn null() {}

// test simd with no arguments
#[function("zero() -> int")]
fn zero() -> i32 {
    0
}

// test simd with 1 arguments
#[function("neg(int8) -> int8")]
#[function("neg(int16) -> int16")]
#[function("neg(int32) -> int32")]
#[function("neg(int64) -> int64")]
#[function("neg(float32) -> float32")]
#[function("neg(float64) -> float64")]
#[function("neg(decimal) -> decimal")]
fn neg<T: Neg<Output = T>>(x: T) -> T {
    x.neg()
}

// test simd with 2 arguments
#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

#[function("add(decimal, decimal) -> decimal")]
fn add<T: Add<Output = T>>(x: T, y: T) -> T {
    x + y
}

#[function("identity(boolean) -> boolean")]
#[function("identity(int8) -> int8")]
#[function("identity(int16) -> int16")]
#[function("identity(int32) -> int32")]
#[function("identity(int64) -> int64")]
#[function("identity(uint8) -> uint8")]
#[function("identity(uint16) -> uint16")]
#[function("identity(uint32) -> uint32")]
#[function("identity(uint64) -> uint64")]
#[function("identity(float32) -> float32")]
#[function("identity(float64) -> float64")]
#[function("identity(decimal) -> decimal")]
#[function("identity(date) -> date")]
#[function("identity(time) -> time")]
#[function("identity(timestamp) -> timestamp")]
// #[function("identity(timestamptz) -> timestamptz")]
#[function("identity(interval) -> interval")]
#[function("identity(json) -> json")]
#[function("identity(string) -> string")]
#[function("identity(binary) -> binary")]
#[function("identity(largestring) -> largestring")]
#[function("identity(largebinary) -> largebinary")]
fn identity<T>(x: T) -> T {
    x
}

#[function("option_add(int, int) -> int")]
fn option_add(x: i32, y: Option<i32>) -> i32 {
    x + y.unwrap_or(0)
}

#[function("div(int, int) -> int")]
fn div(x: i32, y: i32) -> Result<i32, &'static str> {
    x.checked_div(y).ok_or("division by zero")
}

#[function("to_json(boolean) -> json")]
#[function("to_json(int*) -> json")]
#[function("to_json(uint*) -> json")]
#[function("to_json(float*) -> json")]
#[function("to_json(string) -> json")]
fn to_json(x: Option<impl Into<serde_json::Value>>) -> serde_json::Value {
    match x {
        Some(x) => x.into(),
        None => serde_json::Value::Null,
    }
}

#[function("datetime(date, time) -> timestamp")]
fn datetime(date: NaiveDate, time: NaiveTime) -> NaiveDateTime {
    NaiveDateTime::new(date, time)
}

#[function("length(string) -> int")]
#[function("length(binary) -> int")]
#[function("length(largestring) -> int")]
#[function("length(largebinary) -> int")]
fn length(s: impl AsRef<[u8]>) -> i32 {
    s.as_ref().len() as i32
}

#[function("substring(string, int) -> string")]
fn substring_string(s: &str, start: i32) -> &str {
    s.char_indices()
        .nth(start.max(0) as usize)
        .map_or("", |(i, _)| &s[i..])
}

#[function("substring(binary, int) -> binary")]
fn substring_binary(s: &[u8], start: i32) -> &[u8] {
    let start = start.max(0).min(s.len() as i32) as usize;
    &s[start..]
}

#[function("to_string1(int) -> string")]
#[function("to_string1(int) -> largestring")]
fn to_string1(x: i32) -> String {
    x.to_string()
}

#[function("to_string2(int) -> string")]
#[function("to_string2(int) -> largestring")]
fn to_string2(x: i32) -> Box<str> {
    x.to_string().into()
}

#[function("to_string3(int) -> string")]
#[function("to_string3(int) -> largestring")]
fn to_string3(x: i32, output: &mut impl std::fmt::Write) {
    write!(output, "{x}").unwrap();
}

#[function("to_string4(int) -> string")]
#[function("to_string4(int) -> largestring")]
fn to_string4(x: i32, output: &mut impl std::fmt::Write) -> Option<()> {
    let x = usize::try_from(x).ok()?;
    write!(output, "{x}").unwrap();
    Some(())
}

#[function("bytes1(int) -> binary")]
#[function("bytes1(int) -> largebinary")]
fn bytes1(x: i32) -> Vec<u8> {
    vec![0; x as usize]
}

#[function("bytes2(int) -> binary")]
#[function("bytes2(int) -> largebinary")]
fn bytes2(x: i32) -> Box<[u8]> {
    vec![0; x as usize].into()
}

#[function("bytes3(int) -> binary")]
#[function("bytes3(int) -> largebinary")]
fn bytes3(x: i32) -> [u8; 10] {
    [x as u8; 10]
}

// FIXME: std::io::Write is not supported yet
// #[function("bytes4(int) -> binary")]
// #[function("bytes4(int) -> largebinary")]
// fn bytes4(x: i32, output: &mut impl std::io::Write) {
//     for _ in 0..x {
//         output.write_all(&[0]).unwrap();
//     }
// }

#[function("array_sum(int8[]) -> int8")]
#[function("array_sum(int16[]) -> int16")]
#[function("array_sum(int32[]) -> int32")]
#[function("array_sum(int64[]) -> int64")]
#[function("array_sum(float32[]) -> float32")]
#[function("array_sum(float64[]) -> float64")]
fn array_sum<T: Sum + Copy>(s: &[T]) -> T {
    s.iter().cloned().sum()
}

#[function("split(string) -> string[]")]
fn split(s: &str) -> impl Iterator<Item = &str> {
    s.split(',')
}

#[function("int8_array(int8[]) -> int8[]")]
#[function("int16_array(int16[]) -> int16[]")]
#[function("int32_array(int32[]) -> int32[]")]
#[function("int64_array(int64[]) -> int64[]")]
#[function("uint8_array(uint8[]) -> uint8[]")]
#[function("uint16_array(uint16[]) -> uint16[]")]
#[function("uint32_array(uint32[]) -> uint32[]")]
#[function("uint64_array(uint64[]) -> uint64[]")]
#[function("float32_array(float32[]) -> float32[]")]
#[function("float64_array(float64[]) -> float64[]")]
fn primitive_array<T>(_: &[T]) -> impl Iterator<Item = T> + use<T> {
    [].into_iter()
}

#[function("string_array(string[]) -> string[]")]
fn string_array<'b>(_: &StringArray) -> impl Iterator<Item = &'b str> + use<'b> {
    [].into_iter()
}

#[function("large_string_array(largestring[]) -> largestring[]")]
fn large_string_array(_: &LargeStringArray) -> impl Iterator<Item = String> + use<> {
    [].into_iter()
}

#[function("binary_array(binary[]) -> binary[]")]
fn binary_array<'b>(_: &BinaryArray) -> impl Iterator<Item = &'b [u8]> + use<'b> {
    [].into_iter()
}

#[function("large_binary_array(largebinary[]) -> largebinary[]")]
fn large_binary_array(_: &LargeBinaryArray) -> impl Iterator<Item = Vec<u8>> + use<> {
    [].into_iter()
}

#[derive(StructType)]
struct KeyValue<'a> {
    key: &'a str,
    value: &'a str,
}

#[function("key_value(string) -> struct KeyValue")]
fn key_value(kv: &str) -> Option<KeyValue<'_>> {
    let (key, value) = kv.split_once('=')?;
    Some(KeyValue { key, value })
}

#[function("key_values(string) -> setof struct KeyValue")]
fn key_values(kv: &str) -> impl Iterator<Item = KeyValue<'_>> {
    kv.split(',').filter_map(|kv| {
        kv.split_once('=')
            .map(|(key, value)| KeyValue { key, value })
    })
}

#[derive(StructType)]
struct StructOfAll {
    // FIXME: panic on 'StructBuilder and field_builders are of unequal lengths.'
    // a: (),
    b: Option<bool>,
    a: i8,
    c: i16,
    d: i32,
    e: i64,
    aa: u8,
    cc: u16,
    dd: u32,
    ee: u64,
    f: f32,
    g: f64,
    h: Decimal,
    i: NaiveDate,
    j: NaiveTime,
    k: NaiveDateTime,
    l: Interval,
    m: serde_json::Value,
    n: String,
    o: Vec<u8>,
    p: Vec<String>,
    q: KeyValue<'static>,
}

#[function("struct_of_all() -> struct StructOfAll")]
fn struct_of_all() -> StructOfAll {
    StructOfAll {
        // a: (),
        b: None,
        a: 0,
        c: 1,
        d: 2,
        e: 3,
        aa: 4,
        cc: 5,
        dd: 6,
        ee: 7,
        f: 4.0,
        g: 5.0,
        h: Decimal::new(6, 3),
        i: NaiveDate::from_ymd_opt(2022, 4, 8).unwrap(),
        j: NaiveTime::from_hms_micro_opt(12, 34, 56, 789_012).unwrap(),
        k: NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2022, 4, 8).unwrap(),
            NaiveTime::from_hms_micro_opt(12, 34, 56, 789_012).unwrap(),
        ),
        l: Interval {
            months: 7,
            days: 8,
            nanos: 9,
        },
        m: serde_json::json!({ "key": "value" }),
        n: "string".to_string(),
        o: vec![10, 11, 12],
        p: vec!["a".to_string(), "b".to_string()],
        q: KeyValue {
            key: "a",
            value: "b",
        },
    }
}

#[function("range(int) -> setof int")]
fn range(x: i32) -> impl Iterator<Item = i32> {
    0..x
}

#[function("json_array_elements(json) ->> json")]
fn json_array_elements(
    x: serde_json::Value,
) -> Result<impl Iterator<Item = serde_json::Value>, &'static str> {
    match x {
        serde_json::Value::Array(x) => Ok(x.into_iter()),
        _ => Err("not an array"),
    }
}

#[function(
    "many_args(int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int) -> int"
)]
#[allow(clippy::too_many_arguments)]
fn many_args(
    a: i32,
    b: i32,
    c: i32,
    d: i32,
    e: i32,
    f: i32,
    g: i32,
    h: i32,
    i: i32,
    j: i32,
    k: i32,
    l: i32,
    m: i32,
    n: i32,
    o: i32,
    p: i32,
) -> i32 {
    a + b + c + d + e + f + g + h + i + j + k + l + m + n + o + p
}

#[test]
#[allow(clippy::bool_assert_comparison)]
fn test_neg() {
    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = neg_int32_int32_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+
        | neg |
        +-----+
        | -1  |
        |     |
        +-----+"#]],
    );
}

#[test]
fn test_div() {
    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), Some(-1), None]);
    let arg1 = Int32Array::from(vec![Some(0), Some(-1), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = div_int32_int32_int32_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+------------------+
        | div | error            |
        +-----+------------------+
        |     | division by zero |
        | 1   |                  |
        |     |                  |
        +-----+------------------+"#]],
    );
}

#[test]
fn test_key_value() {
    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a=b", "??"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = key_value_string_struct_KeyValue_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | key_value          |
        +--------------------+
        | {key: a, value: b} |
        |                    |
        +--------------------+"#]],
    );
}

#[test]
fn test_key_values() {
    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a=b,c=d"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut stream = key_values_string_struct_KeyValue_eval(&input).unwrap();
    let output = stream.next().unwrap().unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+--------------------+
        | row | key_values         |
        +-----+--------------------+
        | 0   | {key: a, value: b} |
        | 0   | {key: c, value: d} |
        +-----+--------------------+"#]],
    );
}

#[test]
fn test_struct_of_all() {
    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![1]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = struct_of_all_struct_StructOfAll_eval(&input).unwrap();
    check(
        &[output],
        expect![[
            r#"+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| struct_of_all                                                                                                                                                                                                                                                        |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| {b: , a: 0, c: 1, d: 2, e: 3, aa: 4, cc: 5, dd: 6, ee: 7, f: 4.0, g: 5.0, h: 0.006, i: 2022-04-08, j: 12:34:56.789012, k: 2022-04-08T12:34:56.789012, l: 7 mons 8 days 0.000000009 secs, m: {"key":"value"}, n: string, o: 0a0b0c, p: [a, b], q: {key: a, value: b}} |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#
        ]],
    );
}

#[test]
fn test_split() {
    let schema = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
    let arg0 = StringArray::from(vec!["a,b"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = split_string_stringarray_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------+
        | split  |
        +--------+
        | [a, b] |
        +--------+"#]],
    );
}

#[test]
fn test_option_add() {
    let schema = Schema::new(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Int32, true),
    ]);
    let arg0 = Int32Array::from(vec![Some(1), Some(1), None, None]);
    let arg1 = Int32Array::from(vec![Some(1), None, Some(1), None]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = option_add_int32_int32_int32_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +------------+
        | option_add |
        +------------+
        | 2          |
        | 1          |
        |            |
        |            |
        +------------+"#]],
    );
}

#[test]
fn test_array_sum() {
    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::new_list(DataType::Int32, true),
        true,
    )]);
    let arg0 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(0), Some(1), Some(2)]),
        None,
        Some(vec![Some(3), None, Some(5)]),
        Some(vec![Some(6), Some(7)]),
    ]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = array_sum_int32array_int32_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----------+
        | array_sum |
        +-----------+
        | 3         |
        |           |
        | 8         |
        | 13        |
        +-----------+"#]],
    );
}

#[test]
fn test_temporal() {
    let schema = Schema::new(vec![
        Field::new("date", DataType::Date32, true),
        Field::new("time", DataType::Time64(TimeUnit::Microsecond), true),
    ]);
    let arg0 = Date32Array::from(vec![Date32Type::from_naive_date(
        NaiveDate::from_ymd_opt(2022, 4, 8).unwrap(),
    )]);
    let arg1 = Time64MicrosecondArray::from(vec![time_to_time64us(
        NaiveTime::from_hms_micro_opt(12, 34, 56, 789_012).unwrap(),
    )]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = datetime_date32_time64_timestamp_eval(&input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------------------+
        | datetime                   |
        +----------------------------+
        | 2022-04-08T12:34:56.789012 |
        +----------------------------+"#]],
    );
}

#[test]
fn test_decimal_add() {
    let schema = Schema::new(vec![decimal_field("a"), decimal_field("b")]);
    let arg0 = StringArray::from(vec!["0.0001"]);
    let arg1 = StringArray::from(vec!["0.0002"]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = add_decimal_decimal_decimal_eval(&input).unwrap();
    assert_eq!(output.schema().field(0), &decimal_field("add"));
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
fn test_json() {
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = to_json_int32_json_eval(&input).unwrap();
    assert_eq!(output.schema().field(0), &json_field("to_json"));
    check(
        &[output],
        expect![[r#"
        +---------+
        | to_json |
        +---------+
        | 1       |
        | null    |
        +---------+"#]],
    );
}

#[test]
fn test_range() {
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut stream = range_int32_int32_eval(&input).unwrap();
    assert_eq!(
        *stream.schema(),
        Schema::new(vec![
            Field::new("row", DataType::Int32, true),
            Field::new("range", DataType::Int32, true),
        ])
    );

    let output = stream.next().unwrap().unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+-------+
        | row | range |
        +-----+-------+
        | 0   | 0     |
        | 2   | 0     |
        | 2   | 1     |
        | 2   | 2     |
        +-----+-------+"#]],
    );

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![1000000]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    // for large set, the output is split into multiple batches
    let mut i = 0;
    for result in range_int32_int32_eval(&input).unwrap() {
        let output = result.unwrap();
        let array = output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for x in array {
            assert_eq!(x, Some(i));
            i += 1;
        }
    }
}

#[test]
fn test_json_array_elements() {
    let schema = Schema::new(vec![json_field("d")]);
    let arg0 = StringArray::from(vec![r#"[null,1,""]"#, "1"]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut stream = json_array_elements_json_json_eval(&input).unwrap();
    let output = stream.next().unwrap().unwrap();
    check(
        &[output],
        expect![[r#"
        +-----+---------------------+--------------+
        | row | json_array_elements | error        |
        +-----+---------------------+--------------+
        | 0   | null                |              |
        | 0   | 1                   |              |
        | 0   | ""                  |              |
        | 1   |                     | not an array |
        +-----+---------------------+--------------+"#]],
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
