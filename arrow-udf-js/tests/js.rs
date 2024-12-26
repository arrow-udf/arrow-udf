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

use std::{sync::Arc, time::Duration};

use arrow_array::{
    types::*, ArrayRef, BinaryArray, Date32Array, Decimal128Array, Decimal256Array, Int32Array,
    LargeBinaryArray, LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::i256;
use arrow_cast::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_schema::{DataType, Field, Schema};
use arrow_udf_js::{CallMode, Runtime};
use expect_test::{expect, Expect};

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
            DataType::Int32,
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
            DataType::Utf8,
            CallMode::CalledOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(5), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_string", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-----------+
        | to_string |
        +-----------+
        | 5         |
        | null      |
        +-----------+"#]],
    );
}

#[test]
fn test_time_zone() {
    test_zone_in(None);
    test_zone_in(Some("+00:00".into()));
    test_zone_in(Some("-03:00".into()));
    test_zone_out(
        None,
        expect![[r#"
        +-------------------------+
        | from_unix               |
        +-------------------------+
        | 1970-01-01T00:00:00.123 |
        | 1970-01-01T00:00:00     |
        +-------------------------+"#]],
    );
    test_zone_out(
        Some("+00:00".into()),
        expect![[r#"
        +--------------------------+
        | from_unix                |
        +--------------------------+
        | 1970-01-01T00:00:00.123Z |
        | 1970-01-01T00:00:00Z     |
        +--------------------------+"#]],
    );
    test_zone_out(
        Some("-03:00".into()),
        expect![[r#"
        +-------------------------------+
        | from_unix                     |
        +-------------------------------+
        | 1969-12-31T21:00:00.123-03:00 |
        | 1969-12-31T21:00:00-03:00     |
        +-------------------------------+"#]],
    );
}

fn test_zone_in(zone: Option<Arc<str>>) {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function to_unix(t) {
            return t?.valueOf();
        }
    "#;
    runtime
        .add_function(
            "to_unix",
            DataType::Float64,
            CallMode::CalledOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "t",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, zone.clone()),
        true,
    )]);
    let arg0 = TimestampMicrosecondArray::from(vec![Some(123456), None]).with_timezone_opt(zone);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_unix", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +---------+
        | to_unix |
        +---------+
        | 123.0   |
        |         |
        +---------+"#]],
    );
}

fn test_zone_out(zone: Option<Arc<str>>, expected: Expect) {
    let mut runtime = Runtime::new().unwrap();

    let js_code = r#"
        export function from_unix(ms) {
            return new Date(ms);
        }
    "#;
    runtime
        .add_function(
            "from_unix",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, zone.clone()),
            CallMode::CalledOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("ms", DataType::Float64, true)]);
    let arg0 = arrow_array::Float64Array::from(vec![Some(123.25), None]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("from_unix", &input).unwrap();
    check(&[output], expected);
}

#[test]
fn test_concat() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "concat",
            DataType::Binary,
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
    check(
        &[output],
        expect![[r#"
        +----------------------+
        | concat               |
        +----------------------+
        | 68656c6c6f776f726c64 |
        +----------------------+"#]],
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
            export function json_array_access(array, i) {
                return array[i];
            }
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
fn test_json_stringify() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "json_stringify",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function json_stringify(object) {
                return JSON.stringify(object);
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![json_field("json")]);
    let arg0 = StringArray::from(vec![r#"[1, null, ""]"#]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("json_stringify", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | json_stringify |
        +----------------+
        | [1,null,""]    |
        +----------------+"#]],
    );
}

#[test]
fn test_binary_json_stringify() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "add_element",
            binary_json_field("object"),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function add_element(object) {
                object.push(10);
                return object;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![binary_json_field("json")]);
    let arg0 = BinaryArray::from(vec![(r#"[1, null, ""]"#).as_bytes()]);
    let input = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("add_element", &input).unwrap();
    let row = output
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(std::str::from_utf8(row).unwrap(), r#"[1,null,"",10]"#);
}

#[test]
fn test_large_binary_json_stringify() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "add_element",
            large_binary_json_field("object"),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function add_element(object) {
                object.push(10);
                return object;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![large_binary_json_field("json")]);
    let arg0 = LargeBinaryArray::from(vec![(r#"[1, null, ""]"#).as_bytes()]);
    let input = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("add_element", &input).unwrap();
    let row = output
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(std::str::from_utf8(row).unwrap(), r#"[1,null,"",10]"#);
}

#[test]
fn test_large_string_as_string() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "string_length",
            DataType::LargeUtf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function string_length(s) {
                return "string length is " + s.length;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("s", DataType::LargeUtf8, true)]);
    let arg0 = LargeStringArray::from(vec![r#"hello"#]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("string_length", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------------------+
        | string_length      |
        +--------------------+
        | string length is 5 |
        +--------------------+"#]],
    );
}

#[test]
fn test_decimal128() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "decimal128_add",
            DataType::Decimal128(19, 2),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function decimal128_add(a, b) {
                return a + b + BigDecimal('0.000001');
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Decimal128(19, 2), true),
        Field::new("b", DataType::Decimal128(19, 2), true),
    ]);
    let arg0 = Decimal128Array::from(vec![Some(100), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let arg1 = Decimal128Array::from(vec![Some(201), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal128_add", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | decimal128_add |
        +----------------+
        | 3.01           |
        |                |
        +----------------+"#]],
    );
}

#[test]
fn test_decimal256() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "decimal256_add",
            DataType::Decimal256(19, 2),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function decimal256_add(a, b) {
                return a + b + BigDecimal('0.000001');
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("a", DataType::Decimal256(19, 2), true),
        Field::new("b", DataType::Decimal256(19, 2), true),
    ]);
    let arg0 = Decimal256Array::from(vec![Some(i256::from(100)), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let arg1 = Decimal256Array::from(vec![Some(i256::from(201)), None])
        .with_precision_and_scale(19, 2)
        .unwrap();
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal256_add", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | decimal256_add |
        +----------------+
        | 3.01           |
        |                |
        +----------------+"#]],
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
            export function decimal_add(a, b) {
                return a + b;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![decimal_field("a"), decimal_field("b")]);
    let arg0 = StringArray::from(vec!["0.0001"]);
    let arg1 = StringArray::from(vec!["0.0002"]);
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();

    let output = runtime.call("decimal_add", &input).unwrap();
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
fn test_timestamp_second_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
        true,
    )]);
    let arg0 = TimestampSecondArray::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[test]
fn test_timestamp_millisecond_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        true,
    )]);
    let arg0 = TimestampMillisecondArray::from(vec![Some(1000), None, Some(3000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[test]
fn test_timestamp_microsecond_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        true,
    )]);
    let arg0 = TimestampMicrosecondArray::from(vec![Some(1000000), None, Some(3000000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +---------------------+
        | timestamp_array     |
        +---------------------+
        | 1970-01-01T00:00:01 |
        |                     |
        | 1970-01-01T00:00:03 |
        +---------------------+"#]],
    );
}

#[test]
fn test_timestamp_nanosecond_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "timestamp_array",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            CallMode::ReturnNullOnNullInput,
            r#"
            export function timestamp_array(a) {
                return a;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new(
        "x",
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        true,
    )]);
    let arg0 = TimestampNanosecondArray::from(vec![Some(1000000), None, Some(3000000)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("timestamp_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-------------------------+
        | timestamp_array         |
        +-------------------------+
        | 1970-01-01T00:00:00.001 |
        |                         |
        | 1970-01-01T00:00:00.003 |
        +-------------------------+"#]],
    );
}

#[test]
fn test_date32_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "date_array",
            DataType::Date32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function date_array(a) {
                return a;
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Date32, true)]);
    let arg0 = Date32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("date_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +------------+
        | date_array |
        +------------+
        | 1970-01-02 |
        |            |
        | 1970-01-04 |
        +------------+"#]],
    );
}

#[test]
fn test_typed_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "object_type",
            DataType::Utf8,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function object_type(a) {
                return Object.prototype.toString.call(a);
            }
            "#,
        )
        .unwrap();

    /// Generate a record batch with a single column of type `List<T>`.
    fn array_input<T: ArrowPrimitiveType>() -> RecordBatch {
        let schema = Schema::new(vec![Field::new(
            "x",
            DataType::new_list(T::DATA_TYPE, true),
            true,
        )]);
        let arg0 =
            ListArray::from_iter_primitive::<T, _, _>(vec![Some(vec![Some(Default::default())])]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap()
    }

    let cases = [
        // (input, JS object type)
        (array_input::<Int8Type>(), "Int8Array"),
        (array_input::<Int16Type>(), "Int16Array"),
        (array_input::<Int32Type>(), "Int32Array"),
        (array_input::<Int64Type>(), "BigInt64Array"),
        (array_input::<UInt8Type>(), "Uint8Array"),
        (array_input::<UInt16Type>(), "Uint16Array"),
        (array_input::<UInt32Type>(), "Uint32Array"),
        (array_input::<UInt64Type>(), "BigUint64Array"),
        (array_input::<Float32Type>(), "Float32Array"),
        (array_input::<Float64Type>(), "Float64Array"),
    ];

    for (input, expected) in cases.iter() {
        let output = runtime.call("object_type", input).unwrap();
        let object_type = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(object_type, format!("[object {}]", expected));
    }
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
            export function to_array(x) {
                if(x == null) {
                    return null;
                }
                return [x];
            }
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
fn test_return_large_array() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "to_large_array",
            DataType::new_large_list(DataType::Int32, true),
            CallMode::CalledOnNullInput,
            r#"
            export function to_large_array(x) {
                if(x == null) {
                    return null;
                }
                return [x, x+1, x+2];
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("to_large_array", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +----------------+
        | to_large_array |
        +----------------+
        | [1, 2, 3]      |
        |                |
        | [3, 4, 5]      |
        +----------------+"#]],
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
            export function key_value(s) {
                const [key, value] = s.split("=", 2);
                return {key, value};
            }
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
            export function to_json(object) {
                return object;
            }
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
        +---------------------------+
        | to_json                   |
        +---------------------------+
        | {"key":"a","value":"b"}   |
        | {"key":null,"value":null} |
        +---------------------------+"#]],
    );
}

#[test]
fn test_range() {
    let mut runtime = Runtime::new().unwrap();

    runtime
        .add_function(
            "range",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            r#"
            export function* range(n) {
                for (let i = 0; i < n; i++) {
                    yield i;
                }
            }
            "#,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let mut outputs = runtime.call_table_function("range", &input, 2).unwrap();

    assert_eq!(outputs.schema().field(0).name(), "row");
    assert_eq!(outputs.schema().field(1).name(), "range");
    assert_eq!(outputs.schema().field(1).data_type(), &DataType::Int32);

    let o1 = outputs.next().unwrap().unwrap();
    let o2 = outputs.next().unwrap().unwrap();
    assert_eq!(o1.num_rows(), 2);
    assert_eq!(o2.num_rows(), 2);
    assert!(outputs.next().is_none());

    check(
        &[o1, o2],
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
            export function create_state() {
                return {sum: 0, weight: 0};
            }
            export function accumulate(state, value, weight) {
                state.sum += value * weight;
                state.weight += weight;
                return state;
            }
            export function retract(state, value, weight) {
                state.sum -= value * weight;
                state.weight -= weight;
                return state;
            }
            export function merge(state1, state2) {
                state1.sum += state2.sum;
                state1.weight += state2.weight;
                return state1;
            }
            export function finish(state) {
                return state.sum / state.weight;
            }
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
fn test_timeout() {
    let mut runtime = Runtime::new().unwrap();
    runtime.set_timeout(Some(Duration::from_millis(1)));

    let js_code = r#"
        export function square(x) {
            let sum = 0;
            for (let i = 0; i < x; i++) {
                sum += x;
            }
            return sum;
        }
    "#;
    runtime
        .add_function(
            "square",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![100]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("square", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +--------+
        | square |
        +--------+
        | 10000  |
        +--------+"#]],
    );

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![i32::MAX]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let err = runtime.call("square", &input).unwrap_err();
    assert!(format!("{err:?}").contains("interrupted"))
}

#[test]
fn test_memory_limit() {
    let mut runtime = Runtime::new().unwrap();
    runtime.set_memory_limit(Some(1 << 20)); // 1MB

    let js_code = r#"
        export function alloc(x) {
            new Array(x).fill(0);
            return x;
        }
    "#;
    runtime
        .add_function(
            "alloc",
            DataType::Int32,
            CallMode::ReturnNullOnNullInput,
            js_code,
        )
        .unwrap();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![100]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let output = runtime.call("alloc", &input).unwrap();
    check(
        &[output],
        expect![[r#"
        +-------+
        | alloc |
        +-------+
        | 100   |
        +-------+"#]],
    );

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let arg0 = Int32Array::from(vec![1 << 20]);
    let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();

    let err = runtime.call("alloc", &input).unwrap_err();
    assert!(format!("{err:?}").contains("out of memory"))
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
export function echo(x) {
    return x + "!"
}
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

/// assert Runtime is Send and Sync
#[test]
fn test_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Runtime>();
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

/// Returns a field with JSON type.
fn binary_json_field(name: &str) -> Field {
    Field::new(name, DataType::Binary, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with JSON type.
fn large_binary_json_field(name: &str) -> Field {
    Field::new(name, DataType::LargeBinary, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
}

/// Returns a field with decimal type.
fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
}
