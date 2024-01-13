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

//! Convert arrow array from/to python objects.

use anyhow::Result;
use arrow_array::{array::*, builder::*};
use arrow_schema::DataType;
use rquickjs::{Ctx, Error, FromJs, IntoJs, Value};
use std::sync::Arc;

macro_rules! get_jsvalue {
    ($array_type: ty, $ctx:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_js($ctx)
    }};
}

/// Get array element as a JS Value.
pub fn get_jsvalue<'a>(ctx: &Ctx<'a>, array: &dyn Array, i: usize) -> Result<Value<'a>, Error> {
    if array.is_null(i) {
        return Ok(Value::new_null(ctx.clone()));
    }
    match array.data_type() {
        DataType::Null => Ok(Value::new_null(ctx.clone())),
        DataType::Boolean => get_jsvalue!(BooleanArray, ctx, array, i),
        DataType::Int8 => get_jsvalue!(Int8Array, ctx, array, i),
        DataType::Int16 => get_jsvalue!(Int16Array, ctx, array, i),
        DataType::Int32 => get_jsvalue!(Int32Array, ctx, array, i),
        DataType::Int64 => get_jsvalue!(Int64Array, ctx, array, i),
        DataType::UInt8 => get_jsvalue!(UInt8Array, ctx, array, i),
        DataType::UInt16 => get_jsvalue!(UInt16Array, ctx, array, i),
        DataType::UInt32 => get_jsvalue!(UInt32Array, ctx, array, i),
        DataType::UInt64 => get_jsvalue!(UInt64Array, ctx, array, i),
        DataType::Float32 => get_jsvalue!(Float32Array, ctx, array, i),
        DataType::Float64 => get_jsvalue!(Float64Array, ctx, array, i),
        DataType::Utf8 => get_jsvalue!(StringArray, ctx, array, i),
        DataType::Binary => get_jsvalue!(BinaryArray, ctx, array, i),
        // json type
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            ctx.json_parse(array.value(i))
        }
        _ => todo!(),
    }
}

macro_rules! build_array {
    (NullBuilder, $ctx:expr, $values:expr) => {{
        let mut builder = NullBuilder::with_capacity($values.len());
        for pyobj in $values {
            if pyobj.is_null() {
                builder.append_null();
            } else {
                builder.append_empty_value();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // primitive types
    ($builder_type: ty, $ctx:expr, $values:expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for val in $values {
            if val.is_null() {
                builder.append_null();
            } else {
                builder.append_value(FromJs::from_js($ctx, val)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // string and bytea
    ($builder_type: ty, $elem_type: ty, $ctx:expr, $values:expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len(), 1024);
        for val in $values {
            if val.is_null() {
                builder.append_null();
            } else {
                builder.append_value(<$elem_type>::from_js($ctx, val)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

/// Build arrow array from JS objects.
pub fn build_array<'a>(
    data_type: &DataType,
    ctx: &Ctx<'a>,
    values: Vec<Value<'a>>,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Null => build_array!(NullBuilder, ctx, values),
        DataType::Boolean => build_array!(BooleanBuilder, ctx, values),
        DataType::Int8 => build_array!(Int8Builder, ctx, values),
        DataType::Int16 => build_array!(Int16Builder, ctx, values),
        DataType::Int32 => build_array!(Int32Builder, ctx, values),
        DataType::Int64 => build_array!(Int64Builder, ctx, values),
        DataType::UInt8 => build_array!(UInt8Builder, ctx, values),
        DataType::UInt16 => build_array!(UInt16Builder, ctx, values),
        DataType::UInt32 => build_array!(UInt32Builder, ctx, values),
        DataType::UInt64 => build_array!(UInt64Builder, ctx, values),
        DataType::Float32 => build_array!(Float32Builder, ctx, values),
        DataType::Float64 => build_array!(Float64Builder, ctx, values),
        DataType::Utf8 => build_array!(StringBuilder, String, ctx, values),
        DataType::Binary => build_array!(BinaryBuilder, Vec::<u8>, ctx, values),
        DataType::LargeUtf8 => {
            let mut builder = LargeStringBuilder::with_capacity(values.len(), 1024);
            for val in values {
                if val.is_null() {
                    builder.append_null();
                } else if let Some(s) = ctx.json_stringify(val)? {
                    builder.append_value(s.to_string()?);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!(),
    }
}
