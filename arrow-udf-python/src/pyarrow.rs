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
use pyo3::{types::PyString, IntoPy, PyObject, Python};
use std::sync::Arc;

macro_rules! get_pyobject {
    ($array_type: ty, $py:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_py($py)
    }};
}

/// Get array element as a python object.
pub fn get_pyobject(py: Python<'_>, array: &dyn Array, i: usize) -> Result<PyObject> {
    if array.is_null(i) {
        return Ok(py.None());
    }
    Ok(match array.data_type() {
        DataType::Null => py.None(),
        DataType::Boolean => get_pyobject!(BooleanArray, py, array, i),
        DataType::Int8 => get_pyobject!(Int8Array, py, array, i),
        DataType::Int16 => get_pyobject!(Int16Array, py, array, i),
        DataType::Int32 => get_pyobject!(Int32Array, py, array, i),
        DataType::Int64 => get_pyobject!(Int64Array, py, array, i),
        DataType::UInt8 => get_pyobject!(UInt8Array, py, array, i),
        DataType::UInt16 => get_pyobject!(UInt16Array, py, array, i),
        DataType::UInt32 => get_pyobject!(UInt32Array, py, array, i),
        DataType::UInt64 => get_pyobject!(UInt64Array, py, array, i),
        DataType::Float32 => get_pyobject!(Float32Array, py, array, i),
        DataType::Float64 => get_pyobject!(Float64Array, py, array, i),
        DataType::Utf8 => get_pyobject!(StringArray, py, array, i),
        DataType::Binary => get_pyobject!(BinaryArray, py, array, i),
        // json type
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let json_str = PyString::new(py, array.value(i));
            // XXX: it is slow to call eval every time
            let json_loads = py.eval("json.loads", None, None)?;
            json_loads.call1((json_str,))?.into()
        }
        _ => todo!(),
    })
}

macro_rules! build_array {
    (NullBuilder, $py:expr, $pyobjects:expr) => {{
        let mut builder = NullBuilder::with_capacity($pyobjects.len());
        for pyobj in $pyobjects {
            if pyobj.is_none($py) {
                builder.append_null();
            } else {
                builder.append_empty_value();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // primitive types
    ($builder_type: ty, $py:expr, $pyobjects:expr) => {{
        let mut builder = <$builder_type>::with_capacity($pyobjects.len());
        for pyobj in $pyobjects {
            if pyobj.is_none($py) {
                builder.append_null();
            } else {
                builder.append_value(pyobj.extract($py)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // string and bytea
    ($builder_type: ty, $elem_type: ty, $py:expr, $pyobjects:expr) => {{
        let mut builder = <$builder_type>::with_capacity($pyobjects.len(), 1024);
        for pyobj in $pyobjects {
            if pyobj.is_none($py) {
                builder.append_null();
            } else {
                builder.append_value(pyobj.extract::<$elem_type>($py)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

/// Build arrow array from python objects.
pub fn build_array(data_type: &DataType, py: Python<'_>, values: &[PyObject]) -> Result<ArrayRef> {
    match data_type {
        DataType::Null => build_array!(NullBuilder, py, values),
        DataType::Boolean => build_array!(BooleanBuilder, py, values),
        DataType::Int8 => build_array!(Int8Builder, py, values),
        DataType::Int16 => build_array!(Int16Builder, py, values),
        DataType::Int32 => build_array!(Int32Builder, py, values),
        DataType::Int64 => build_array!(Int64Builder, py, values),
        DataType::UInt8 => build_array!(UInt8Builder, py, values),
        DataType::UInt16 => build_array!(UInt16Builder, py, values),
        DataType::UInt32 => build_array!(UInt32Builder, py, values),
        DataType::UInt64 => build_array!(UInt64Builder, py, values),
        DataType::Float32 => build_array!(Float32Builder, py, values),
        DataType::Float64 => build_array!(Float64Builder, py, values),
        DataType::Utf8 => build_array!(StringBuilder, &str, py, values),
        DataType::Binary => build_array!(BinaryBuilder, &[u8], py, values),
        // json type
        DataType::LargeUtf8 => {
            let json_dumps = py.eval("json.dumps", None, None)?;
            let mut builder = LargeStringBuilder::with_capacity(values.len(), 1024);
            for val in values {
                if val.is_none(py) {
                    builder.append_null();
                    continue;
                };
                let json_str = json_dumps.call1((val,))?;
                builder.append_value(json_str.extract::<&str>()?);
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!(),
    }
}
