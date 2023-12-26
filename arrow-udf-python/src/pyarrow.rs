//! Convert arrow array from/to python objects.

use anyhow::Result;
use arrow_array::{array::*, builder::*};
use arrow_schema::DataType;
use pyo3::{IntoPy, PyObject, Python};
use std::sync::Arc;

macro_rules! get_pyobject {
    ($array_type: ty, $py:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_py($py)
    }};
}

/// Get array element as a python object.
pub fn get_pyobject(py: Python<'_>, array: &dyn Array, i: usize) -> PyObject {
    if array.is_null(i) {
        return py.None();
    }
    match array.data_type() {
        DataType::Null => py.None(),
        DataType::Boolean => get_pyobject!(BooleanArray, py, array, i),
        DataType::Int16 => get_pyobject!(Int16Array, py, array, i),
        DataType::Int32 => get_pyobject!(Int32Array, py, array, i),
        DataType::Int64 => get_pyobject!(Int64Array, py, array, i),
        DataType::Float32 => get_pyobject!(Float32Array, py, array, i),
        DataType::Float64 => get_pyobject!(Float64Array, py, array, i),
        DataType::Utf8 => get_pyobject!(StringArray, py, array, i),
        DataType::Binary => get_pyobject!(BinaryArray, py, array, i),
        _ => todo!(),
    }
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
pub fn build_array(
    data_type: &DataType,
    py: Python<'_>,
    pyobjects: &[PyObject],
) -> Result<ArrayRef> {
    match data_type {
        DataType::Null => build_array!(NullBuilder, py, pyobjects),
        DataType::Boolean => build_array!(BooleanBuilder, py, pyobjects),
        DataType::Int16 => build_array!(Int16Builder, py, pyobjects),
        DataType::Int32 => build_array!(Int32Builder, py, pyobjects),
        DataType::Int64 => build_array!(Int64Builder, py, pyobjects),
        DataType::Float32 => build_array!(Float32Builder, py, pyobjects),
        DataType::Float64 => build_array!(Float64Builder, py, pyobjects),
        DataType::Utf8 => build_array!(StringBuilder, &str, py, pyobjects),
        DataType::Binary => build_array!(BinaryBuilder, &[u8], py, pyobjects),
        _ => todo!(),
    }
}
