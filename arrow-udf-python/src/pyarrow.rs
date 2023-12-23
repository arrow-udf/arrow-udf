//! Convert arrow array from/to python objects.

use anyhow::Result;
use arrow_array::{array::*, builder::*};
use arrow_schema::DataType;
use pyo3::{IntoPy, PyObject, Python};
use std::sync::Arc;

/// Get array element as a python object.
pub fn get_pyobject<'a>(py: Python<'a>, array: &dyn Array, i: usize) -> PyObject {
    if array.is_null(i) {
        return py.None();
    }
    match array.data_type() {
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            array.value(i).into_py(py)
        }
        _ => todo!(),
    }
}

/// Build arrow array from python objects.
pub fn build_array(
    data_type: &DataType,
    py: Python<'_>,
    pyobjects: &[PyObject],
) -> Result<ArrayRef> {
    let len = pyobjects.len();
    match data_type {
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(len);
            for pyobj in pyobjects {
                if pyobj.is_none(py) {
                    builder.append_null();
                } else {
                    builder.append_value(pyobj.extract::<i32>(py)?);
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!(),
    }
}
