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

use arrow_array::{array::*, builder::*};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use pyo3::{exceptions::PyTypeError, types::PyAnyMethods, IntoPy, PyObject, PyResult, Python};
use std::{borrow::Cow, sync::Arc};

macro_rules! get_pyobject {
    ($array_type: ty, $py:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_py($py)
    }};
}

macro_rules! build_array {
    (NullBuilder, $py:expr, $pyobjects:expr) => {{
        let mut builder = NullBuilder::new();
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
    // view types
    ($builder_type: ty, $elem_type: ty, $py:expr, $pyobjects:expr, $dummy: expr) => {{
        let mut builder = <$builder_type>::with_capacity($pyobjects.len());
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

macro_rules! build_json_array {
    ($py:expr, $pyobjects:expr) => {{
        let json_dumps = $py.eval_bound("json.dumps", None, None)?;
        let mut builder = StringBuilder::with_capacity($pyobjects.len(), 1024);
        for pyobj in $pyobjects {
            if pyobj.is_none($py) {
                builder.append_null();
                continue;
            };
            let json_str = json_dumps.call1((pyobj,))?;
            builder.append_value(json_str.extract::<&str>()?);
        }
        Ok(Arc::new(builder.finish()))
    }};
}

#[derive(Debug, Clone)]
pub struct Converter {
    arrow_extension_key: Cow<'static, str>,
    json_extension_name: Cow<'static, str>,
    decimal_extension_name: Cow<'static, str>,
    pickle_extension_name: Cow<'static, str>,
}

impl Converter {
    pub fn new() -> Self {
        Self {
            arrow_extension_key: "ARROW:extension:name".into(),
            json_extension_name: "arrowudf.json".into(),
            decimal_extension_name: "arrowudf.decimal".into(),
            pickle_extension_name: "arrowudf.pickle".into(),
        }
    }

    #[allow(dead_code)]
    pub fn set_arrow_extension_key(&mut self, key: &str) {
        self.arrow_extension_key = key.to_string().into();
    }

    #[allow(dead_code)]
    pub fn set_json_extension_name(&mut self, name: &str) {
        self.json_extension_name = name.to_string().into();
    }

    #[allow(dead_code)]
    pub fn set_decimal_extension_name(&mut self, name: &str) {
        self.decimal_extension_name = name.to_string().into();
    }

    /// Get array element as a python object.
    pub fn get_pyobject(
        &self,
        py: Python<'_>,
        field: &Field,
        array: &dyn Array,
        i: usize,
    ) -> PyResult<PyObject> {
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
            // TODO: make this a macro
            DataType::Utf8 => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.json_extension_name => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let string = array.value(i);
                    // XXX: it is slow to call eval every time
                    let json_loads = py.eval_bound("json.loads", None, None)?;
                    json_loads.call1((string,))?.into()
                }
                Some(x) if x == &self.decimal_extension_name => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let string = array.value(i);
                    // XXX: it is slow to call eval every time
                    let decimal_constructor = py.eval_bound("decimal.Decimal", None, None)?;
                    decimal_constructor.call1((string,))?.into()
                }
                _ => get_pyobject!(StringArray, py, array, i),
            },
            DataType::LargeUtf8 => get_pyobject!(LargeStringArray, py, array, i),
            DataType::Binary => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.pickle_extension_name => {
                    let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let bytes = array.value(i);
                    let pickle_loads = py.eval_bound("pickle.loads", None, None)?;
                    pickle_loads.call1((bytes,))?.into()
                }
                _ => get_pyobject!(BinaryArray, py, array, i),
            },
            DataType::LargeBinary => get_pyobject!(LargeBinaryArray, py, array, i),
            DataType::Utf8View => get_pyobject!(StringViewArray, py, array, i),
            DataType::BinaryView => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.pickle_extension_name => {
                    let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                    let bytes = array.value(i);
                    let pickle_loads = py.eval_bound("pickle.loads", None, None)?;
                    pickle_loads.call1((bytes,))?.into()
                }
                _ => get_pyobject!(BinaryViewArray, py, array, i),
            },

            DataType::List(field) => {
                let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let list = array.value(i);
                let mut values = Vec::with_capacity(list.len());
                for j in 0..list.len() {
                    values.push(self.get_pyobject(py, field, list.as_ref(), j)?);
                }
                values.into_py(py)
            }
            DataType::Struct(fields) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let object = py.eval_bound("Struct()", None, None)?;
                for (j, field) in fields.iter().enumerate() {
                    let value = self.get_pyobject(py, field, array.column(j).as_ref(), i)?;
                    object.setattr(field.name().as_str(), value)?;
                }
                object.into()
            }
            other => {
                return Err(PyTypeError::new_err(format!(
                    "Unimplemented datatype {}",
                    other
                )))
            }
        })
    }

    /// Build arrow array from python objects.
    pub fn build_array(
        &self,
        field: &Field,
        py: Python<'_>,
        values: &[PyObject],
    ) -> PyResult<ArrayRef> {
        match field.data_type() {
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
            DataType::Utf8 => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.json_extension_name => {
                    build_json_array!(py, values)
                }
                Some(x) if x == &self.decimal_extension_name => {
                    let mut builder = StringBuilder::with_capacity(values.len(), 1024);
                    for val in values {
                        if val.is_none(py) {
                            builder.append_null();
                        } else {
                            builder.append_value(val.to_string());
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => build_array!(StringBuilder, &str, py, values),
            },
            DataType::LargeUtf8 => build_array!(LargeStringBuilder, &str, py, values),
            DataType::Binary => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.pickle_extension_name => {
                    let pickle_dumps = py.eval_bound("pickle.dumps", None, None)?;

                    let mut builder = BinaryBuilder::with_capacity(1, 0);
                    for value in values {
                        let pickled_value = pickle_dumps.call1((value,))?;
                        if pickled_value.is_none() {
                            builder.append_null();
                        } else {
                            let bytes = pickled_value.extract::<&[u8]>()?;
                            builder.append_value(bytes);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => build_array!(BinaryBuilder, &[u8], py, values),
            },
            DataType::LargeBinary => {
                match field.metadata().get(self.arrow_extension_key.as_ref()) {
                    Some(x) if x == &self.json_extension_name => {
                        build_json_array!(py, values)
                    }
                    _ => build_array!(LargeBinaryBuilder, &[u8], py, values),
                }
            }
            DataType::Utf8View => build_array!(StringViewBuilder, &str, py, values, 1),
            DataType::BinaryView => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == &self.json_extension_name => {
                    build_json_array!(py, values)
                }
                _ => build_array!(BinaryViewBuilder, &[u8], py, values, 1),
            },
            // list
            DataType::List(inner) => {
                // flatten lists
                let mut flatten_values = vec![];
                let mut offsets = Vec::<i32>::with_capacity(values.len() + 1);
                offsets.push(0);
                for val in values {
                    if !val.is_none(py) {
                        let array = val.bind(py);
                        flatten_values.reserve(array.len()?);
                        for elem in array.iter()? {
                            flatten_values.push(elem?.into());
                        }
                    }
                    offsets.push(flatten_values.len() as i32);
                }
                let values_array = self.build_array(inner, py, &flatten_values)?;
                let nulls = values.iter().map(|v| !v.is_none(py)).collect();
                Ok(Arc::new(ListArray::new(
                    inner.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values_array,
                    Some(nulls),
                )))
            }
            // large list
            DataType::LargeList(inner) => {
                // flatten lists
                let mut flatten_values = vec![];
                let mut offsets = Vec::<i64>::with_capacity(values.len() + 1);
                offsets.push(0);
                for val in values {
                    if !val.is_none(py) {
                        let array = val.bind(py);
                        flatten_values.reserve(array.len()?);
                        for elem in array.iter()? {
                            flatten_values.push(elem?.into());
                        }
                    }
                    offsets.push(flatten_values.len() as i64);
                }
                let values_array = self.build_array(inner, py, &flatten_values)?;
                let nulls = values.iter().map(|v| !v.is_none(py)).collect();
                Ok(Arc::new(LargeListArray::new(
                    inner.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values_array,
                    Some(nulls),
                )))
            }
            DataType::Struct(fields) => {
                let mut arrays = Vec::with_capacity(fields.len());
                for field in fields {
                    let mut field_values = Vec::with_capacity(values.len());
                    for val in values {
                        let v = if val.is_none(py) {
                            py.None()
                        } else if let Ok(value) = val.getattr(py, field.name().as_str()) {
                            value
                        } else {
                            val.bind(py).get_item(field.name().as_str())?.into()
                        };
                        field_values.push(v);
                    }
                    arrays.push(self.build_array(field, py, &field_values)?);
                }
                let nulls = values.iter().map(|v| !v.is_none(py)).collect();
                Ok(Arc::new(StructArray::new(
                    fields.clone(),
                    arrays,
                    Some(nulls),
                )))
            }
            other => Err(PyTypeError::new_err(format!(
                "Unimplemented datatype {}",
                other
            ))),
        }
    }
}
