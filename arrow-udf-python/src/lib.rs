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

use anyhow::Result;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pyo3::types::{PyModule, PyTuple};
use pyo3::{PyObject, PyResult, Python};
use std::sync::Arc;

mod ffi;
mod pyarrow;

/// A Python UDF.
pub struct Function {
    name: String,
    function: PyObject,
    return_type: DataType,
    mode: CallMode,
}

impl Function {
    /// Create a new Python UDF runtime from a Python code.
    pub fn new(name: &str, return_type: DataType, mode: CallMode, code: &str) -> Result<Self> {
        pyo3::prepare_freethreaded_python();
        let function = Python::with_gil(|py| -> PyResult<PyObject> {
            Ok(PyModule::from_code(py, code, "", "")?.getattr(name)?.into())
        })?;
        Ok(Self {
            name: name.into(),
            function,
            return_type,
            mode,
        })
    }

    /// Call the Python UDF.
    pub fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // convert each row to python objects and call the function
        let array = Python::with_gil(|py| -> Result<ArrayRef> {
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                for column in input.columns() {
                    let pyobj = pyarrow::get_pyobject(py, column, i);
                    row.push(pyobj);
                }
                if self.mode == CallMode::ReturnNullOnNullInput && row.iter().any(|v| v.is_none(py))
                {
                    results.push(py.None());
                    continue;
                }
                let args = PyTuple::new(py, row.drain(..));
                let result = self.function.call1(py, args)?;
                results.push(result);
            }
            let result = pyarrow::build_array(&self.return_type, py, &results)?;
            Ok(result)
        })?;
        let schema = Schema::new(vec![Field::new(
            &self.name,
            array.data_type().clone(),
            true,
        )]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
    }
}

/// Whether the function will be called when some of its arguments are null.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum CallMode {
    /// The function will be called normally when some of its arguments are null.
    /// It is then the function author's responsibility to check for null values if necessary and respond appropriately.
    #[default]
    CalledOnNullInput,

    /// The function always returns null whenever any of its arguments are null.
    /// If this parameter is specified, the function is not executed when there are null arguments;
    /// instead a null result is assumed automatically.
    ReturnNullOnNullInput,
}
