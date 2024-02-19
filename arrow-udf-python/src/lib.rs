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

use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pyo3::types::{PyModule, PyTuple};
use pyo3::{PyObject, PyResult, Python};
use std::collections::HashMap;
use std::sync::Arc;

mod ffi;
mod pyarrow;

/// The Python UDF runtime.
pub struct Runtime {
    functions: HashMap<String, Function>,
}

/// A Python UDF.
pub struct Function {
    function: PyObject,
    return_type: DataType,
    mode: CallMode,
}

impl Runtime {
    /// Create a new Python UDF runtime.
    pub fn new() -> Result<Self> {
        Ok(Self {
            functions: HashMap::new(),
        })
    }

    /// Add a new function from Python code.
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: DataType,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        let function = Python::with_gil(|py| -> PyResult<PyObject> {
            Ok(PyModule::from_code(py, code, "", "")?.getattr(name)?.into())
        })?;
        let function = Function {
            function,
            return_type,
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Call the Python UDF.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        // convert each row to python objects and call the function
        let array = Python::with_gil(|py| -> Result<ArrayRef> {
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                row.clear();
                for column in input.columns() {
                    let pyobj = pyarrow::get_pyobject(py, column, i);
                    row.push(pyobj);
                }
                if function.mode == CallMode::ReturnNullOnNullInput
                    && row.iter().any(|v| v.is_none(py))
                {
                    results.push(py.None());
                    continue;
                }
                let args = PyTuple::new(py, row.drain(..));
                let result = function.function.call1(py, args)?;
                results.push(result);
            }
            let result = pyarrow::build_array(&function.return_type, py, &results)?;
            Ok(result)
        })?;
        let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
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
