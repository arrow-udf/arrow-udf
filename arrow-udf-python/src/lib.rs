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

#![doc = include_str!("../README.md")]

// Notice for developers:
// This library uses the sub-interpreter and per-interpreter GIL introduced in Python 3.12
// to support concurrent execution of different functions in multiple threads.
// However, pyo3 has not yet safely supported sub-interpreter. We use the raw FFI API of pyo3 to implement sub-interpreter.
// Therefore, special attention is needed:
// **All PyObject created in a sub-interpreter must be destroyed in the same sub-interpreter.**
// Otherwise, it will cause a crash the next time Python is called.
// Special attention is needed for PyErr in PyResult.
// Remember to convert `PyErr` using the `pyerr_to_anyhow` function before passing it out of the sub-interpreter.

use self::interpreter::SubInterpreter;
use anyhow::{Context, Result};
use arrow_array::builder::{ArrayBuilder, Int32Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use into_field::IntoField;
use pyo3::types::{PyAnyMethods, PyIterator, PyModule, PyTuple};
use pyo3::{Py, PyObject};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

// #[cfg(Py_3_12)]
mod interpreter;
mod into_field;
mod pyarrow;

/// The Python UDF runtime.
///
/// Each runtime owns a Python interpreter with its own GIL.
pub struct Runtime {
    interpreter: SubInterpreter,
    functions: HashMap<String, Function>,
    aggregates: HashMap<String, Aggregate>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("functions", &self.functions.keys())
            .finish()
    }
}

/// A user defined function.
struct Function {
    function: PyObject,
    return_field: FieldRef,
    mode: CallMode,
}

/// A user defined aggregate function.
struct Aggregate {
    state_field: FieldRef,
    output_field: FieldRef,
    mode: CallMode,
    create_state: PyObject,
    accumulate: PyObject,
    finish: PyObject,
}

/// A builder for `Runtime`.
#[derive(Default, Debug)]
pub struct Builder {
    sandboxed: bool,
    removed_symbols: Vec<String>,
}

impl Builder {
    /// Set whether the runtime is sandboxed.
    ///
    /// When sandboxed, only a limited set of modules can be imported, and some built-in functions are disabled.
    /// This is useful for running untrusted code.
    ///
    /// Allowed modules: `json`, `decimal`, `re`, `math`, `datetime`, `time`.
    ///
    /// Disallowed builtins: `breakpoint`, `exit`, `eval`, `help`, `input`, `open`, `print`.
    ///
    /// The default is `false`.
    pub fn sandboxed(mut self, sandboxed: bool) -> Self {
        self.sandboxed = sandboxed;
        self.remove_symbol("__builtins__.breakpoint")
            .remove_symbol("__builtins__.exit")
            .remove_symbol("__builtins__.eval")
            .remove_symbol("__builtins__.help")
            .remove_symbol("__builtins__.input")
            .remove_symbol("__builtins__.open")
            .remove_symbol("__builtins__.print")
    }

    /// Remove a symbol from builtins.
    ///
    /// # Examples
    ///
    /// ```
    /// # use arrow_udf_python::Runtime;
    /// let builder = Runtime::builder().remove_symbol("__builtins__.eval");
    /// ```
    pub fn remove_symbol(mut self, symbol: &str) -> Self {
        self.removed_symbols.push(symbol.to_string());
        self
    }

    /// Build the `Runtime`.
    pub fn build(self) -> Result<Runtime> {
        let interpreter = SubInterpreter::new()?;
        interpreter.run(
            r#"
# internal use for json types
import json

# an internal class used for struct input arguments
class Struct:
    pass
"#,
        )?;
        if self.sandboxed {
            let mut script = r#"
# limit the modules that can be imported
original_import = __builtins__.__import__

def limited_import(name, globals=None, locals=None, fromlist=(), level=0):
    # FIXME: 'sys' should not be allowed, but it is required by 'decimal'
    # FIXME: 'time.sleep' should not be allowed, but 'time' is required by 'datetime'
    allowlist = (
        'json',
        'decimal',
        're',
        'math',
        'datetime',
        'time',
        'operator',
        'numbers',
        'abc',
        'sys',
        'contextvars',
        '_io',
        '_contextvars',
        '_pydecimal',
        '_pydatetime',
    )
    if level == 0 and name in allowlist:
        return original_import(name, globals, locals, fromlist, level)
    raise ImportError(f'import {name} is not allowed')

__builtins__.__import__ = limited_import
del limited_import
"#
            .to_string();
            for symbol in self.removed_symbols {
                script.push_str(&format!("del {}\n", symbol));
            }
            interpreter.run(&script)?;
        }
        Ok(Runtime {
            interpreter,
            functions: HashMap::new(),
            aggregates: HashMap::new(),
        })
    }
}

impl Runtime {
    /// Create a new Python UDF runtime.
    pub fn new() -> Result<Self> {
        Builder::default().build()
    }

    /// Return a new builder for `Runtime`.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Add a new function from Python code.
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        self.add_function_with_handler(name, return_type, mode, code, name)
    }

    /// Add a new function from Python code with custom handler name
    pub fn add_function_with_handler(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
        handler: &str,
    ) -> Result<()> {
        let function = self.interpreter.with_gil(|py| {
            Ok(PyModule::from_code_bound(py, code, "", "")?
                .getattr(handler)?
                .into())
        })?;
        let function = Function {
            function,
            return_field: return_type.into_field(name).into(),
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Add a new aggregate function from Python code.
    pub fn add_aggregate(
        &mut self,
        name: &str,
        state_type: impl IntoField,
        output_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        let (create_state, accumulate, finish) = self.interpreter.with_gil(|py| {
            let module = PyModule::from_code_bound(py, code, "", "")?;
            let create_state = module.getattr("create_state")?;
            let accumulate = module.getattr("accumulate")?;
            let finish = module.getattr("finish")?;
            Ok((create_state.into(), accumulate.into(), finish.into()))
        })?;
        let aggregate = Aggregate {
            state_field: state_type.into_field(name).into(),
            output_field: output_type.into_field(name).into(),
            mode,
            create_state,
            accumulate,
            finish,
        };
        self.aggregates.insert(name.to_string(), aggregate);
        Ok(())
    }

    /// Remove a function.
    pub fn del_function(&mut self, name: &str) -> Result<()> {
        let function = self.functions.remove(name).context("function not found")?;
        _ = self.interpreter.with_gil(|_| {
            drop(function);
            Ok(())
        });
        Ok(())
    }

    /// Remove an aggregate function.
    pub fn del_aggregate(&mut self, name: &str) -> Result<()> {
        let aggregate = self.functions.remove(name).context("function not found")?;
        _ = self.interpreter.with_gil(|_| {
            drop(aggregate);
            Ok(())
        });
        Ok(())
    }

    /// Call the Python UDF.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        // convert each row to python objects and call the function
        let (output, error) = self.interpreter.with_gil(|py| {
            let mut results = Vec::with_capacity(input.num_rows());
            let mut errors = vec![];
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                if function.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    results.push(py.None());
                    continue;
                }
                row.clear();
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = pyarrow::get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new_bound(py, row.drain(..));
                match function.function.call1(py, args) {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        results.push(py.None());
                        errors.push((i, e.to_string()));
                    }
                }
            }
            let output = pyarrow::build_array(&function.return_field, py, &results)?;
            let error = build_error_array(input.num_rows(), errors);
            Ok((output, error))
        })?;
        if let Some(error) = error {
            let schema = Schema::new(vec![
                function.return_field.clone(),
                Field::new("error", DataType::Utf8, true).into(),
            ]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![output, error])?)
        } else {
            let schema = Schema::new(vec![function.return_field.clone()]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![output])?)
        }
    }

    /// Call a table function.
    pub fn call_table_function<'a>(
        &'a self,
        name: &'a str,
        input: &'a RecordBatch,
        chunk_size: usize,
    ) -> Result<RecordBatchIter<'a>> {
        assert!(chunk_size > 0);
        let function = self.functions.get(name).context("function not found")?;

        // initial state
        Ok(RecordBatchIter {
            interpreter: &self.interpreter,
            input,
            function,
            schema: Arc::new(Schema::new(vec![
                Field::new("row", DataType::Int32, true).into(),
                function.return_field.clone(),
            ])),
            chunk_size,
            row: 0,
            generator: None,
        })
    }

    /// Create a new state for an aggregate function.
    pub fn create_state(&self, name: &str) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let state = self.interpreter.with_gil(|py| {
            let state = aggregate.create_state.call0(py)?;
            let state = pyarrow::build_array(&aggregate.state_field, py, &[state])?;
            Ok(state)
        })?;
        Ok(state)
    }

    /// Call accumulate of an aggregate function.
    pub fn accumulate(
        &self,
        name: &str,
        state: &dyn Array,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        // convert each row to python objects and call the accumulate function
        let new_state = self.interpreter.with_gil(|py| {
            let state = pyarrow::get_pyobject(py, &aggregate.state_field, state, 0)?;

            let mut row = Vec::with_capacity(1 + input.num_columns());
            for i in 0..input.num_rows() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    continue;
                }
                row.clear();
                row.push(state.clone());
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = pyarrow::get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new_bound(py, row.drain(..));
                aggregate.accumulate.call1(py, args)?;
            }
            let output = pyarrow::build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })?;
        Ok(new_state)
    }

    /// Get the result of an aggregate function.
    pub fn finish(&self, name: &str, state: &dyn Array) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let output = self.interpreter.with_gil(|py| {
            let state = pyarrow::get_pyobject(py, &aggregate.state_field, state, 0)?;
            let result = aggregate
                .finish
                .call1(py, PyTuple::new_bound(py, [state]))?;
            let output = pyarrow::build_array(&aggregate.output_field, py, &[result])?;
            Ok(output)
        })?;
        Ok(output)
    }
}

/// An iterator over the result of a table function.
pub struct RecordBatchIter<'a> {
    interpreter: &'a SubInterpreter,
    input: &'a RecordBatch,
    function: &'a Function,
    schema: SchemaRef,
    chunk_size: usize,
    // mutable states
    /// Current row index.
    row: usize,
    /// Generator of the current row.
    generator: Option<Py<PyIterator>>,
}

impl RecordBatchIter<'_> {
    /// Get the schema of the output.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.row == self.input.num_rows() {
            return Ok(None);
        }
        let batch = self.interpreter.with_gil(|py| {
            let mut indexes = Int32Builder::with_capacity(self.chunk_size);
            let mut results = Vec::with_capacity(self.input.num_rows());
            let mut errors = vec![];
            let mut row = Vec::with_capacity(self.input.num_columns());
            while self.row < self.input.num_rows() && results.len() < self.chunk_size {
                let generator = if let Some(g) = self.generator.as_ref() {
                    g
                } else {
                    // call the table function to get a generator
                    if self.function.mode == CallMode::ReturnNullOnNullInput
                        && (self.input.columns().iter()).any(|column| column.is_null(self.row))
                    {
                        self.row += 1;
                        continue;
                    }
                    row.clear();
                    for (column, field) in
                        (self.input.columns().iter()).zip(self.input.schema().fields())
                    {
                        let val = pyarrow::get_pyobject(py, field, column, self.row)?;
                        row.push(val);
                    }
                    let args = PyTuple::new_bound(py, row.drain(..));
                    match self.function.function.bind(py).call1(args) {
                        Ok(result) => {
                            let iter = result.iter()?.into();
                            self.generator.insert(iter)
                        }
                        Err(e) => {
                            // append a row with null value and error message
                            indexes.append_value(self.row as i32);
                            results.push(py.None());
                            errors.push((indexes.len(), e.to_string()));
                            self.row += 1;
                            continue;
                        }
                    }
                };
                match generator.bind(py).clone().next() {
                    Some(Ok(value)) => {
                        indexes.append_value(self.row as i32);
                        results.push(value.into());
                    }
                    Some(Err(e)) => {
                        indexes.append_value(self.row as i32);
                        results.push(py.None());
                        errors.push((indexes.len(), e.to_string()));
                        self.row += 1;
                        self.generator = None;
                    }
                    None => {
                        self.row += 1;
                        self.generator = None;
                    }
                }
            }

            if results.is_empty() {
                return Ok(None);
            }
            let indexes = Arc::new(indexes.finish());
            let output = pyarrow::build_array(&self.function.return_field, py, &results)
                .context("failed to build arrow array from return values")?;
            let error = build_error_array(indexes.len(), errors);
            if let Some(error) = error {
                Ok(Some(
                    RecordBatch::try_new(
                        Arc::new(append_error_to_schema(&self.schema)),
                        vec![indexes, output, error],
                    )
                    .unwrap(),
                ))
            } else {
                Ok(Some(
                    RecordBatch::try_new(self.schema.clone(), vec![indexes, output]).unwrap(),
                ))
            }
        })?;
        Ok(batch)
    }
}

impl Iterator for RecordBatchIter<'_> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

impl Drop for RecordBatchIter<'_> {
    fn drop(&mut self) {
        if let Some(generator) = self.generator.take() {
            _ = self.interpreter.with_gil(|_| {
                drop(generator);
                Ok(())
            });
        }
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

impl Drop for Runtime {
    fn drop(&mut self) {
        // `PyObject` must be dropped inside the interpreter
        _ = self.interpreter.with_gil(|_| {
            self.functions.clear();
            Ok(())
        });
    }
}

fn build_error_array(num_rows: usize, errors: Vec<(usize, String)>) -> Option<ArrayRef> {
    if errors.is_empty() {
        return None;
    }
    let data_capacity = errors.iter().map(|(i, _)| i).sum();
    let mut builder = StringBuilder::with_capacity(num_rows, data_capacity);
    for (i, msg) in errors {
        while builder.len() + 1 < i {
            builder.append_null();
        }
        builder.append_value(&msg);
    }
    while builder.len() < num_rows {
        builder.append_null();
    }
    Some(Arc::new(builder.finish()))
}

/// Append an error field to the schema.
fn append_error_to_schema(schema: &Schema) -> Schema {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new("error", DataType::Utf8, true)));
    Schema::new(fields)
}
