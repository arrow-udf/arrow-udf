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

pub use self::into_field::IntoField;
use anyhow::{anyhow, bail, Context, Error, Result};
use arrow_array::builder::{ArrayBuilder, Int32Builder, StringBuilder};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use pyo3::types::{PyAnyMethods, PyIterator, PyModule, PyTuple};
use pyo3::{Py, PyObject, Python};
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt::Debug;
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{mem, thread};

mod into_field;
mod pyarrow;

/// The task we send to the Python runtime (a thread-isolated Python interpreter).
type Task = Box<dyn FnOnce(Python) + Send + 'static>;

/// A runtime to execute user defined functions in Python.
///
/// # Usages
///
/// - Create a new runtime with [`Runtime::new`] or [`Runtime::builder`].
/// - For scalar functions, use [`add_function`] and [`call`].
/// - For table functions, use [`add_function`] and [`call_table_function`].
/// - For aggregate functions, create the function with [`add_aggregate`], and then
///     - create a new state with [`create_state`],
///     - update the state with [`accumulate`] or [`accumulate_or_retract`],
///     - merge states with [`merge`],
///     - finally get the result with [`finish`].
///
/// Click on each function to see the example.
///
/// [`add_function`]: Runtime::add_function
/// [`add_aggregate`]: Runtime::add_aggregate
/// [`call`]: Runtime::call
/// [`call_table_function`]: Runtime::call_table_function
/// [`create_state`]: Runtime::create_state
/// [`accumulate`]: Runtime::accumulate
/// [`accumulate_or_retract`]: Runtime::accumulate_or_retract
/// [`merge`]: Runtime::merge
/// [`finish`]: Runtime::finish
pub struct Runtime {
    functions: HashMap<String, Function>,
    aggregates: HashMap<String, Aggregate>,
    converter: pyarrow::Converter,
    sender: Option<Sender<Task>>,
    handle: Option<JoinHandle<()>>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("functions", &self.functions.keys())
            .field("aggregates", &self.aggregates.keys())
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
    retract: Option<PyObject>,
    finish: Option<PyObject>,
    merge: Option<PyObject>,
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
        let (sender, receiver) = mpsc::channel::<Task>();

        let handle = thread::spawn(move || {
            pyo3::prepare_freethreaded_python();

            for task in receiver {
                Python::with_gil(task)
            }
        });

        let runtime = Runtime {
            functions: HashMap::new(),
            aggregates: HashMap::new(),
            converter: pyarrow::Converter::new(),
            sender: Some(sender),
            handle: Some(handle),
        };

        runtime.run(
            r#"
# internal use for json types
import json
import pickle
import decimal

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
            runtime.run(&script)?;
        }
        Ok(runtime)
    }
}

impl Runtime {
    /// Create a new `Runtime`.
    pub fn new() -> Result<Self> {
        Builder::default().build()
    }

    /// Return a new builder for `Runtime`.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Send a task in the Python runtime.
    fn task_send<F>(&self, task: F) -> Result<()>
    where
        F: FnOnce(Python) + Send + 'static,
    {
        self.sender
            .as_ref()
            .ok_or_else(|| anyhow!("runtime has been shutdown"))?
            .send(Box::new(task))
            .map_err(|err| anyhow!("failed to send task to python runtime: {:?}", err))
    }

    /// Execute a task in the Python runtime.
    fn task_execute<F, C, R>(&self, ctx: &C, task: F) -> Result<R>
    where
        F: FnOnce(Python, &C) -> Result<R> + Send + 'static,
        R: Send + 'static,
        C: Send,
    {
        let (sender, receiver) = mpsc::channel();

        // Capture ctx as a ptr.
        let ctx_ptr = UnsafeContext::new(ctx);

        self.sender
            .as_ref()
            .ok_or_else(|| anyhow!("runtime has been shutdown"))?
            .send(Box::new(move |py| {
                // Safety
                //
                // Borrow the context into our python runtime.
                // This operation is safe since we make sure that the context is not dropped
                // before this runtime.
                let ctx_ref: &C = ctx_ptr.get();

                let result = task(py, ctx_ref);
                sender
                    .send(result)
                    .expect("failed to send result back to the caller, this should never happen");
            }))
            .map_err(|err| anyhow!("failed to send task to python runtime: {:?}", err))?;

        receiver
            .recv()
            .map_err(|err| anyhow!("failed to receive result from python runtime: {:?}", err))?
    }

    /// Run Python code in the interpreter.
    fn run(&self, code: &str) -> Result<()> {
        self.task_execute(&code, |py, code| {
            py.run(&CString::new(*code)?, None, None)
                .map_err(|e| e.into())
        })
    }

    /// Add a new scalar function or table function.
    ///
    /// # Arguments
    ///
    /// - `name`: The name of the function.
    /// - `return_type`: The data type of the return value.
    /// - `mode`: Whether the function will be called when some of its arguments are null.
    /// - `code`: The Python code of the function.
    ///
    /// The code should define a function with the same name as the function.
    /// The function should return a value for scalar functions, or yield values for table functions.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_udf_python::{Runtime, CallMode};
    /// # use arrow_schema::DataType;
    /// let mut runtime = Runtime::new().unwrap();
    /// // add a scalar function
    /// runtime
    ///     .add_function(
    ///         "gcd",
    ///         DataType::Int32,
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def gcd(a: int, b: int) -> int:
    ///     while b:
    ///         a, b = b, a % b
    ///     return a
    /// "#,
    ///     )
    ///     .unwrap();
    /// // add a table function
    /// runtime
    ///     .add_function(
    ///         "series",
    ///         DataType::Int32,
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def series(n: int):
    ///     for i in range(n):
    ///         yield i
    /// "#,
    ///     )
    ///     .unwrap();
    /// ```
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        self.add_function_with_handler(name, return_type, mode, code, name)
    }

    /// Add a new scalar function or table function with custom handler name.
    ///
    /// # Arguments
    ///
    /// - `handler`: The name of function in Python code to be called.
    /// - others: Same as [`add_function`].
    ///
    /// [`add_function`]: Runtime::add_function
    pub fn add_function_with_handler(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
        handler: &str,
    ) -> Result<()> {
        let return_type = return_type.into_field(name);
        let ctx = &(name, code, handler);

        let function = self.task_execute(ctx, |py, (name, code, handler)| {
            Ok::<_, Error>(
                PyModule::from_code(
                    py,
                    &CString::new(*code)?,
                    &CString::new(*name)?,
                    &CString::new(*name)?,
                )?
                .getattr(handler)?
                .into(),
            )
        })?;
        let function = Function {
            function,
            return_field: return_type.into(),
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Add a new aggregate function from Python code.
    ///
    /// # Arguments
    ///
    /// - `name`: The name of the function.
    /// - `state_type`: The data type of the internal state.
    /// - `output_type`: The data type of the aggregate value.
    /// - `mode`: Whether the function will be called when some of its arguments are null.
    /// - `code`: The Python code of the aggregate function.
    ///
    /// The code should define at least two functions:
    ///
    /// - `create_state() -> state`: Create a new state object.
    /// - `accumulate(state, *args) -> state`: Accumulate a new value into the state, returning the updated state.
    ///
    /// optionally, the code can define:
    ///
    /// - `finish(state) -> value`: Get the result of the aggregate function.
    ///     If not defined, the state is returned as the result.
    ///     In this case, `output_type` must be the same as `state_type`.
    /// - `retract(state, *args) -> state`: Retract a value from the state, returning the updated state.
    /// - `merge(state, state) -> state`: Merge two states, returning the merged state.
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_udf_python::{Runtime, CallMode};
    /// # use arrow_schema::DataType;
    /// let mut runtime = Runtime::new().unwrap();
    /// runtime
    ///     .add_aggregate(
    ///         "sum",
    ///         DataType::Int32, // state_type
    ///         DataType::Int32, // output_type
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    /// def create_state():
    ///     return 0
    ///
    /// def accumulate(state, value):
    ///     return state + value
    ///
    /// def retract(state, value):
    ///     return state - value
    ///
    /// def merge(state1, state2):
    ///     return state1 + state2
    ///         "#,
    ///     )
    ///     .unwrap();
    /// ```
    pub fn add_aggregate(
        &mut self,
        name: &str,
        state_type: impl IntoField,
        output_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        let state_type = state_type.into_field(name);
        let output_type = output_type.into_field(name);

        let ctx = &(state_type, output_type, mode, code, name);

        let aggregate =
            self.task_execute(ctx, |py, (state_type, output_type, mode, code, name)| {
                let module = PyModule::from_code(
                    py,
                    &CString::new(*code)?,
                    &CString::new(*name)?,
                    &CString::new(*name)?,
                )?;
                Ok::<_, Error>(Aggregate {
                    state_field: state_type.clone().into(),
                    output_field: output_type.clone().into(),
                    mode: *mode,
                    create_state: module.getattr("create_state")?.into(),
                    accumulate: module.getattr("accumulate")?.into(),
                    retract: module.getattr("retract").ok().map(|f| f.into()),
                    finish: module.getattr("finish").ok().map(|f| f.into()),
                    merge: module.getattr("merge").ok().map(|f| f.into()),
                })
            })?;
        if aggregate.finish.is_none() && aggregate.state_field != aggregate.output_field {
            bail!("`output_type` must be the same as `state_type` when `finish` is not defined");
        }
        self.aggregates.insert(name.to_string(), aggregate);
        Ok(())
    }

    /// Remove a scalar or table function.
    pub fn del_function(&mut self, name: &str) -> Result<()> {
        let function = self.functions.remove(name).context("function not found")?;
        self.task_send(|_| {
            drop(function);
        })
    }

    /// Remove an aggregate function.
    pub fn del_aggregate(&mut self, name: &str) -> Result<()> {
        let aggregate = self.functions.remove(name).context("function not found")?;
        self.task_send(|_| {
            drop(aggregate);
        })
    }

    /// Call a scalar function.
    ///
    /// # Example
    ///
    /// ```
    #[doc = include_str!("doc_create_function.txt")]
    /// // suppose we have created a scalar function `gcd`
    /// // see the example in `add_function`
    ///
    /// let schema = Schema::new(vec![
    ///     Field::new("x", DataType::Int32, true),
    ///     Field::new("y", DataType::Int32, true),
    /// ]);
    /// let arg0 = Int32Array::from(vec![Some(25), None]);
    /// let arg1 = Int32Array::from(vec![Some(15), None]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0), Arc::new(arg1)]).unwrap();
    ///
    /// let output = runtime.call("gcd", &input).unwrap();
    /// assert_eq!(&**output.column(0), &Int32Array::from(vec![Some(5), None]));
    /// ```
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;

        let ctx = &(input, function, &self.converter);

        // convert each row to python objects and call the function
        let (output, error) = self.task_execute(ctx, |py, (input, function, converter)| {
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
                    let pyobj = converter.get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new(py, row.drain(..))?;
                match function.function.call1(py, args) {
                    Ok(result) => results.push(result),
                    Err(e) => {
                        results.push(py.None());
                        errors.push((i, e.to_string()));
                    }
                }
            }
            let output = converter.build_array(&function.return_field, py, &results)?;
            let error = build_error_array(input.num_rows(), errors);
            Ok::<_, anyhow::Error>((output, error))
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
    ///
    /// # Example
    ///
    /// ```
    #[doc = include_str!("doc_create_function.txt")]
    /// // suppose we have created a table function `series`
    /// // see the example in `add_function`
    ///
    /// let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3)]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let mut outputs = runtime.call_table_function("series", &input, 10).unwrap();
    /// let output = outputs.next().unwrap().unwrap();
    /// let pretty = arrow_cast::pretty::pretty_format_batches(&[output]).unwrap().to_string();
    /// assert_eq!(pretty, r#"
    /// +-----+--------+
    /// | row | series |
    /// +-----+--------+
    /// | 0   | 0      |
    /// | 2   | 0      |
    /// | 2   | 1      |
    /// | 2   | 2      |
    /// +-----+--------+"#.trim());
    /// ```
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
            runtime: self,
            input,
            function,
            schema: Arc::new(Schema::new(vec![
                Field::new("row", DataType::Int32, true).into(),
                function.return_field.clone(),
            ])),
            chunk_size,
            row: 0,
            generator: None,
            converter: &self.converter,
        })
    }

    /// Create a new state for an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![0]));
    /// ```
    pub fn create_state(&self, name: &str) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;

        let ctx = &(aggregate, &self.converter);

        self.task_execute(ctx, |py, (aggregate, converter)| {
            let state = aggregate.create_state.call0(py)?;
            let state = converter.build_array(&aggregate.state_field, py, &[state])?;
            Ok(state)
        })
    }

    /// Call accumulate of an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    ///
    /// let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let state = runtime.accumulate("sum", &state, &input).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![9]));
    /// ```
    pub fn accumulate(
        &self,
        name: &str,
        state: &dyn Array,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        let converter = &self.converter;
        let aggregate = self.aggregates.get(name).context("function not found")?;

        let ctx = &(state, input, converter, aggregate);

        // convert each row to python objects and call the accumulate function
        self.task_execute(ctx, |py, (state, input, converter, aggregate)| {
            let mut state = converter.get_pyobject(py, &aggregate.state_field, *state, 0)?;

            let mut row = Vec::with_capacity(1 + input.num_columns());
            for i in 0..input.num_rows() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput
                    && input.columns().iter().any(|column| column.is_null(i))
                {
                    continue;
                }
                row.clear();
                row.push(state.clone_ref(py));
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let pyobj = converter.get_pyobject(py, field, column, i)?;
                    row.push(pyobj);
                }
                let args = PyTuple::new(py, row.drain(..))?;
                state = aggregate.accumulate.call1(py, args)?;
            }
            let output = converter.build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })
    }

    /// Call accumulate or retract of an aggregate function.
    ///
    /// The `ops` is a boolean array that indicates whether to accumulate or retract each row.
    /// `false` for accumulate and `true` for retract.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let state = runtime.create_state("sum").unwrap();
    ///
    /// let schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
    /// let arg0 = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    /// let ops = BooleanArray::from(vec![false, false, true, false]);
    /// let input = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arg0)]).unwrap();
    ///
    /// let state = runtime.accumulate_or_retract("sum", &state, &ops, &input).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![3]));
    /// ```
    pub fn accumulate_or_retract(
        &self,
        name: &str,
        state: &dyn Array,
        ops: &BooleanArray,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let retract = aggregate
            .retract
            .as_ref()
            .context("function does not support retraction")?;

        let ctx = &(input, aggregate, retract, &self.converter, ops, state);

        // convert each row to python objects and call the accumulate function
        self.task_execute(
            ctx,
            |py, (input, aggregate, retract, converter, ops, state)| {
                let mut state = converter.get_pyobject(py, &aggregate.state_field, *state, 0)?;

                let mut row = Vec::with_capacity(1 + input.num_columns());
                for i in 0..input.num_rows() {
                    if aggregate.mode == CallMode::ReturnNullOnNullInput
                        && input.columns().iter().any(|column| column.is_null(i))
                    {
                        continue;
                    }
                    row.clear();
                    row.push(state.clone_ref(py));
                    for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                        let pyobj = converter.get_pyobject(py, field, column, i)?;
                        row.push(pyobj);
                    }
                    let args = PyTuple::new(py, row.drain(..))?;
                    let func = if ops.is_valid(i) && ops.value(i) {
                        retract
                    } else {
                        &aggregate.accumulate
                    };
                    state = func.call1(py, args)?;
                }
                let output = converter.build_array(&aggregate.state_field, py, &[state])?;
                Ok(output)
            },
        )
    }

    /// Merge states of an aggregate function.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let states = Int32Array::from(vec![Some(1), None, Some(3), Some(5)]);
    ///
    /// let state = runtime.merge("sum", &states).unwrap();
    /// assert_eq!(&*state, &Int32Array::from(vec![9]));
    /// ```
    pub fn merge(&self, name: &str, states: &dyn Array) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let merge = aggregate.merge.as_ref().context("merge not found")?;

        let ctx = &(states, &self.converter, aggregate, merge);
        self.task_execute(ctx, |py, (states, converter, aggregate, merge)| {
            let mut state = converter.get_pyobject(py, &aggregate.state_field, *states, 0)?;
            for i in 1..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    continue;
                }
                let state2 = converter.get_pyobject(py, &aggregate.state_field, *states, i)?;
                let args = PyTuple::new(py, [state, state2])?;
                state = merge.call1(py, args)?;
            }
            let output = converter.build_array(&aggregate.state_field, py, &[state])?;
            Ok(output)
        })
    }

    /// Get the result of an aggregate function.
    ///
    /// If the `finish` function is not defined, the state is returned as the result.
    ///
    /// # Example
    /// ```
    #[doc = include_str!("doc_create_aggregate.txt")]
    /// let states: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(5)]));
    ///
    /// let outputs = runtime.finish("sum", &states).unwrap();
    /// assert_eq!(&outputs, &states);
    /// ```
    pub fn finish(&self, name: &str, input_states: &ArrayRef) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let Some(finish) = &aggregate.finish else {
            return Ok(input_states.clone());
        };

        let ctx = (input_states, &self.converter, aggregate, finish);

        self.task_execute(&ctx, |py, (states, converter, aggregate, finish)| {
            let mut results = Vec::with_capacity(states.len());
            for i in 0..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    results.push(py.None());
                    continue;
                }
                let state = converter.get_pyobject(py, &aggregate.state_field, states, i)?;
                let args = PyTuple::new(py, [state])?;
                let result = finish.call1(py, args)?;
                results.push(result);
            }
            let output = converter.build_array(&aggregate.output_field, py, &results)?;
            Ok(output)
        })
    }
}

/// An iterator over the result of a table function.
pub struct RecordBatchIter<'a> {
    runtime: &'a Runtime,
    input: &'a RecordBatch,
    function: &'a Function,
    schema: SchemaRef,
    chunk_size: usize,
    // mutable states
    /// Current row index.
    row: usize,
    /// Generator of the current row.
    generator: Option<Py<PyIterator>>,
    converter: &'a pyarrow::Converter,
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
        let schema = self.schema.clone();
        let generator = self.generator.take();
        let row = self.row;
        let ctx = (self.chunk_size, self.input, self.function, self.converter);

        let (res, generator, row) = self.runtime.task_execute(
            &ctx,
            move |py, (chunk_size, input, function, converter)| {
                let mut outer_generator = generator;
                let mut outer_row = row;

                let mut indexes = Int32Builder::with_capacity(*chunk_size);
                let mut results = Vec::with_capacity(input.num_rows());
                let mut errors = vec![];
                let mut row = Vec::with_capacity(input.num_columns());
                while outer_row < input.num_rows() && results.len() < *chunk_size {
                    let generator = if let Some(g) = outer_generator.as_ref() {
                        g
                    } else {
                        // call the table function to get a generator
                        if function.mode == CallMode::ReturnNullOnNullInput
                            && (input.columns().iter()).any(|column| column.is_null(outer_row))
                        {
                            outer_row += 1;
                            continue;
                        }
                        row.clear();
                        for (column, field) in (input.columns().iter()).zip(input.schema().fields())
                        {
                            let val = converter.get_pyobject(py, field, column, outer_row)?;
                            row.push(val);
                        }
                        let args = PyTuple::new(py, row.drain(..))?;
                        match function.function.bind(py).call1(args) {
                            Ok(result) => {
                                let iter = result.try_iter()?.into();
                                outer_generator.insert(iter)
                            }
                            Err(e) => {
                                // append a row with null value and error message
                                indexes.append_value(outer_row as i32);
                                results.push(py.None());
                                errors.push((indexes.len(), e.to_string()));
                                outer_row += 1;
                                continue;
                            }
                        }
                    };
                    match generator.bind(py).clone().next() {
                        Some(Ok(value)) => {
                            indexes.append_value(outer_row as i32);
                            results.push(value.into());
                        }
                        Some(Err(e)) => {
                            indexes.append_value(outer_row as i32);
                            results.push(py.None());
                            errors.push((indexes.len(), e.to_string()));
                            outer_row += 1;
                            outer_generator = None;
                        }
                        None => {
                            outer_row += 1;
                            outer_generator = None;
                        }
                    }
                }

                if results.is_empty() {
                    return Ok((None, outer_generator, outer_row));
                }
                let indexes = Arc::new(indexes.finish());
                let output = converter
                    .build_array(&function.return_field, py, &results)
                    .context("failed to build arrow array from return values")?;
                let error = build_error_array(indexes.len(), errors);
                if let Some(error) = error {
                    Ok((
                        Some(
                            RecordBatch::try_new(
                                Arc::new(append_error_to_schema(&schema)),
                                vec![indexes, output, error],
                            )
                            .unwrap(),
                        ),
                        outer_generator,
                        outer_row,
                    ))
                } else {
                    Ok((
                        Some(RecordBatch::try_new(schema, vec![indexes, output]).unwrap()),
                        outer_generator,
                        outer_row,
                    ))
                }
            },
        )?;

        self.generator = generator;
        self.row = row;
        Ok(res)
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
            let _ = self.runtime.task_send(|_| {
                drop(generator);
            });
        }
    }
}

/// Whether the function will be called when some of its arguments are null.
#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
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
        let functions = mem::take(&mut self.functions);
        let aggregates = mem::take(&mut self.aggregates);

        // `PyObject` must be dropped inside the interpreter
        let _ = self.task_send(move |_| {
            let _ = functions;
            let _ = aggregates;
        });

        // Drop the sender.
        let _ = self.sender.take();

        // Make sure the handle has been joined.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
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

/// Passing context to python runtime.
///
/// # Safety
///
/// This struct is used to pass context between runtime.
/// Users must make sure that context is not dropped before the runtime.
#[derive(Copy, Clone)]
struct UnsafeContext {
    ptr: *const (),
}
unsafe impl Send for UnsafeContext {}
unsafe impl Sync for UnsafeContext {}

impl UnsafeContext {
    fn new<C>(ctx: &C) -> Self {
        Self {
            ptr: ctx as *const C as *const (),
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that:
    /// 1. The original context reference is still valid
    /// 2. The context is not mutably borrowed elsewhere
    fn get<C>(&self) -> &C {
        unsafe { &*(self.ptr as *const C) }
    }
}
