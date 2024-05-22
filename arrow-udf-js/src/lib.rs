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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::bail;
use anyhow::{anyhow, Context as _, Result};
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_array::{builder::Int32Builder, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use rquickjs::module::Evaluated;
use rquickjs::FromJs;
use rquickjs::{
    context::intrinsic::All, function::Args, Context, Ctx, Module, Object, Persistent, Value,
};

use self::into_field::IntoField;

mod into_field;
mod jsarrow;

/// The JS UDF runtime.
pub struct Runtime {
    functions: HashMap<String, Function>,
    aggregates: HashMap<String, Aggregate>,
    // NOTE: `functions` must be put before the runtime and context to be dropped first.
    pub converter: jsarrow::Converter,
    runtime: rquickjs::Runtime,
    context: Context,
    /// Timeout of each function call.
    timeout: Option<Duration>,
    /// Deadline of the current function call.
    deadline: Arc<atomic_time::AtomicOptionInstant>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("functions", &self.functions.keys())
            .finish()
    }
}

/// A registered function.
struct Function {
    function: JsFunction,
    return_field: FieldRef,
    mode: CallMode,
}

/// A user defined aggregate function.
struct Aggregate {
    state_field: FieldRef,
    output_field: FieldRef,
    mode: CallMode,
    create_state: JsFunction,
    accumulate: JsFunction,
    retract: Option<JsFunction>,
    finish: Option<JsFunction>,
    merge: Option<JsFunction>,
}

/// A persistent function.
type JsFunction = Persistent<rquickjs::Function<'static>>;

// SAFETY: `rquickjs::Runtime` is `Send` and `Sync`
unsafe impl Send for Runtime {}
unsafe impl Sync for Runtime {}

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

impl Runtime {
    /// Create a new JS UDF runtime from a JS code.
    pub fn new() -> Result<Self> {
        let runtime = rquickjs::Runtime::new().context("failed to create quickjs runtime")?;
        let context = rquickjs::Context::custom::<All>(&runtime)
            .context("failed to create quickjs context")?;

        Ok(Self {
            functions: HashMap::new(),
            aggregates: HashMap::new(),
            runtime,
            context,
            timeout: None,
            deadline: Default::default(),
            converter: jsarrow::Converter::new(),
        })
    }

    /// Set the memory limit of the runtime.
    pub fn set_memory_limit(&self, limit: Option<usize>) {
        self.runtime.set_memory_limit(limit.unwrap_or(0));
    }

    /// Set the timeout of each function call.
    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
        if timeout.is_some() {
            let deadline = self.deadline.clone();
            self.runtime.set_interrupt_handler(Some(Box::new(move || {
                if let Some(deadline) = deadline.load(Ordering::Relaxed) {
                    return deadline <= Instant::now();
                }
                false
            })));
        } else {
            self.runtime.set_interrupt_handler(None);
        }
    }

    /// Add a JS function.
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        self.add_function_with_handler(name, return_type, mode, code, name)
    }

    /// Add a JS function with custom handler name
    pub fn add_function_with_handler(
        &mut self,
        name: &str,
        return_type: impl IntoField,
        mode: CallMode,
        code: &str,
        handler: &str,
    ) -> Result<()> {
        let function = self.context.with(|ctx| {
            let (module, _) = Module::declare(ctx.clone(), name, code)
                .map_err(|e| check_exception(e, &ctx))
                .context("failed to declare module")?
                .eval()
                .map_err(|e| check_exception(e, &ctx))
                .context("failed to evaluate module")?;
            Self::get_function(&ctx, &module, handler)
        })?;
        let function = Function {
            function,
            return_field: return_type.into_field(name).into(),
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Get a function from a module.
    fn get_function<'a>(
        ctx: &Ctx<'a>,
        module: &Module<'a, Evaluated>,
        name: &str,
    ) -> Result<JsFunction> {
        let function: rquickjs::Function = module
            .get(name)
            .context("failed to get function. HINT: make sure the function is exported")?;
        Ok(Persistent::save(ctx, function))
    }

    /// Add a new aggregate function from JS code.
    ///
    /// # Arguments
    ///
    /// - `name`: The name of the function.
    /// - `state_type`: The data type of the internal state.
    /// - `output_type`: The data type of the aggregate value.
    /// - `mode`: Whether the function will be called when some of its arguments are null.
    /// - `code`: The JS code of the aggregate function.
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
    /// # use arrow_udf_js::{Runtime, CallMode};
    /// # use arrow_schema::DataType;
    /// let mut runtime = Runtime::new().unwrap();
    /// runtime
    ///     .add_aggregate(
    ///         "sum",
    ///         DataType::Int32, // state_type
    ///         DataType::Int32, // output_type
    ///         CallMode::ReturnNullOnNullInput,
    ///         r#"
    ///         export function create_state() {
    ///             return 0;
    ///         }
    ///         export function accumulate(state, value) {
    ///             return state + value;
    ///         }
    ///         export function retract(state, value) {
    ///             return state - value;
    ///         }
    ///         export function merge(state1, state2) {
    ///             return state1 + state2;
    ///         }
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
        let aggregate = self.context.with(|ctx| {
            let (module, _) = Module::declare(ctx.clone(), name, code)
                .map_err(|e| check_exception(e, &ctx))
                .context("failed to declare module")?
                .eval()
                .map_err(|e| check_exception(e, &ctx))
                .context("failed to evaluate module")?;
            Ok(Aggregate {
                state_field: state_type.into_field(name).into(),
                output_field: output_type.into_field(name).into(),
                mode,
                create_state: Self::get_function(&ctx, &module, "create_state")?,
                accumulate: Self::get_function(&ctx, &module, "accumulate")?,
                retract: Self::get_function(&ctx, &module, "retract").ok(),
                finish: Self::get_function(&ctx, &module, "finish").ok(),
                merge: Self::get_function(&ctx, &module, "merge").ok(),
            }) as Result<Aggregate>
        })?;
        if aggregate.finish.is_none() && aggregate.state_field != aggregate.output_field {
            bail!("`output_type` must be the same as `state_type` when `finish` is not defined");
        }
        self.aggregates.insert(name.to_string(), aggregate);
        Ok(())
    }

    /// Call the JS UDF.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        // convert each row to python objects and call the function
        self.context.with(|ctx| {
            let js_function = function.function.clone().restore(&ctx)?;
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                row.clear();
                for (column, field) in input.columns().iter().zip(input.schema().fields()) {
                    let val = self
                        .converter
                        .get_jsvalue(&ctx, field, column, i)
                        .context("failed to get jsvalue from arrow array")?;

                    row.push(val);
                }
                if function.mode == CallMode::ReturnNullOnNullInput
                    && row.iter().any(|v| v.is_null())
                {
                    results.push(Value::new_null(ctx.clone()));
                    continue;
                }
                let mut args = Args::new(ctx.clone(), row.len());
                args.push_args(row.drain(..))?;
                let result = self
                    .call_user_fn(&ctx, &js_function, args)
                    .context("failed to call function")?;
                results.push(result);
            }

            let array = self
                .converter
                .build_array(&function.return_field, &ctx, results)
                .context("failed to build arrow array from return values")?;
            let schema = Schema::new(vec![function.return_field.clone()]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
        })
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
            rt: self,
            input,
            function,
            schema: Arc::new(Schema::new(vec![
                Arc::new(Field::new("row", DataType::Int32, false)),
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
        let state = self.context.with(|ctx| {
            let create_state = aggregate.create_state.clone().restore(&ctx)?;
            let state = self
                .call_user_fn(&ctx, &create_state, Args::new(ctx.clone(), 0))
                .context("failed to call create_state")?;
            let state = self
                .converter
                .build_array(&aggregate.state_field, &ctx, vec![state])?;
            Ok(state) as Result<_>
        })?;
        Ok(state)
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
        let aggregate = self.aggregates.get(name).context("function not found")?;
        // convert each row to python objects and call the accumulate function
        let new_state = self.context.with(|ctx| {
            let accumulate = aggregate.accumulate.clone().restore(&ctx)?;
            let mut state = self
                .converter
                .get_jsvalue(&ctx, &aggregate.state_field, state, 0)?;

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
                    let pyobj = self.converter.get_jsvalue(&ctx, field, column, i)?;
                    row.push(pyobj);
                }
                let mut args = Args::new(ctx.clone(), row.len());
                args.push_args(row.drain(..))?;
                state = self
                    .call_user_fn(&ctx, &accumulate, args)
                    .context("failed to call accumulate")?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, &ctx, vec![state])?;
            Ok(output) as Result<_>
        })?;
        Ok(new_state)
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
        // convert each row to python objects and call the accumulate function
        let new_state = self.context.with(|ctx| {
            let accumulate = aggregate.accumulate.clone().restore(&ctx)?;
            let retract = aggregate
                .retract
                .clone()
                .context("function does not support retraction")?
                .restore(&ctx)?;

            let mut state = self
                .converter
                .get_jsvalue(&ctx, &aggregate.state_field, state, 0)?;

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
                    let pyobj = self.converter.get_jsvalue(&ctx, field, column, i)?;
                    row.push(pyobj);
                }
                let func = if ops.is_valid(i) && ops.value(i) {
                    &retract
                } else {
                    &accumulate
                };
                let mut args = Args::new(ctx.clone(), row.len());
                args.push_args(row.drain(..))?;
                state = self
                    .call_user_fn(&ctx, func, args)
                    .context("failed to call accumulate or retract")?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, &ctx, vec![state])?;
            Ok(output) as Result<_>
        })?;
        Ok(new_state)
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
        let output = self.context.with(|ctx| {
            let merge = aggregate
                .merge
                .clone()
                .context("merge not found")?
                .restore(&ctx)?;
            let mut state = self
                .converter
                .get_jsvalue(&ctx, &aggregate.state_field, states, 0)?;
            for i in 1..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    continue;
                }
                let state2 = self
                    .converter
                    .get_jsvalue(&ctx, &aggregate.state_field, states, i)?;
                let mut args = Args::new(ctx.clone(), 2);
                args.push_args([state, state2])?;
                state = self
                    .call_user_fn(&ctx, &merge, args)
                    .context("failed to call accumulate or retract")?;
            }
            let output = self
                .converter
                .build_array(&aggregate.state_field, &ctx, vec![state])?;
            Ok(output) as Result<_>
        })?;
        Ok(output)
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
    pub fn finish(&self, name: &str, states: &ArrayRef) -> Result<ArrayRef> {
        let aggregate = self.aggregates.get(name).context("function not found")?;
        let Some(finish) = &aggregate.finish else {
            return Ok(states.clone());
        };
        let output = self.context.with(|ctx| {
            let finish = finish.clone().restore(&ctx)?;
            let mut results = Vec::with_capacity(states.len());
            for i in 0..states.len() {
                if aggregate.mode == CallMode::ReturnNullOnNullInput && states.is_null(i) {
                    results.push(Value::new_null(ctx.clone()));
                    continue;
                }
                let state = self
                    .converter
                    .get_jsvalue(&ctx, &aggregate.state_field, states, i)?;
                let mut args = Args::new(ctx.clone(), 1);
                args.push_args([state])?;
                let result = self
                    .call_user_fn(&ctx, &finish, args)
                    .context("failed to call finish")?;
                results.push(result);
            }
            let output = self
                .converter
                .build_array(&aggregate.output_field, &ctx, results)?;
            Ok(output) as Result<_>
        })?;
        Ok(output)
    }

    /// Call a user function.
    ///
    /// If `timeout` is set, the function will be interrupted after the timeout.
    fn call_user_fn<'js, T: FromJs<'js>>(
        &self,
        ctx: &Ctx<'js>,
        f: &rquickjs::Function<'js>,
        args: Args<'js>,
    ) -> Result<T> {
        let result = if let Some(timeout) = self.timeout {
            self.deadline
                .store(Some(Instant::now() + timeout), Ordering::Relaxed);
            let result = f.call_arg(args);
            self.deadline.store(None, Ordering::Relaxed);
            result
        } else {
            f.call_arg(args)
        };
        result.map_err(|e| check_exception(e, ctx))
    }
}

/// An iterator over the result of a table function.
pub struct RecordBatchIter<'a> {
    rt: &'a Runtime,
    input: &'a RecordBatch,
    function: &'a Function,
    schema: SchemaRef,
    chunk_size: usize,
    // mutable states
    /// Current row index.
    row: usize,
    /// Generator of the current row.
    generator: Option<Persistent<Object<'static>>>,
    converter: &'a jsarrow::Converter,
}

// XXX: not sure if this is safe.
unsafe impl Send for RecordBatchIter<'_> {}

impl RecordBatchIter<'_> {
    /// Get the schema of the output.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.row == self.input.num_rows() {
            return Ok(None);
        }
        self.rt.context.with(|ctx| {
            let js_function = self.function.function.clone().restore(&ctx)?;
            let mut indexes = Int32Builder::with_capacity(self.chunk_size);
            let mut results = Vec::with_capacity(self.input.num_rows());
            let mut row = Vec::with_capacity(self.input.num_columns());
            // restore generator from state
            let mut generator = match self.generator.take() {
                Some(generator) => {
                    let gen = generator.restore(&ctx)?;
                    let next: rquickjs::Function =
                        gen.get("next").context("failed to get 'next' method")?;
                    Some((gen, next))
                }
                None => None,
            };
            while self.row < self.input.num_rows() && results.len() < self.chunk_size {
                let (gen, next) = if let Some(g) = generator.as_ref() {
                    g
                } else {
                    // call the table function to get a generator
                    row.clear();
                    for (column, field) in
                        (self.input.columns().iter()).zip(self.input.schema().fields())
                    {
                        let val = self
                            .converter
                            .get_jsvalue(&ctx, field, column, self.row)
                            .context("failed to get jsvalue from arrow array")?;
                        row.push(val);
                    }
                    if self.function.mode == CallMode::ReturnNullOnNullInput
                        && row.iter().any(|v| v.is_null())
                    {
                        self.row += 1;
                        continue;
                    }
                    let mut args = Args::new(ctx.clone(), row.len());
                    args.push_args(row.drain(..))?;
                    let gen: Object = self
                        .rt
                        .call_user_fn(&ctx, &js_function, args)
                        .context("failed to call function")?;
                    let next: rquickjs::Function =
                        gen.get("next").context("failed to get 'next' method")?;
                    let mut args = Args::new(ctx.clone(), 0);
                    args.this(gen.clone())?;
                    generator.insert((gen, next))
                };
                let mut args = Args::new(ctx.clone(), 0);
                args.this(gen.clone())?;
                let object: Object = self
                    .rt
                    .call_user_fn(&ctx, next, args)
                    .context("failed to call next")?;
                let value: Value = object.get("value")?;
                let done: bool = object.get("done")?;
                if done {
                    self.row += 1;
                    generator = None;
                    continue;
                }
                indexes.append_value(self.row as i32);
                results.push(value);
            }
            self.generator = generator.map(|(gen, _)| Persistent::save(&ctx, gen));

            if results.is_empty() {
                return Ok(None);
            }
            let indexes = Arc::new(indexes.finish());
            let array = self
                .converter
                .build_array(&self.function.return_field, &ctx, results)
                .context("failed to build arrow array from return values")?;
            Ok(Some(RecordBatch::try_new(
                self.schema.clone(),
                vec![indexes, array],
            )?))
        })
    }
}

impl Iterator for RecordBatchIter<'_> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next().transpose()
    }
}

/// Get exception from `ctx` if the error is an exception.
fn check_exception(err: rquickjs::Error, ctx: &Ctx) -> anyhow::Error {
    match err {
        rquickjs::Error::Exception => {
            anyhow!("exception generated by QuickJS: {:?}", ctx.catch())
        }
        e => e.into(),
    }
}
