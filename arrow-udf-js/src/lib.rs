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
use std::sync::Arc;

use anyhow::{anyhow, Context as _, Result};
use arrow_array::{builder::Int32Builder, Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rquickjs::{
    context::intrinsic::{BaseObjects, BigDecimal, Eval, Json, TypedArrays},
    function::Args,
    Context, Ctx, Object, Persistent, Value,
};

mod jsarrow;

/// The JS UDF runtime.
pub struct Runtime {
    functions: HashMap<String, Function>,
    /// The `BigDecimal` constructor.
    bigdecimal: Persistent<rquickjs::Function<'static>>,
    // NOTE: `functions` and `bigdecimal` must be put before the runtime and context to be dropped first.
    _runtime: rquickjs::Runtime,
    context: Context,
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
    function: Persistent<rquickjs::Function<'static>>,
    return_type: DataType,
    mode: CallMode,
}

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
        // `Eval` is required to compile JS code.
        let context =
            rquickjs::Context::custom::<(BaseObjects, Eval, Json, BigDecimal, TypedArrays)>(
                &runtime,
            )
            .context("failed to create quickjs context")?;
        let bigdecimal = context.with(|ctx| {
            let bigdecimal: rquickjs::Function = ctx.eval("BigDecimal")?;
            Ok(Persistent::save(&ctx, bigdecimal)) as Result<_>
        })?;
        Ok(Self {
            functions: HashMap::new(),
            bigdecimal,
            _runtime: runtime,
            context,
        })
    }

    /// Add a JS function.
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: DataType,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        self.add_function_with_handler(name, return_type, mode, code, name)
    }

    /// Add a JS function with custom handler name
    pub fn add_function_with_handler(
        &mut self,
        name: &str,
        return_type: DataType,
        mode: CallMode,
        code: &str,
        handler: &str,
    ) -> Result<()> {
        let function = self.context.with(|ctx| {
            let module = ctx
                .clone()
                .compile("main", code)
                .map_err(|e| check_exception(e, &ctx))
                .context("failed to compile module")?;
            let function: rquickjs::Function = module
                .get(handler)
                .context("failed to get function. HINT: make sure the function is exported")?;
            Ok(Persistent::save(&ctx, function)) as Result<_>
        })?;
        let function = Function {
            function,
            return_type,
            mode,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Call the JS UDF.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        // convert each row to python objects and call the function
        self.context.with(|ctx| {
            let bigdecimal = self.bigdecimal.clone().restore(&ctx)?;
            let js_function = function.function.clone().restore(&ctx)?;
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                row.clear();
                for column in input.columns() {
                    let val = jsarrow::get_jsvalue(&ctx, &bigdecimal, column, i)
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
                let result = js_function
                    .call_arg(args)
                    .map_err(|e| check_exception(e, &ctx))
                    .context("failed to call function")?;
                results.push(result);
            }
            let array = jsarrow::build_array(&function.return_type, &ctx, results)
                .context("failed to build arrow array from return values")?;
            let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
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
            context: &self.context,
            bigdecimal: &self.bigdecimal,
            input,
            function,
            schema: Arc::new(Schema::new(vec![
                Field::new("row", DataType::Int32, true),
                Field::new(name, function.return_type.clone(), true),
            ])),
            chunk_size,
            row: 0,
            generator: None,
        })
    }
}

/// An iterator over the result of a table function.
pub struct RecordBatchIter<'a> {
    context: &'a Context,
    bigdecimal: &'a Persistent<rquickjs::Function<'static>>,
    input: &'a RecordBatch,
    function: &'a Function,
    schema: SchemaRef,
    chunk_size: usize,
    // mutable states
    /// Current row index.
    row: usize,
    /// Generator of the current row.
    generator: Option<Persistent<Object<'static>>>,
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
        self.context.with(|ctx| {
            let bigdecimal = self.bigdecimal.clone().restore(&ctx)?;
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
                    for column in self.input.columns() {
                        let val = jsarrow::get_jsvalue(&ctx, &bigdecimal, column, self.row)
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
                    let gen = js_function
                        .call_arg::<Object>(args)
                        .map_err(|e| check_exception(e, &ctx))
                        .context("failed to call function")?;
                    let next: rquickjs::Function =
                        gen.get("next").context("failed to get 'next' method")?;
                    let mut args = Args::new(ctx.clone(), 0);
                    args.this(gen.clone())?;
                    generator.insert((gen, next))
                };
                let mut args = Args::new(ctx.clone(), 0);
                args.this(gen.clone())?;
                let object: Object = next
                    .call_arg(args)
                    .map_err(|e| check_exception(e, &ctx))
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
            let array = jsarrow::build_array(&self.function.return_type, &ctx, results)
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
