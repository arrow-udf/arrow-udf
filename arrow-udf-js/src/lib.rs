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

use std::{collections::HashMap, sync::Arc};

use anyhow::{Context as _, Result};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rquickjs::{function::Args, Context, Persistent};

mod jsarrow;

/// The JS UDF runtime.
pub struct Runtime {
    // NOTE: the function must be put before the runtime and context to be dropped first.
    functions: HashMap<String, Function>,
    _runtime: rquickjs::Runtime,
    context: Context,
}

/// A registered function.
struct Function {
    function: Persistent<rquickjs::Function<'static>>,
    return_type: DataType,
}

impl Runtime {
    /// Create a new JS UDF runtime from a JS code.
    pub fn new() -> Result<Self> {
        let runtime = rquickjs::Runtime::new().context("failed to create quickjs runtime")?;
        let context =
            rquickjs::Context::full(&runtime).context("failed to create quickjs context")?;
        Ok(Self {
            functions: HashMap::new(),
            _runtime: runtime,
            context,
        })
    }

    /// Add a JS function.
    pub fn add_function(&mut self, name: &str, return_type: DataType, code: &str) -> Result<()> {
        let function = self.context.with(|ctx| {
            let module = ctx
                .clone()
                .compile("main", code)
                .context("failed to compile module")?;
            let function: rquickjs::Function =
                module.get(name).context("failed to get function")?;
            Ok(Persistent::save(&ctx, function)) as Result<_>
        })?;
        let function = Function {
            function,
            return_type,
        };
        self.functions.insert(name.to_string(), function);
        Ok(())
    }

    /// Call the JS UDF.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        // convert each row to python objects and call the function
        self.context.with(|ctx| {
            let function = self.functions.get(name).context("function not found")?;
            let js_function = function.function.clone().restore(&ctx)?;
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                for column in input.columns() {
                    let val = jsarrow::get_jsvalue(&ctx, column, i)?;
                    row.push(val);
                }
                let mut args = Args::new(ctx.clone(), row.len());
                args.push_args(row.drain(..))?;
                let result = js_function.call_arg(args)?;
                results.push(result);
            }
            let array = jsarrow::build_array(&function.return_type, &ctx, results)?;
            let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
        })
    }
}
