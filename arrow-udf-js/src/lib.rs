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

use std::sync::Arc;

use anyhow::{Context as _, Result};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rquickjs::{function::Args, Context, Function, Persistent};

mod jsarrow;

/// The JS UDF runtime.
pub struct Runtime {
    // NOTE: the function must be put before the runtime and context to be dropped first.
    function: Persistent<Function<'static>>,
    _runtime: rquickjs::Runtime,
    context: Context,
    function_name: String,
    return_type: DataType,
}

impl Runtime {
    /// Create a new JS UDF runtime from a JS code.
    pub fn new(function_name: &str, return_type: DataType, code: &str) -> Result<Self> {
        let runtime = rquickjs::Runtime::new().context("failed to create quickjs runtime")?;
        let context =
            rquickjs::Context::full(&runtime).context("failed to create quickjs context")?;
        let function = context.with(|ctx| {
            let module = ctx
                .clone()
                .compile("main", code)
                .context("failed to compile module")?;
            let function: Function = module
                .get(function_name)
                .context("failed to get function")?;
            Ok(Persistent::save(&ctx, function)) as Result<_>
        })?;
        Ok(Self {
            _runtime: runtime,
            context,
            function,
            function_name: function_name.into(),
            return_type,
        })
    }

    /// Call the JS UDF.
    pub fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // convert each row to python objects and call the function
        self.context.with(|ctx| {
            let function = self.function.clone().restore(&ctx)?;
            let mut results = Vec::with_capacity(input.num_rows());
            let mut row = Vec::with_capacity(input.num_columns());
            for i in 0..input.num_rows() {
                for column in input.columns() {
                    let val = jsarrow::get_jsvalue(&ctx, column, i)?;
                    row.push(val);
                }
                let mut args = Args::new(ctx.clone(), row.len());
                args.push_args(row.drain(..))?;
                let result = function.call_arg(args)?;
                results.push(result);
            }
            let array = jsarrow::build_array(&self.return_type, &ctx, results)?;
            let schema = Schema::new(vec![Field::new(
                &self.function_name,
                array.data_type().clone(),
                true,
            )]);
            Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
        })
    }
}
