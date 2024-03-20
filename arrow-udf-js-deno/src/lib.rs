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

use std::fmt::Debug;
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc, task::Poll};

use anyhow::{Context, Result};
use arrow_array::{builder::Int32Builder, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_udf_js_deno_runtime::deno_runtime;
use futures::{Future, Stream, StreamExt, TryStreamExt};

use crate::deno_arrow::get_jsvalue;

mod deno_arrow;
pub mod tokio_spawn_pinned;
mod v8;
mod values_future;

static SNAPSHOT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/ARROW_DENO_RUNTIME.snap"));

thread_local! {
    static THREAD_ISOLATE: Rc<RefCell<InternalRuntime>>  = Rc::new(RefCell::new( InternalRuntime::new() ));
    static THREAD_RUNTIME: Arc<Runtime>  = Arc::new( Runtime::new_internal() );
}

pub fn on_thread_stop() {
    THREAD_ISOLATE.with(|isolate| {
        //We need to drop the runtime to avoid a deadlock
        isolate
            .borrow_mut()
            .deno_runtime
            .borrow_mut()
            .drop_runtime()
    });
}

/// Whether the function will be called when some of its arguments are null.
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone)]
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

pub struct InternalRuntime {
    functions: HashMap<String, Function>,
    deno_runtime: Rc<RefCell<deno_runtime::DenoRuntime>>,
    big_decimal: ::v8::Global<::v8::Function>,
}

#[derive(Clone)]
struct Function {
    mode: CallMode,
    function: ::v8::Global<::v8::Function>,
    return_type: DataType,
}

pub struct RecordBatchIter {
    inner: RecordBatchIterInternal,
    local_pool: tokio_spawn_pinned::LocalPoolHandle,
}

impl RecordBatchIter {
    pub async fn next(&mut self) -> Option<Result<RecordBatch>> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                self.inner.next().await
            } else {
                let mut inner = self.inner.clone();
                self.local_pool
                    .spawn_pinned(|| async move { inner.next().await })
                    .await
                    .unwrap()
            }
        } else {
            None
        }
    }

    pub async fn for_each<Fut, F>(self, f: F)
    where
        F: FnMut(Result<RecordBatch>) -> Fut + Send + 'static,
        Fut: Future<Output = ()>,
    {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                self.inner.for_each(f).await;
            } else {
                let inner = self.inner;
                self.local_pool
                    .spawn_pinned(|| async move { inner.for_each(f).await })
                    .await
                    .unwrap();
            }
        } else {
            panic!("No tokio runtime found")
        }
    }

    pub async fn try_next(&mut self) -> Result<Option<RecordBatch>> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                self.inner.try_next().await
            } else {
                let mut inner = self.inner.clone();
                self.local_pool
                    .spawn_pinned(|| async move { inner.try_next().await })
                    .await?
            }
        } else {
            Err(anyhow::anyhow!("No tokio runtime found"))
        }
    }

    pub fn schema(&self) -> &Schema {
        self.inner.schema()
    }
}

pub(crate) type GeneratorState =
    Rc<RefCell<Option<(::v8::Global<::v8::Object>, ::v8::Global<::v8::Function>)>>>;

#[derive(Clone)]
pub(crate) struct RecordBatchIterInternal {
    input: RecordBatch,
    function: Function,
    big_decimal: ::v8::Global<::v8::Function>,
    schema: SchemaRef,
    chunk_size: usize,
    promise: Rc<RefCell<Option<::v8::Global<::v8::Promise>>>>,
    // mutable states
    generator: GeneratorState,
    /// Current row index.
    row: Rc<RefCell<usize>>,
    state: RecordBatchIterState,
}

#[derive(Debug, Clone)]
pub(crate) enum RecordBatchIterState {
    Processing,
    WaitingForIteratorPromise,
    WaitingForNextPromise,
}

unsafe impl Send for RecordBatchIterInternal {}

pub struct Runtime {
    local_pool: tokio_spawn_pinned::LocalPoolHandle,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime").finish()
    }
}

impl Runtime {
    pub fn new() -> Arc<Self> {
        THREAD_RUNTIME.with(|runtime| runtime.clone())
    }

    pub fn new_internal() -> Self {
        Self {
            local_pool: tokio_spawn_pinned::LocalPoolHandle::new_with_runtime_creator(1, || {
                tokio::runtime::Builder::new_current_thread()
                    .thread_name("local-pool")
                    .on_thread_stop(on_thread_stop)
                    .enable_all()
                    .build()
                    .expect("Failed to start a pinned worker thread runtime")
            }),
        }
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn call(&self, name: &str, input: RecordBatch) -> Result<RecordBatch> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                let runtime = runtime.borrow();
                runtime.call(name, &input).await
            } else {
                let name = name.to_string();
                self.local_pool
                    .spawn_pinned(|| async move {
                        let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                        let runtime = runtime.borrow();
                        runtime.call(&name, &input).await
                    })
                    .await?
            }
        } else {
            Err(anyhow::anyhow!("No tokio runtime found"))
        }
    }

    /// Add a JS function.
    pub async fn add_function(
        &self,
        name: &str,
        return_type: DataType,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread {
                let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                let mut runtime = runtime.borrow_mut();
                runtime.add_function(name, return_type, mode, code)
            } else {
                let name = name.to_string();
                let code = code.to_string();
                self.local_pool
                    .spawn_pinned(|| async move {
                        let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                        let mut runtime = runtime.borrow_mut();
                        runtime.add_function(&name, return_type, mode, &code)
                    })
                    .await?
            }
        } else {
            Err(anyhow::anyhow!("No tokio runtime found"))
        }
    }

    pub async fn call_table_function(
        &self,
        name: &str,
        input: RecordBatch,
        chunk_size: usize,
    ) -> Result<RecordBatchIter> {
        if let Ok(current) = tokio::runtime::Handle::try_current() {
            let inner = if current.runtime_flavor() == tokio::runtime::RuntimeFlavor::CurrentThread
            {
                let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                let runtime = runtime.borrow();
                runtime.call_table_function(name, input, chunk_size)
            } else {
                let name = name.to_string();
                self.local_pool
                    .spawn_pinned(move || async move {
                        let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
                        let runtime = runtime.borrow();
                        runtime.call_table_function(&name, input, chunk_size)
                    })
                    .await?
            }?;

            Ok(RecordBatchIter {
                inner,
                local_pool: self.local_pool.clone(),
            })
        } else {
            Err(anyhow::anyhow!("No tokio runtime found"))
        }
    }
}

impl InternalRuntime {
    pub fn new() -> Self {
        let deno_runtime = Rc::new(RefCell::new(arrow_udf_js_deno_runtime::create_runtime(
            SNAPSHOT,
        )));
        let big_decimal = {
            let mut runtime = deno_runtime.borrow_mut();
            let js_runtime = runtime.get_js_runtime();
            let scope = &mut js_runtime.handle_scope();
            let context = scope.get_current_context();
            let global = context.global(scope);
            let key = ::v8::String::new(scope, "BigDecimal").unwrap();
            let big_decimal = global.get(scope, key.into()).unwrap();
            let big_decimal = ::v8::Local::<::v8::Function>::try_from(big_decimal).unwrap();
            ::v8::Global::new(scope, big_decimal)
        };

        Self {
            functions: HashMap::new(),
            deno_runtime,
            big_decimal,
        }
    }

    /// Add a JS function.
    pub fn add_function(
        &mut self,
        name: &str,
        return_type: DataType,
        mode: CallMode,
        code: &str,
    ) -> Result<()> {
        let mut js_runtime = self.deno_runtime.borrow_mut();
        let scope = &mut js_runtime.handle_scope();

        let module = crate::v8::V8::compile_module(scope, name, code)?;

        if let Some(module) = module {
            if module
                .instantiate_module(scope, crate::v8::V8::unexpected_module_resolve_callback)
                .unwrap_or_default()
            {
                let namespace = module.get_module_namespace();
                if namespace.is_module_namespace_object() {
                    let namespace_obj = namespace
                        .to_object(scope)
                        .context("Couldn't get the namespace object")?;
                    let f_str =
                        ::v8::String::new(scope, name).context("Couldn't create an string")?;

                    if let Some(f) = namespace_obj.get(scope, f_str.into()) {
                        if let Ok(function) = ::v8::Local::<::v8::Function>::try_from(f) {
                            let function = ::v8::Global::new(scope, function);

                            let function = Function {
                                mode,
                                function,
                                return_type,
                            };
                            self.functions.insert(name.to_string(), function);
                            return Ok(());
                        } else {
                            // esbuild exports all the functions a default export
                            if module.evaluate(scope).is_some() {
                                let default_export_name =
                                    ::v8::String::new(scope, "default").unwrap();
                                if let Some(obj) =
                                    namespace_obj.get(scope, default_export_name.into())
                                {
                                    if let Ok(obj) = ::v8::Local::<::v8::Object>::try_from(obj) {
                                        let f_str = ::v8::String::new(scope, name)
                                            .context("Couldn't create an string")?;
                                        if let Some(f) = obj.get(scope, f_str.into()) {
                                            if let Ok(function) =
                                                ::v8::Local::<::v8::Function>::try_from(f)
                                            {
                                                let function = ::v8::Global::new(scope, function);
                                                let function = Function {
                                                    mode,
                                                    function,
                                                    return_type,
                                                };
                                                self.functions.insert(name.to_string(), function);
                                                return Ok(());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        anyhow::bail!("Function {} could not be added", name);
    }

    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        let function = self.functions.get(name).context("function not found")?;
        let mut js_runtime = self.deno_runtime.borrow_mut();
        js_runtime.v8_isolate().perform_microtask_checkpoint();
        let mut results = Vec::with_capacity(input.num_rows());

        let mut promises = Vec::with_capacity(input.num_rows());

        let mut cancel_receiver: Option<tokio::sync::mpsc::Receiver<String>> = None;

        let abort_controller = {
            let scope = &mut js_runtime.handle_scope();
            let abort_controller = v8::V8::create_abort_controller_context(scope)?;

            let try_catch = &mut ::v8::TryCatch::new(scope);

            let f = ::v8::Local::<::v8::Function>::new(try_catch, function.function.clone());

            let mut args = Vec::with_capacity(input.num_columns());

            for i in 0..input.num_rows() {
                args.clear();
                for column in input.columns() {
                    let val = deno_arrow::get_jsvalue(try_catch, column, &self.big_decimal, i)
                        .context("failed to get jsvalue from arrow array")?;
                    args.push(val);
                }

                if function.mode == CallMode::ReturnNullOnNullInput
                    && args.iter().any(|v| v.is_null())
                {
                    let null: ::v8::Local<::v8::Value> = ::v8::null(try_catch).into();
                    results.push(::v8::Global::new(try_catch, null));
                    continue;
                }

                let recv = ::v8::undefined(try_catch).into();

                match f.call(try_catch, recv, &args) {
                    Some(val) => {
                        if let Ok(promise) = ::v8::Local::<::v8::Promise>::try_from(val) {
                            let undefined: ::v8::Local<::v8::Value> =
                                ::v8::undefined(try_catch).into();
                            results.push(::v8::Global::new(try_catch, undefined));
                            promises.push((i, ::v8::Global::new(try_catch, promise)));
                        } else {
                            results.push(::v8::Global::new(try_catch, val));
                        }
                    }
                    None => {
                        assert!(try_catch.has_caught());
                        //Avoids killing the isolate even if it was requested
                        if try_catch.is_execution_terminating() {
                            try_catch.cancel_terminate_execution();
                            anyhow::bail!("Execution was terminated");
                        }

                        let exception = try_catch.exception().unwrap();
                        let result =
                            crate::v8::V8::exception_to_err_result(try_catch, exception, false);
                        return result;
                    }
                }
            }
            abort_controller
        };

        if !promises.is_empty() {
            let mut idx = Vec::with_capacity(promises.len());
            let mut values = Vec::with_capacity(promises.len());
            for (i, promise) in promises {
                idx.push(i);
                values.push(promise);
            }
            let values_future = values_future::ValuesFuture::new(
                js_runtime.get_js_runtime(),
                Some(abort_controller),
                &mut cancel_receiver,
                values,
            );

            let result_values = values_future.await?;
            for (i, result) in idx.into_iter().zip(result_values) {
                results[i] = result;
            }
        }

        let scope = &mut js_runtime.handle_scope();
        let try_catch = &mut ::v8::TryCatch::new(scope);

        let array = deno_arrow::build_array(&function.return_type, try_catch, results)
            .context("failed to build arrow array from return values")?;
        let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![array])?)
    }

    /// Call a table function.
    pub(crate) fn call_table_function(
        &self,
        name: &str,
        input: RecordBatch,
        chunk_size: usize,
    ) -> Result<RecordBatchIterInternal> {
        assert!(chunk_size > 0);
        let function = self.functions.get(name).context("function not found")?;

        // initial state
        Ok(RecordBatchIterInternal {
            input,
            function: function.clone(),
            schema: Arc::new(Schema::new(vec![
                Field::new("row", DataType::Int32, true),
                Field::new(name, function.return_type.clone(), true),
            ])),
            big_decimal: self.big_decimal.clone(),
            chunk_size,
            row: Rc::new(RefCell::new(0)),
            generator: Rc::new(RefCell::new(None)),
            promise: Rc::new(RefCell::new(None)),
            state: RecordBatchIterState::Processing,
        })
    }
}

impl Default for InternalRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordBatchIterInternal {
    /// Get the schema of the output.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn all_rows(&self) -> bool {
        let r = self.row.borrow();
        *r >= self.input.num_rows()
    }

    fn get_generated_object(
        scope: &mut ::v8::HandleScope<'_>,
        value: ::v8::Local<::v8::Value>,
    ) -> Result<(::v8::Global<::v8::Value>, bool)> {
        let object = ::v8::Local::<::v8::Object>::try_from(value)
            .ok()
            .context("failed to get object")?;
        let key = ::v8::String::new(scope, "value").context("failed to create 'value' string")?;
        let value = object
            .get(scope, key.into())
            .context("failed to get 'value' property")?;

        let key = ::v8::String::new(scope, "done").context("failed to create 'done' string")?;
        let done = object
            .get(scope, key.into())
            .context("failed to get 'done' property")?;
        let done = done.boolean_value(scope);
        let value = ::v8::Global::new(scope, value);
        Ok((value, done))
    }

    fn get_generator_and_function(
        scope: &mut ::v8::HandleScope<'_>,
        value: ::v8::Local<::v8::Value>,
    ) -> Result<(::v8::Global<::v8::Object>, ::v8::Global<::v8::Function>)> {
        let mut gen = ::v8::Local::<::v8::Object>::try_from(value)
            .ok()
            .context("failed to get generator object")?;

        let iter_symbol = ::v8::Symbol::get_async_iterator(scope);
        if let Some(iter) = gen.get(scope, iter_symbol.into()) {
            if iter.is_function() {
                let function = ::v8::Local::<::v8::Function>::try_from(iter)
                    .ok()
                    .context("failed to get generator object")?;

                if let Some(value) = function.call(scope, gen.into(), &[]) {
                    gen = ::v8::Local::<::v8::Object>::try_from(value)
                        .ok()
                        .context("failed to get generator object")?;
                }
            }
        }

        let key = ::v8::String::new(scope, "next").context("failed to create 'next' string")?;
        let next = gen
            .get(scope, key.into())
            .context("failed to get 'next' method")?;

        let next = ::v8::Local::<::v8::Function>::try_from(next)
            .ok()
            .context("failed to get 'next' method")?;

        let gen = ::v8::Global::new(scope, gen);
        let next = ::v8::Global::new(scope, next);
        Ok((gen, next))
    }
}

impl Stream for RecordBatchIterInternal {
    type Item = Result<RecordBatch>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = self.get_mut();

        let runtime = THREAD_ISOLATE.with(|isolate| isolate.clone());
        let runtime = runtime.borrow();

        let mut deno_runtime = runtime.deno_runtime.borrow_mut();
        let js_runtime = deno_runtime.get_js_runtime();

        let state = js_runtime.poll_event_loop(cx, Default::default());
        let scope = &mut js_runtime.handle_scope();

        let mut indexes = Int32Builder::with_capacity(inner.chunk_size);
        let mut results = Vec::with_capacity(inner.input.num_rows());

        let promise = {
            let p = inner.promise.borrow();
            p.clone()
        };

        let mut continue_processing = true;

        if let Some(promise) = promise {
            continue_processing = false;
            let promise = ::v8::Local::<::v8::Promise>::new(scope, promise);
            match promise.state() {
                ::v8::PromiseState::Pending => match state {
                    Poll::Ready(Ok(_)) => {
                        let msg = "Promise resolution is still pending but the event loop has already resolved.";
                        return Poll::Ready(Some(Err(deno_core::error::generic_error(msg))));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                },
                ::v8::PromiseState::Fulfilled => {
                    _ = inner.promise.take();
                    let value = promise.result(scope);
                    if matches!(inner.state, RecordBatchIterState::WaitingForIteratorPromise) {
                        let (gen, next) = Self::get_generator_and_function(scope, value)?;
                        let mut generator = inner.generator.borrow_mut();
                        *generator = Some((gen, next));
                        drop(generator);
                        continue_processing = true;
                    } else {
                        let (value, done) = Self::get_generated_object(scope, value)?;
                        if done {
                            let mut row = inner.row.borrow_mut();
                            *row += 1;
                            drop(row);

                            let mut gen = inner.generator.borrow_mut();
                            *gen = None;
                            drop(gen);
                            if !inner.all_rows() {
                                let waker = cx.waker().clone();
                                waker.wake();
                                return Poll::Pending;
                            } else {
                                return Poll::Ready(None);
                            }
                        } else {
                            let r = inner.row.borrow();
                            indexes.append_value(*r as i32);
                            results.push(value);
                        }
                    }
                    inner.state = RecordBatchIterState::Processing;
                }
                ::v8::PromiseState::Rejected => {
                    let exception = promise.result(scope);
                    inner.state = RecordBatchIterState::Processing;
                    return Poll::Ready(Some(v8::V8::exception_to_err_result(
                        scope, exception, false,
                    )));
                }
            }
        }

        if continue_processing {
            let mut row = Vec::with_capacity(inner.input.num_columns());

            let mut generator = inner.generator.take();

            while !inner.all_rows() && results.len() < inner.chunk_size {
                let (gen, next) = if let Some(g) = generator.as_ref() {
                    g
                } else {
                    // call the table function to get a generator
                    row.clear();
                    for column in inner.input.columns() {
                        let r = inner.row.borrow();
                        let val = get_jsvalue(scope, &column, &inner.big_decimal, *r)
                            .context("failed to get jsvalue from arrow array")?;

                        row.push(val);
                    }
                    if inner.function.mode == CallMode::ReturnNullOnNullInput
                        && row.iter().any(|v| v.is_null())
                    {
                        let mut row = inner.row.borrow_mut();
                        *row += 1;
                        continue;
                    }

                    let try_catch = &mut ::v8::TryCatch::new(scope);

                    let js_function = ::v8::Local::<::v8::Function>::new(
                        try_catch,
                        inner.function.function.clone(),
                    );

                    let recv = ::v8::undefined(try_catch).into();

                    match js_function.call(try_catch, recv, &row) {
                        Some(gen) => {
                            if gen.is_promise() {
                                let promise = match ::v8::Local::<::v8::Promise>::try_from(gen) {
                                    Ok(promise) => promise,
                                    Err(_) => {
                                        let msg = "The value returned by the table function is a promise but it is not a promise object";
                                        return Poll::Ready(Some(Err(anyhow::anyhow!(msg))));
                                    }
                                };

                                let mut p = inner.promise.borrow_mut();
                                *p = Some(::v8::Global::new(try_catch, promise));
                                inner.state = RecordBatchIterState::WaitingForIteratorPromise;
                                let waker = cx.waker().clone();
                                waker.wake();
                                return Poll::Pending;
                            }
                            let (gen, next) = Self::get_generator_and_function(try_catch, gen)?;
                            generator.insert((gen, next))
                        }
                        None => {
                            assert!(try_catch.has_caught());
                            //Avoids killing the isolate even if it was requested
                            if try_catch.is_execution_terminating() {
                                try_catch.cancel_terminate_execution();
                                return Poll::Ready(Some(Err(anyhow::anyhow!(
                                    "Execution was terminated"
                                ))));
                            }

                            let exception = try_catch.exception().unwrap();
                            let result =
                                v8::V8::exception_to_err_result(try_catch, exception, false);
                            return Poll::Ready(Some(result));
                        }
                    }
                };

                let recv = ::v8::Local::new(scope, gen.clone());

                let next = ::v8::Local::new(scope, next.clone());
                let try_catch = &mut ::v8::TryCatch::new(scope);

                match next.call(try_catch, recv.into(), &[]) {
                    Some(object) => {
                        if object.is_promise() {
                            let promise = match ::v8::Local::<::v8::Promise>::try_from(object) {
                                Ok(promise) => promise,
                                Err(_) => {
                                    let msg = "The value returned by the table function is a promise but it is not a promise object";
                                    return Poll::Ready(Some(Err(anyhow::anyhow!(msg))));
                                }
                            };
                            let mut p = inner.promise.borrow_mut();
                            *p = Some(::v8::Global::new(try_catch, promise));

                            inner.state = RecordBatchIterState::WaitingForNextPromise;
                            break;
                        } else {
                            let (value, done) = Self::get_generated_object(try_catch, object)?;

                            if done {
                                let mut row = inner.row.borrow_mut();
                                *row += 1;
                                generator = None;
                                continue;
                            }

                            let value = ::v8::Global::new(try_catch, value);
                            let r = inner.row.borrow();
                            indexes.append_value(*r as i32);
                            results.push(value);
                        }
                    }

                    None => {
                        assert!(try_catch.has_caught());
                        //Avoids killing the isolate even if it was requested
                        if try_catch.is_execution_terminating() {
                            try_catch.cancel_terminate_execution();
                            return Poll::Ready(Some(Err(anyhow::anyhow!(
                                "Execution was terminated"
                            ))));
                        }

                        let exception = try_catch.exception().unwrap();
                        let result = v8::V8::exception_to_err_result(try_catch, exception, false);
                        return Poll::Ready(Some(result));
                    }
                }
            }
            let mut gen = inner.generator.borrow_mut();
            *gen = generator;
            drop(gen);

            if results.is_empty() {
                if inner.promise.borrow().is_none() {
                    return Poll::Ready(None);
                } else {
                    let waker = cx.waker().clone();
                    waker.wake();
                    return Poll::Pending;
                }
            }
        }

        let indexes = Arc::new(indexes.finish());
        let array = deno_arrow::build_array(&inner.function.return_type, scope, results)
            .context("failed to build arrow array from return values")?;

        match RecordBatch::try_new(inner.schema.clone(), vec![indexes, array]) {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(e) => Poll::Ready(Some(Err(anyhow::anyhow!(e.to_string())))),
        }
    }
}
