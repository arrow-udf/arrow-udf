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

use anyhow::Context;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

static ABORT_COUNTER: OnceLock<Arc<AtomicU64>> = OnceLock::new();

pub struct V8 {}

impl V8 {
    pub fn exception_to_err_result<T>(
        scope: &mut v8::HandleScope<'_>,
        exception: v8::Local<v8::Value>,
        in_promise: bool,
    ) -> anyhow::Result<T> {
        let is_terminating_exception = scope.is_execution_terminating();
        let mut exception = exception;

        if is_terminating_exception {
            // TerminateExecution was called. Cancel exception termination so that the
            // exception can be created..
            scope.cancel_terminate_execution();

            // Maybe make a new exception object.
            if exception.is_null_or_undefined() {
                let message = v8::String::new(scope, "execution terminated").unwrap();
                exception = v8::Exception::error(scope, message);
            }
        }

        let mut js_error = deno_core::error::JsError::from_v8_exception(scope, exception);
        if in_promise {
            js_error.exception_message = format!(
                "Uncaught (in promise) {}",
                js_error.exception_message.trim_start_matches("Uncaught ")
            );
        }

        if is_terminating_exception {
            // Re-enable exception termination.
            scope.terminate_execution();
        }

        if let Some(name) = &js_error.name {
            if name == "CancelRequest" {
                if let Some(reason) = js_error.message {
                    anyhow::bail!("CancelRequest: {}", reason);
                }
            }
        }

        Err(js_error.into())
    }

    pub fn script_origin<'a>(
        s: &mut v8::HandleScope<'a>,
        resource_name: v8::Local<'a, v8::String>,
    ) -> v8::ScriptOrigin<'a> {
        let source_map_url = v8::String::new(s, "").unwrap();
        v8::ScriptOrigin::new(
            s,
            resource_name.into(),
            0,
            0,
            false,
            123,
            source_map_url.into(),
            true,
            false,
            false,
        )
    }

    pub fn module_origin<'a>(
        s: &mut v8::HandleScope<'a>,
        resource_name: v8::Local<'a, v8::String>,
    ) -> v8::ScriptOrigin<'a> {
        let source_map_url = v8::String::new(s, "").unwrap();
        v8::ScriptOrigin::new(
            s,
            resource_name.into(),
            0,
            0,
            false,
            123,
            source_map_url.into(),
            true,
            false,
            true,
        )
    }

    pub fn compile_module<'s>(
        scope: &mut v8::HandleScope<'s>,
        name: &str,
        source_code: &str,
    ) -> anyhow::Result<Option<v8::Local<'s, v8::Module>>> {
        let source =
            v8::String::new(scope, source_code).context("Error creating source code string")?;
        let name = v8::String::new(scope, name).context("Error creating function name string")?;
        let origin = V8::module_origin(scope, name);
        let source = v8::script_compiler::Source::new(source, Some(&origin));
        Ok(v8::script_compiler::compile_module(scope, source))
    }

    pub fn unexpected_module_resolve_callback<'a>(
        _context: v8::Local<'a, v8::Context>,
        _specifier: v8::Local<'a, v8::String>,
        _import_assertions: v8::Local<'a, v8::FixedArray>,
        _referrer: v8::Local<'a, v8::Module>,
    ) -> Option<v8::Local<'a, v8::Module>> {
        unreachable!()
    }

    pub fn compile_script<'s>(
        scope: &mut v8::HandleScope<'s>,
        name: &str,
        source_code: &str,
    ) -> anyhow::Result<Option<v8::Local<'s, v8::Script>>> {
        let source =
            v8::String::new(scope, source_code).context("Error creating source code string")?;
        let name = v8::String::new(scope, name).context("Error creating function name string")?;
        let origin = V8::script_origin(scope, name);
        Ok(v8::Script::compile(scope, source, Some(&origin)))
    }

    pub fn execute_script<'s>(
        try_catch: &mut v8::TryCatch<'s, v8::HandleScope>,
        name: &str,
        source_code: &str,
    ) -> anyhow::Result<v8::Local<'s, v8::Value>> {
        let script =
            V8::compile_script(try_catch, name, source_code)?.context("Invalid source code")?;

        match script.run(try_catch) {
            Some(value) => anyhow::Ok(value),
            None => {
                assert!(try_catch.has_caught());
                if try_catch.is_execution_terminating() {
                    try_catch.cancel_terminate_execution();
                    anyhow::bail!("TerminateExecution was called");
                }
                let exception = try_catch.exception().unwrap();
                V8::exception_to_err_result(try_catch, exception, false)
            }
        }
    }

    pub fn create_abort_controller(
        scope: &mut v8::HandleScope<'_>,
    ) -> anyhow::Result<(v8::Global<v8::Value>, v8::Global<v8::Value>)> {
        let try_catch = &mut v8::TryCatch::new(scope);
        let abort_controller =
            V8::execute_script(try_catch, "abortcontroller", "new AbortController()")?;
        let abort_signal = {
            let obj = v8::Local::<v8::Object>::try_from(abort_controller)?;

            let key = v8::String::new(try_catch, "signal").unwrap();
            let s = obj
                .get(try_catch, key.into())
                .context("Invalid signal attribute")?;
            if let Ok(obj) = v8::Local::<v8::Object>::try_from(s) {
                let counter = ABORT_COUNTER
                    .get_or_init(|| Arc::new(AtomicU64::new(0)))
                    .clone();
                let idx = counter.fetch_add(1, Ordering::SeqCst);
                let value = v8::BigInt::new_from_u64(try_catch, idx);
                let key = v8::String::new(try_catch, "abortId").unwrap();
                obj.set(try_catch, key.into(), value.into());
            }

            v8::Global::new(try_catch, s)
        };
        Ok((v8::Global::new(try_catch, abort_controller), abort_signal))
    }

    pub(crate) fn set_promise_hooks_for_abort_signal(
        scope: &mut v8::HandleScope<'_>,
        abort_signal: &v8::Global<v8::Value>,
    ) {
        let context = scope.get_current_context();
        let global = context.global(scope);
        let key = v8::String::new(scope, "__promiseReady").unwrap();
        if global
            .get(scope, key.into())
            .map(|k| k.is_undefined())
            .unwrap_or(true)
        {
            let value = v8::Boolean::new(scope, true);
            global.set(scope, key.into(), value.into());
        }

        let key = v8::String::new(scope, "__abortSignal").unwrap();
        let local_signal = v8::Local::new(scope, abort_signal);
        global.set(scope, key.into(), local_signal).unwrap();
        context.set_slot(scope, abort_signal.clone());
    }

    pub fn create_abort_controller_context(
        scope: &mut v8::HandleScope<'_>,
    ) -> anyhow::Result<v8::Global<v8::Value>> {
        let (abort_controller, abort_signal) = V8::create_abort_controller(scope)?;

        Self::set_promise_hooks_for_abort_signal(scope, &abort_signal);

        Ok(abort_controller)
    }

    #[allow(dead_code)]
    /// Grab a Global handle to a v8 value returned by the expression
    pub fn grab<'s, T>(
        scope: &mut v8::HandleScope<'s>,
        root: v8::Local<'s, v8::Value>,
        path: &str,
    ) -> Option<v8::Local<'s, T>>
    where
        v8::Local<'s, T>: TryFrom<v8::Local<'s, v8::Value>, Error = v8::DataError>,
    {
        path.split('.')
            .try_fold(root, |p, k| {
                v8::Local::<v8::Object>::try_from(p)
                    .ok()
                    .and_then(|p| v8::String::new(scope, k).and_then(|k| p.get(scope, k.into())))
            })
            .and_then(|v| v8::Local::<T>::try_from(v).ok())
    }
}
