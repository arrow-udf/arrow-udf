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

use std::{
    cell::RefCell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

use arrow_udf_js_deno_runtime::deno_runtime::DenoRuntime;
use futures::Future;

pub(super) struct ValuesFuture {
    runtime: Rc<RefCell<DenoRuntime>>,
    promises: Vec<v8::Global<v8::Promise>>,
}

impl ValuesFuture {
    pub(super) fn new(
        runtime: Rc<RefCell<DenoRuntime>>,
        promises: Vec<v8::Global<v8::Promise>>,
    ) -> Self {
        Self { runtime, promises }
    }
}

impl Future for ValuesFuture {
    type Output = anyhow::Result<Vec<v8::Global<v8::Value>>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let s = &mut *self;

        let mut runtime = s.runtime.borrow_mut();
        let js_runtime = runtime.get_js_runtime();

        let state = js_runtime.poll_event_loop(cx, Default::default());
        let scope = &mut js_runtime.handle_scope();

        let mut has_pending = false;
        let mut results = Vec::with_capacity(s.promises.len());
        for promise in s.promises.iter() {
            let promise = v8::Local::<v8::Promise>::new(scope, promise.clone());
            match promise.state() {
                v8::PromiseState::Pending => match state {
                    Poll::Ready(Ok(_)) => {
                        let msg = "Promise resolution is still pending but the event loop has already resolved.";
                        return Poll::Ready(Err(deno_core::error::generic_error(msg)));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => {
                        has_pending = true;
                        break;
                    }
                },
                v8::PromiseState::Rejected => {
                    let exception = promise.result(scope);
                    return Poll::Ready(crate::v8::V8::exception_to_err_result(
                        scope, exception, false,
                    ));
                }
                v8::PromiseState::Fulfilled => {
                    let value = promise.result(scope);
                    let value_handle = v8::Global::new(scope, value);
                    results.push(value_handle);
                }
            }
        }
        if has_pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(results))
        }
    }
}
