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
    pin::Pin,
    task::{Context, Poll},
};

use deno_core::JsRuntime;
use futures::Future;
use tokio::sync::mpsc::Receiver;

pub(super) struct ValuesFuture<'s> {
    runtime: &'s mut JsRuntime,
    promises: Vec<v8::Global<v8::Promise>>,
    abort_controller: Option<v8::Global<v8::Value>>,
    cancel_receiver: &'s mut Option<Receiver<String>>,
}

impl<'s> ValuesFuture<'s> {
    pub(super) fn new(
        runtime: &'s mut JsRuntime,
        abort_controller: Option<v8::Global<v8::Value>>,
        cancel_receiver: &'s mut Option<Receiver<String>>,
        promises: Vec<v8::Global<v8::Promise>>,
    ) -> Self {
        Self {
            runtime,
            promises,
            abort_controller,
            cancel_receiver,
        }
    }
}

impl<'s> Future for ValuesFuture<'s> {
    type Output = anyhow::Result<Vec<v8::Global<v8::Value>>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let s = &mut *self;

        let mut cancel_executed = false;
        if let Some(abort_controller) = &s.abort_controller {
            if let Some(cancel_receiver) = s.cancel_receiver.as_mut() {
                if let Poll::Ready(Some(message)) = cancel_receiver.poll_recv(cx) {
                    // Execute the cancel method and throw an exeception

                    let scope = &mut s.runtime.handle_scope();
                    let local = v8::Local::new(scope, abort_controller.clone());

                    if let Some(abort_function) = v8::Local::<v8::Object>::try_from(local)
                        .ok()
                        .and_then(|obj| {
                            v8::String::new(scope, "abort")
                                .and_then(|key| obj.get(scope, key.into()))
                                .and_then(|v| v8::Local::<v8::Function>::try_from(v).ok())
                        })
                    {
                        let reason = v8::String::new(scope, message.as_str()).unwrap();
                        abort_function.call(scope, local, &[reason.into()]);
                    }
                    cancel_executed = true;
                }
            }
        }

        let state = s.runtime.poll_event_loop(cx, Default::default());
        let scope = &mut s.runtime.handle_scope();

        if cancel_executed {
            s.cancel_receiver.take();
            return Poll::Ready(Err(anyhow::anyhow!("The promise was canceled")));
        }

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
