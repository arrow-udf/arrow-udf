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

use deno_core::JsRuntime;

pub struct DenoRuntime {
    runtime: Option<JsRuntime>,
}

impl Drop for DenoRuntime {
    fn drop(&mut self) {
        self.drop_runtime();
    }
}

impl DenoRuntime {
    pub fn new(runtime: JsRuntime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    pub fn v8_isolate(&mut self) -> &mut v8::OwnedIsolate {
        self.get_js_runtime().v8_isolate()
    }

    pub fn get_js_runtime(&mut self) -> &mut JsRuntime {
        self.runtime
            .as_mut()
            .expect("Should not be called when the runtime is dropped")
    }

    #[inline]
    pub fn handle_scope(&mut self) -> v8::HandleScope {
        self.get_js_runtime().handle_scope()
    }

    pub fn drop_runtime(&mut self) {
        if let Some(mut runtime) = self.runtime.take() {
            runtime.v8_isolate().perform_microtask_checkpoint();
        }
    }
}
