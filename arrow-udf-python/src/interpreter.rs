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

//! High-level API for Python sub-interpreters.

use pyo3::{ffi::*, prepare_freethreaded_python, PyErr, PyResult, Python};

/// A Python sub-interpreter.
pub struct SubInterpreter {
    // NOTE: thread state is only valid in the thread that created it
    state: *mut PyThreadState,
}

impl SubInterpreter {
    /// Create a new Python sub-interpreter.
    pub fn new() -> PyResult<Self> {
        prepare_freethreaded_python();
        // reference: https://github.com/PyO3/pyo3/blob/9a36b5078989a7c07a5e880aea3c6da205585ee3/examples/sequential/tests/test.rs
        let main_state = unsafe { PyThreadState_Swap(std::ptr::null_mut()) };
        let config = PyInterpreterConfig {
            use_main_obmalloc: 0,
            allow_fork: 0,
            allow_exec: 0,
            allow_threads: 0,
            allow_daemon_threads: 0,
            check_multi_interp_extensions: 1,
            gil: PyInterpreterConfig_OWN_GIL,
        };
        let mut state: *mut PyThreadState = std::ptr::null_mut();
        let status: PyStatus = unsafe { Py_NewInterpreterFromConfig(&mut state, &config) };
        if unsafe { PyStatus_IsError(status) } == 1 {
            return Err(PyErr::fetch(unsafe { Python::assume_gil_acquired() }));
        }
        unsafe { PyThreadState_Swap(main_state) };
        Ok(Self { state })
    }

    /// Run a closure in the sub-interpreter.
    pub fn with_gil<F, R>(&self, f: F) -> R
    where
        F: for<'py> FnOnce(Python<'py>) -> R,
    {
        // switch to the sub-interpreter
        let main_state = unsafe { PyThreadState_Swap(self.state) };

        let ret = Python::with_gil(f);

        // switch back to the main interpreter
        unsafe { PyThreadState_Swap(main_state) };
        ret
    }
}

impl Drop for SubInterpreter {
    fn drop(&mut self) {
        unsafe {
            // switch to the sub-interpreter
            let main_state = PyThreadState_Swap(self.state);
            // destroy the sub-interpreter
            Py_EndInterpreter(self.state);
            // switch back to the main interpreter
            PyThreadState_Swap(main_state);
        }
    }
}
