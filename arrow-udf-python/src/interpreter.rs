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

use anyhow::{bail, Result};
use pyo3::{ffi::*, prepare_freethreaded_python, GILPool, PyErr, Python};

use super::pyerr_to_anyhow;

/// A Python sub-interpreter with its own GIL.
#[derive(Debug)]
pub struct SubInterpreter {
    // NOTE: thread state is only valid in the thread that created it
    state: *mut PyThreadState,
}

// XXX: not sure if this is safe
unsafe impl Send for SubInterpreter {}
unsafe impl Sync for SubInterpreter {}

impl SubInterpreter {
    /// Create a new sub-interpreter.
    pub fn new() -> Result<Self> {
        prepare_freethreaded_python();
        // reference: https://github.com/PyO3/pyo3/blob/9a36b5078989a7c07a5e880aea3c6da205585ee3/examples/sequential/tests/test.rs
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
            bail!(PyErr::fetch(unsafe { Python::assume_gil_acquired() }).to_string());
        }
        // release the GIL
        unsafe { PyEval_SaveThread() };
        Ok(Self { state })
    }

    /// Run a closure in the sub-interpreter.
    ///
    /// Please note that if the return value contains any `Py` object (e.g. `PyErr`),
    /// this object must be dropped in this sub-interpreter, otherwise it will cause
    /// `SIGABRT: pointer being freed was not allocated`.
    pub fn with_gil<F, R>(&self, f: F) -> R
    where
        F: for<'py> FnOnce(Python<'py>) -> R,
    {
        // switch to the sub-interpreter and acquire GIL
        unsafe { PyEval_RestoreThread(self.state) };

        // Safety: the GIL is already held
        // this pool is used to increment the internal GIL count of pyo3.
        let pool = unsafe { GILPool::new() };
        let ret = f(pool.python());
        drop(pool);

        // release the GIL
        unsafe { PyEval_SaveThread() };
        ret
    }

    /// Run Python code in the sub-interpreter.
    pub fn run(&self, code: &str) -> Result<()> {
        self.with_gil(|py| py.run(code, None, None).map_err(pyerr_to_anyhow))?;
        Ok(())
    }
}

impl Drop for SubInterpreter {
    fn drop(&mut self) {
        unsafe {
            // switch to the sub-interpreter
            PyEval_RestoreThread(self.state);
            // destroy the sub-interpreter
            Py_EndInterpreter(self.state);
        }
    }
}
