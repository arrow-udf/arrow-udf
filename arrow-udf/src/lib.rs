// Copyright 2023 RisingWave Labs
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

use arrow_array::{ArrayRef, RecordBatch};
pub use arrow_udf_macros::function;
pub use error::{Error, Result};

mod byte_builder;
pub mod error;
#[cfg(feature = "global_registry")]
pub mod sig;

/// A scalar function that operates on a record batch.
pub type ScalarFunction = fn(input: &RecordBatch) -> Result<ArrayRef>;

/// Types used by the generated code.
pub mod types {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Interval {
        pub months: i32,
        pub days: i32,
        pub nanos: i64,
    }
}

/// Internal APIs used by macros.
#[doc(hidden)]
pub mod codegen {
    pub use crate::byte_builder::*;
    pub use arrow_arith;
    pub use arrow_array;
    pub use arrow_schema;
    pub use chrono;
    pub use itertools;
    #[cfg(feature = "global_registry")]
    pub use linkme;
    pub use rust_decimal;
    pub use serde_json;

    use crate::{error::MultiError, Error, ScalarFunction};
    use arrow_array::RecordBatch;
    use arrow_ipc::{reader::FileReader, writer::FileWriter};
    use arrow_schema::{Field, Schema};
    use std::{mem::ManuallyDrop, sync::Arc};

    #[no_mangle]
    unsafe extern "C" fn alloc(len: usize, align: usize) -> *mut u8 {
        std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(len, align))
    }

    #[no_mangle]
    unsafe extern "C" fn dealloc(ptr: *mut u8, len: usize, align: usize) {
        std::alloc::dealloc(
            ptr,
            std::alloc::Layout::from_size_align_unchecked(len, align),
        );
    }

    #[no_mangle]
    #[used]
    static ARROWUDF_VERSION: u8 = 1;

    #[repr(C)]
    #[derive(Debug)]
    pub struct FFIResult {
        pub out_ptr: *const u8,
        pub out_len: usize,
        pub err_ptr: *const u8,
        pub err_len: usize,
    }

    /// A wrapper function for calling a scalar function from C.
    ///
    /// The input record batch is read from the IPC buffer pointed to by `ptr` and `len`.
    /// The output record batch is written to the IPC buffer pointed to by `out_ptr` and `out_len`.
    /// The error message is written to the buffer pointed to by `err_ptr` and `err_len`.
    /// If there is no batch / error, `out_ptr` / `err_ptr` is set to null.
    /// The caller is responsible for deallocating the output buffer if it is not null.
    ///
    /// # Safety
    ///
    /// `ptr`, `len` must point to a valid buffer.
    pub unsafe fn scalar_ffi_wrapper(
        function: ScalarFunction,
        ptr: *const u8,
        len: usize,
    ) -> FFIResult {
        let input = std::slice::from_raw_parts(ptr, len);
        match call_scalar(function, input) {
            Ok((data, err)) => {
                let mut msg = ManuallyDrop::new(err.map(|e| e.to_string().into_boxed_str()));
                FFIResult {
                    out_ptr: data.as_ptr(),
                    out_len: data.len(),
                    err_ptr: msg.as_mut().map_or(std::ptr::null(), |s| s.as_ptr()) as _,
                    err_len: msg.as_mut().map_or(0, |s| s.len()),
                }
            }
            Err(err) => {
                let msg = ManuallyDrop::new(err.to_string().into_boxed_str());
                FFIResult {
                    out_ptr: std::ptr::null(),
                    out_len: 0,
                    err_ptr: msg.as_ptr() as _,
                    err_len: msg.len(),
                }
            }
        }
    }

    fn call_scalar(
        function: ScalarFunction,
        input_bytes: &[u8],
    ) -> Result<(Box<[u8]>, Option<MultiError>), Error> {
        let mut reader = FileReader::try_new(std::io::Cursor::new(input_bytes), None)?;
        let input_batch = reader.next().unwrap()?;

        let (output_array, errors) = match function(&input_batch) {
            Ok(array) => (array, None),
            Err(Error::Function(array, err)) => (array, Some(err)),
            Err(err) => return Err(err),
        };

        let mut buf = vec![];
        // Write data to IPC buffer
        let schema = Schema::new(vec![Field::new(
            "result",
            output_array.data_type().clone(),
            true,
        )]);
        let mut writer = FileWriter::try_new(&mut buf, &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(output_array)])?;
        writer.write(&batch)?;
        writer.finish()?;
        drop(writer);

        Ok((buf.into(), errors))
    }
}
