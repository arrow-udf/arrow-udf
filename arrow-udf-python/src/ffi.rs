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

//! FFI interfaces.
//!
//! # Safety
//!
//! This module is not thread safe. It is intended to be used in a single threaded environment.

use anyhow::{Context, Result};
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use std::ffi::CStr;

use crate::Runtime;

#[no_mangle]
unsafe extern "C" fn alloc(len: usize) -> *mut u8 {
    std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(len, 1))
}

#[no_mangle]
unsafe extern "C" fn dealloc(ptr: *mut u8, len: usize) {
    std::alloc::dealloc(ptr, std::alloc::Layout::from_size_align_unchecked(len, 1));
}

/// Add a function.
#[no_mangle]
unsafe extern "C" fn add_function(
    name: *const i8,
    code: *const i8,
    return_type_ptr: *const u8,
    return_type_len: usize,
) -> i32 {
    let name = CStr::from_ptr(name).to_str().expect("invalid utf8");
    let code = CStr::from_ptr(code).to_str().expect("invalid utf8");
    let return_type = {
        let bytes = std::slice::from_raw_parts(return_type_ptr, return_type_len);
        let reader =
            FileReader::try_new(std::io::Cursor::new(bytes), None).expect("invalid schema");
        reader.schema().field(0).data_type().clone()
    };
    let runtime = Runtime::new(name, return_type, code).expect("failed to initialize runtime");
    RUNTIMES.push((name.to_string(), runtime));
    0
}

/// Call a function.
#[no_mangle]
unsafe extern "C" fn call(name: *const i8, ptr: *const u8, len: usize) -> u64 {
    let name = CStr::from_ptr(name).to_str().expect("invalid utf8");
    let input = std::slice::from_raw_parts(ptr, len);
    match call_internal(name, input) {
        Ok(data) => {
            let ptr = data.as_ptr();
            let len = data.len();
            std::mem::forget(data);
            ((ptr as u64) << 32) | (len as u64)
        }
        Err(_) => 0,
    }
}

static mut RUNTIMES: Vec<(String, Runtime)> = vec![];

fn call_internal(name: &str, input: &[u8]) -> Result<Box<[u8]>> {
    let mut reader = FileReader::try_new(std::io::Cursor::new(input), None)?;
    let input_batch = reader.next().unwrap()?;

    let (_, runtime) = unsafe {
        RUNTIMES
            .iter()
            .find(|(n, _)| n == name)
            .with_context(|| format!("function not found: {name}"))?
    };
    let output_batch = runtime.call(&input_batch)?;

    let mut buf = vec![];
    // Write data to IPC buffer
    let mut writer = FileWriter::try_new(&mut buf, &output_batch.schema())?;
    writer.write(&output_batch)?;
    writer.finish()?;
    drop(writer);

    Ok(buf.into())
}
