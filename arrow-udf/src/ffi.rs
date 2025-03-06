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

//! FFI interfaces.

use crate::{Error, Result};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

/// A symbol indicating the ABI version.
///
/// The version follows semantic versioning `MAJOR.MINOR`.
/// - The major version is incremented when incompatible API changes are made.
/// - The minor version is incremented when new functionality are added in a backward compatible manner.
///
/// # Changelog
///
/// - 3.0: Change type names in signatures.
/// - 2.0: Add user defined struct type.
/// - 1.0: Initial version.
#[no_mangle]
#[used]
pub static ARROWUDF_VERSION_3_0: () = ();

/// Allocate memory.
///
/// # Safety
///
/// See [`std::alloc::GlobalAlloc::alloc`].
#[no_mangle]
pub unsafe extern "C" fn alloc(len: usize, align: usize) -> *mut u8 {
    std::alloc::alloc(std::alloc::Layout::from_size_align_unchecked(len, align))
}

/// Deallocate memory.
///
/// # Safety
///
/// See [`std::alloc::GlobalAlloc::dealloc`].
#[no_mangle]
pub unsafe extern "C" fn dealloc(ptr: *mut u8, len: usize, align: usize) {
    std::alloc::dealloc(
        ptr,
        std::alloc::Layout::from_size_align_unchecked(len, align),
    );
}

/// A FFI-safe slice.
#[repr(C)]
#[derive(Debug)]
pub struct CSlice {
    pub ptr: *const u8,
    pub len: usize,
}

/// Alias type for function accepting no arguments and returning a record batch.
type Func0Arg1Ret = fn() -> Result<RecordBatch>;
/// Alias type for function accepting a record batch and returning a record batch.
type Func1Arg1Ret = fn(input: &RecordBatch) -> Result<RecordBatch>;
/// Alias type for function accepting a record batch and returning an iterator of record batches.
type Func1ArgNRet = fn(input: &RecordBatch) -> Result<Box<dyn RecordBatchReader>>;

/// The internal safe wrapper for calling a [`Func0Arg1Ret`].
fn call_0_arg_1_ret_inner(f: Func0Arg1Ret) -> Result<Box<[u8]>> {
    let result = f()?;

    // write output batch to IPC buffer
    let mut buf = vec![];
    let mut writer = FileWriter::try_new(&mut buf, &result.schema())?;
    writer.write(&result)?;
    writer.finish()?;
    drop(writer);

    Ok(buf.into())
}

/// The internal safe wrapper for calling a [`Func1Arg1Ret`].
fn call_1_arg_1_ret_inner(f: Func1Arg1Ret, input_bytes: &[u8]) -> Result<Box<[u8]>> {
    // read input batch from IPC buffer
    let mut reader = FileReader::try_new(std::io::Cursor::new(input_bytes), None)?;
    let input_batch = reader
        .next()
        .ok_or_else(|| Error::IpcError("no record batch".into()))??;

    let output_batch = f(&input_batch)?;

    // write output batch to IPC buffer
    let mut buf = vec![];
    let mut writer = FileWriter::try_new(&mut buf, &output_batch.schema())?;
    writer.write(&output_batch)?;
    writer.finish()?;
    drop(writer);

    Ok(buf.into())
}

/// An opaque type for iterating over record batches.
pub struct RecordBatchIter {
    iter: Box<dyn RecordBatchReader>,
}

/// The internal safe wrapper for calling a [`Func1ArgNRet`].
fn call_1_arg_n_ret_inner(f: Func1ArgNRet, input_bytes: &[u8]) -> Result<Box<RecordBatchIter>> {
    // read input batch from IPC buffer
    let mut reader = FileReader::try_new(std::io::Cursor::new(input_bytes), None)?;
    let input_batch = reader
        .next()
        .ok_or_else(|| Error::IpcError("no record batch".into()))??;

    let iter = f(&input_batch)?;
    Ok(Box::new(RecordBatchIter { iter }))
}

/// Call a [`Func0Arg1Ret`] function from `extern "C"` code.
///
/// The output data is written to the buffer pointed to by `out_slice`.
/// The caller is responsible for deallocating the output buffer.
///
/// The return value is 0 on success, -1 on error.
/// If successful, the record batch is written to the buffer.
/// If failed, the error message is written to the buffer.
///
/// # Safety
///
/// `out_slice` must point to a valid buffer.
pub unsafe fn call_0_arg_1_ret(f: Func0Arg1Ret, out_slice: *mut CSlice) -> i32 {
    match call_0_arg_1_ret_inner(f) {
        Ok(data) => {
            out_slice.write(CSlice {
                ptr: data.as_ptr(),
                len: data.len(),
            });
            std::mem::forget(data);
            0
        }
        Err(err) => {
            let msg = err.to_string().into_boxed_str();
            out_slice.write(CSlice {
                ptr: msg.as_ptr(),
                len: msg.len(),
            });
            std::mem::forget(msg);
            -1
        }
    }
}

/// Call a [`Func1Arg1Ret`] function from `extern "C"` code.
///
/// The input record batch is read from the IPC buffer pointed to by `ptr` and `len`.
///
/// The output data is written to the buffer pointed to by `out_slice`.
/// The caller is responsible for deallocating the output buffer.
///
/// The return value is 0 on success, -1 on error.
/// If successful, the record batch is written to the buffer.
/// If failed, the error message is written to the buffer.
///
/// # Safety
///
/// `ptr`, `len`, `out_slice` must point to a valid buffer.
pub unsafe fn call_1_arg_1_ret(
    f: Func1Arg1Ret,
    ptr: *const u8,
    len: usize,
    out_slice: *mut CSlice,
) -> i32 {
    let input = std::slice::from_raw_parts(ptr, len);
    match call_1_arg_1_ret_inner(f, input) {
        Ok(data) => {
            out_slice.write(CSlice {
                ptr: data.as_ptr(),
                len: data.len(),
            });
            std::mem::forget(data);
            0
        }
        Err(err) => {
            let msg = err.to_string().into_boxed_str();
            out_slice.write(CSlice {
                ptr: msg.as_ptr(),
                len: msg.len(),
            });
            std::mem::forget(msg);
            -1
        }
    }
}

/// Call a [`Func1ArgNRet`] function from `extern "C"` code.
///
/// The input record batch is read from the IPC buffer pointed to by `ptr` and `len`.
///
/// The output iterator is written to `out_slice`.
///
/// The return value is 0 on success, -1 on error.
/// If successful, the record batch is written to the buffer.
/// If failed, the error message is written to the buffer.
///
/// # Safety
///
/// `ptr`, `len`, `out_slice` must point to a valid buffer.
pub unsafe fn call_1_arg_n_ret(
    f: Func1ArgNRet,
    ptr: *const u8,
    len: usize,
    out_slice: *mut CSlice,
) -> i32 {
    let input = std::slice::from_raw_parts(ptr, len);
    match call_1_arg_n_ret_inner(f, input) {
        Ok(iter) => {
            out_slice.write(CSlice {
                ptr: Box::into_raw(iter) as *const u8,
                len: std::mem::size_of::<RecordBatchIter>(),
            });
            0
        }
        Err(err) => {
            let msg = err.to_string().into_boxed_str();
            out_slice.write(CSlice {
                ptr: msg.as_ptr(),
                len: msg.len(),
            });
            std::mem::forget(msg);
            -1
        }
    }
}

/// Get the next record batch from the iterator.
///
/// The output record batch is written to the buffer pointed to by `out`.
/// The caller is responsible for deallocating the output buffer.
///
/// # Safety
///
/// `iter` and `out` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn record_batch_iterator_next(iter: *mut RecordBatchIter, out: *mut CSlice) {
    let iter = iter.as_mut().expect("null pointer");
    if let Some(Ok(batch)) = iter.iter.next() {
        let mut buf = vec![];
        let mut writer = FileWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let buf = buf.into_boxed_slice();

        out.write(CSlice {
            ptr: buf.as_ptr(),
            len: buf.len(),
        });
        std::mem::forget(buf);
    } else {
        // TODO: return error message
        out.write(CSlice {
            ptr: std::ptr::null(),
            len: 0,
        });
    }
}

/// Drop the iterator.
///
/// # Safety
///
/// `iter` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn record_batch_iterator_drop(iter: *mut RecordBatchIter) {
    drop(Box::from_raw(iter));
}
