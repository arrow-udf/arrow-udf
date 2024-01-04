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

use crate::{Error, ScalarFunction, TableFunction};
use arrow_array::RecordBatch;
use arrow_ipc::{reader::FileReader, writer::FileWriter};

/// A symbol indicating the ABI version.
#[no_mangle]
#[used]
pub static ARROWUDF_VERSION_1: () = ();

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

/// A wrapper for calling scalar functions from C.
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
pub unsafe fn scalar_wrapper(
    function: ScalarFunction,
    ptr: *const u8,
    len: usize,
    out_slice: *mut CSlice,
) -> i32 {
    let input = std::slice::from_raw_parts(ptr, len);
    match call_scalar(function, input) {
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

/// The internal wrapper that returns a Result.
fn call_scalar(function: ScalarFunction, input_bytes: &[u8]) -> Result<Box<[u8]>, Error> {
    let mut reader = FileReader::try_new(std::io::Cursor::new(input_bytes), None)?;
    let input_batch = reader
        .next()
        .ok_or_else(|| Error::IpcError("no record batch".into()))??;

    let output_batch = function(&input_batch)?;

    // Write data to IPC buffer
    let mut buf = vec![];
    let mut writer = FileWriter::try_new(&mut buf, &output_batch.schema())?;
    writer.write(&output_batch)?;
    writer.finish()?;
    drop(writer);

    Ok(buf.into())
}

/// An opaque type for iterating over record batches.
pub struct RecordBatchIter {
    /// The input record batch is borrowed by `iter`. Its lifetime must be longer than `iter`.
    _input: Box<RecordBatch>,
    /// This iterator borrows `input`.
    iter: Box<dyn Iterator<Item = RecordBatch>>,
}

/// A wrapper for calling table functions from C.
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
pub unsafe fn table_wrapper(
    function: TableFunction,
    ptr: *const u8,
    len: usize,
    out_slice: *mut CSlice,
) -> i32 {
    let input = std::slice::from_raw_parts(ptr, len);
    match call_table(function, input) {
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

fn call_table(function: TableFunction, input_bytes: &[u8]) -> Result<Box<RecordBatchIter>, Error> {
    let mut reader = FileReader::try_new(std::io::Cursor::new(input_bytes), None)?;
    let input_batch = reader
        .next()
        .ok_or_else(|| Error::IpcError("no record batch".into()))??;

    let input = Box::new(input_batch);
    // SAFETY: The lifetime of `input` is longer than `iter`.
    let input_ref = unsafe { std::mem::transmute(input.as_ref()) };
    let iter = function(input_ref)?;
    Ok(Box::new(RecordBatchIter {
        _input: input,
        iter,
    }))
}

/// Get the next record batch from the iterator.
///
/// The output record batch is written to the buffer pointed to by `out`.
/// The caller is responsible for deallocating the output buffer.
#[no_mangle]
pub unsafe extern "C" fn record_batch_iterator_next(iter: *mut RecordBatchIter, out: *mut CSlice) {
    let iter = iter.as_mut().expect("null pointer");
    if let Some(batch) = iter.iter.next() {
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
        out.write(CSlice {
            ptr: std::ptr::null(),
            len: 0,
        });
    }
}

/// Drop the iterator.
#[no_mangle]
pub unsafe extern "C" fn record_batch_iterator_drop(iter: *mut RecordBatchIter) {
    drop(Box::from_raw(iter));
}
