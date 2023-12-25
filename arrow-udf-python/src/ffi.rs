//! FFI interfaces.
//!
//! # Safety
//!
//! This module is not thread safe. It is intended to be used in a single threaded environment.

use anyhow::Result;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::DataType;
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

#[no_mangle]
unsafe extern "C" fn init(
    code: *const i8,
    function_name: *const i8,
    return_type: *const i8,
) -> i32 {
    let code = CStr::from_ptr(code).to_str().expect("invalid utf8");
    let function_name = CStr::from_ptr(function_name)
        .to_str()
        .expect("invalid utf8");
    let return_type_str = CStr::from_ptr(return_type).to_str().expect("invalid utf8");
    let return_type = parse_data_type(return_type_str);

    let runtime =
        Runtime::new(code, function_name, return_type).expect("failed to initialize runtime");
    RUNTIME = Some(runtime);
    0
}

#[no_mangle]
unsafe extern "C" fn call(ptr: *const u8, len: usize) -> u64 {
    let input = std::slice::from_raw_parts(ptr, len);
    match call_internal(input) {
        Ok(data) => {
            let ptr = data.as_ptr();
            let len = data.len();
            std::mem::forget(data);
            ((ptr as u64) << 32) | (len as u64)
        }
        Err(_) => 0,
    }
}

static mut RUNTIME: Option<Runtime> = None;

fn call_internal(input: &[u8]) -> Result<Box<[u8]>> {
    let mut reader = FileReader::try_new(std::io::Cursor::new(input), None)?;
    let input_batch = reader.next().unwrap()?;

    let runtime = unsafe { RUNTIME.as_ref().unwrap() };
    let output_batch = runtime.call(&input_batch)?;

    let mut buf = vec![];
    // Write data to IPC buffer
    let mut writer = FileWriter::try_new(&mut buf, &output_batch.schema())?;
    writer.write(&output_batch)?;
    writer.finish()?;
    drop(writer);

    Ok(buf.into())
}

fn parse_data_type(s: &str) -> DataType {
    match s {
        "null" => DataType::Null,
        "boolean" => DataType::Boolean,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "varchar" => DataType::Utf8,
        "bytea" => DataType::Binary,
        _ => panic!("unsupported data type: {}", s),
    }
}
