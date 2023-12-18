use anyhow::{ensure, Context};
use arrow_array::RecordBatch;
use std::collections::HashMap;
use std::fmt::Debug;
use wasmtime::*;
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};

pub struct Runtime {
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    functions: HashMap<String, TypedFunc<(u32, u32), u64>>,
    memory: Memory,
    store: Store<WasiCtx>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("functions", &self.functions.keys())
            .field("memory_size", &self.memory.data_size(&self.store))
            .finish()
    }
}

impl Runtime {
    /// Create a new UDF runtime from a WASM binary.
    pub fn new(binary: &[u8]) -> Result<Self> {
        // use a global engine by default
        lazy_static::lazy_static! {
            static ref ENGINE: Engine = Engine::default();
        }
        Self::new_with_engine(binary, &ENGINE)
    }

    /// Create a new UDF runtime from a WASM binary with a customized engine.
    pub fn new_with_engine(binary: &[u8], engine: &Engine) -> Result<Self> {
        let module = Module::from_binary(engine, binary).context("failed to load wasm binary")?;

        let mut linker = Linker::new(engine);
        wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;

        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio() // FIXME: remove this
            .build();
        let mut store = Store::new(engine, wasi);

        let instance = linker.instantiate(&mut store, &module)?;
        let mut functions = HashMap::new();
        for export in module.exports() {
            let Some(encoded) = export.name().strip_prefix("arrowudf_") else {
                continue;
            };
            let name = base64_decode(encoded).context("invalid symbol")?;
            let func = instance.get_typed_func::<(u32, u32), u64>(&mut store, export.name())?;
            functions.insert(name, func);
        }
        let alloc = instance.get_typed_func(&mut store, "alloc")?;
        let dealloc = instance.get_typed_func(&mut store, "dealloc")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("no memory")?;

        Ok(Runtime {
            alloc,
            dealloc,
            memory,
            store,
            functions,
        })
    }

    /// Call a function.
    pub fn call(&mut self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        // TODO: optimize data transfer
        // currently there are 3 copies in input path:
        //      host record batch -> host encoding -> wasm memory -> wasm record batch
        // and 2 copies in output path:
        //      wasm record batch -> wasm memory -> host record batch

        // get function
        let func = self
            .functions
            .get(name)
            .with_context(|| format!("function not found: {name}"))?;

        // encode input batch
        let input = encode_record_batch(input)?;
        let input_len = u32::try_from(input.len()).context("input too large")?;

        // allocate memory
        let input_ptr = self.alloc.call(&mut self.store, input_len)?;
        ensure!(input_ptr != 0, "failed to alloc");

        // write input to memory
        self.memory
            .write(&mut self.store, input_ptr as usize, &input)?;

        // call function
        let output_slice = func.call(&mut self.store, (input_ptr, input_len))?;
        let output_ptr = (output_slice >> 32) as u32;
        let output_len = output_slice as u32;
        ensure!(output_ptr != 0, "returned error");
        let output_range = output_ptr as usize..(output_ptr + output_len) as usize;
        let output_bytes = self
            .memory
            .data(&self.store)
            .get(output_range)
            .context("return out of bounds")?;
        let output = decode_record_batch(output_bytes)?;

        // deallocate memory
        // FIXME: RAII for memory allocation
        self.dealloc.call(&mut self.store, (input_ptr, input_len))?;
        self.dealloc
            .call(&mut self.store, (output_ptr, output_len))?;

        Ok(output)
    }
}

/// Decode a string from symbol name using customized base64.
fn base64_decode(input: &str) -> Result<String> {
    use base64::{
        alphabet::Alphabet,
        engine::{general_purpose::NO_PAD, GeneralPurpose},
        Engine,
    };
    // standard base64 uses '+' and '/', which is not a valid symbol name.
    // we use '$' and '_' instead.
    let alphabet =
        Alphabet::new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789$_").unwrap();
    let engine = GeneralPurpose::new(&alphabet, NO_PAD);
    let bytes = engine.decode(input)?;
    String::from_utf8(bytes).context("invalid utf8")
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = vec![];
    let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

fn decode_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batch = reader.next().unwrap()?;
    Ok(batch)
}
