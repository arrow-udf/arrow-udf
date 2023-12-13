use arrow_array::{ArrayRef, RecordBatch};
use wasmtime::*;
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};

pub struct Runtime {
    engine: Engine,
    module: Module,
    instance: Instance,
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    memory: Memory,
    store: Store<WasiCtx>,
    symbols: Vec<(String, String)>, // (encoded, decoded)
}

impl Runtime {
    pub fn new(binary: &[u8]) -> Result<Self> {
        let engine = Engine::default();
        let module = Module::from_binary(&engine, binary)?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;

        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio() // FIXME: remove this
            .build();
        let mut store = Store::new(&engine, wasi);

        let symbols = module
            .exports()
            .filter(|export| export.name().starts_with("arrowudf_"))
            .map(|export| {
                (
                    export.name().to_owned(),
                    base64_decode(export.name().strip_prefix("arrowudf_").unwrap()),
                )
            })
            .collect::<Vec<_>>();
        let instance = linker.instantiate(&mut store, &module)?;
        let alloc = instance.get_typed_func(&mut store, "alloc")?;
        let dealloc = instance.get_typed_func(&mut store, "dealloc")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .expect("no memory");

        Ok(Runtime {
            engine,
            module,
            instance,
            alloc,
            dealloc,
            memory,
            store,
            symbols,
        })
    }

    pub fn call(&mut self, name: &str, input: &RecordBatch) -> Result<ArrayRef> {
        // TODO: optimize data transfer
        // currently there are 3 copies in input path:
        //      host record batch -> host encoding -> wasm memory -> wasm record batch
        // and 2 copies in output path:
        //      wasm record batch -> wasm memory -> host record batch

        // get function
        let func = self
            .instance
            .get_typed_func::<(u32, u32), u64>(&mut self.store, name)?;

        // encode input batch
        let input = encode_record_batch(input)?;
        let input_len = u32::try_from(input.len()).expect("input too large");

        // allocate memory
        let input_ptr = self.alloc.call(&mut self.store, input_len)?;
        assert!(input_ptr != 0, "failed to alloc");

        // write input to memory
        self.memory
            .write(&mut self.store, input_ptr as usize, &input)?;

        // call function
        let output_slice = func.call(&mut self.store, (input_ptr, input_len))?;
        let output_ptr = (output_slice >> 32) as u32;
        let output_len = output_slice as u32;
        assert!(output_ptr != 0, "returned error");
        let output_range = output_ptr as usize..(output_ptr + output_len) as usize;
        let output_bytes = self
            .memory
            .data(&self.store)
            .get(output_range)
            .expect("return out of bounds");
        let output = decode_record_batch(output_bytes)?;

        // deallocate memory
        // FIXME: RAII for memory allocation
        self.dealloc.call(&mut self.store, (input_ptr, input_len))?;
        self.dealloc
            .call(&mut self.store, (output_ptr, output_len))?;

        Ok(output.column(0).clone())
    }

    /// List all functions. The first element of each tuple is the symbol name, and the second is
    /// the function signature.
    pub fn functions(&self) -> &[(String, String)] {
        &self.symbols
    }
}

/// Decode a string from symbol name using customized base64.
fn base64_decode(input: &str) -> String {
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
    String::from_utf8(engine.decode(input).unwrap()).unwrap()
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = vec![];
    let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

fn decode_record_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = arrow_ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None)?;
    let batch = reader.next().unwrap()?;
    Ok(batch)
}
