use anyhow::{anyhow, bail, ensure, Context};
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Mutex;
use wasmtime::*;
use wasmtime_wasi::{sync::WasiCtxBuilder, WasiCtx};

/// The WASM UDF runtime.
///
/// This runtime contains an instance pool and can be shared by multiple threads.
pub struct Runtime {
    module: Module,
    /// Configurations.
    config: Config,
    /// Function names.
    functions: HashSet<String>,
    /// Instance pool.
    instances: Mutex<Vec<Instance>>,
}

/// Configurations.
#[derive(Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Memory size limit in bytes.
    pub memory_size_limit: Option<usize>,
}

struct Instance {
    // extern "C" fn(len: usize, align: usize) -> *mut u8
    alloc: TypedFunc<(u32, u32), u32>,
    // extern "C" fn(ptr: *mut u8, len: usize, align: usize)
    dealloc: TypedFunc<(u32, u32, u32), ()>,
    // extern "C" fn(out: *mut FFIResult, ptr: *const u8, len: usize)
    functions: HashMap<String, TypedFunc<(u32, u32, u32), ()>>,
    memory: Memory,
    store: Store<(WasiCtx, StoreLimits)>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("config", &self.config)
            .field("functions", &self.functions)
            .field("instances", &self.instances.lock().unwrap().len())
            .finish()
    }
}

impl Runtime {
    /// Create a new UDF runtime from a WASM binary.
    pub fn new(binary: &[u8]) -> Result<Self> {
        Self::with_config(binary, Config::default())
    }

    /// Create a new UDF runtime from a WASM binary with configuration.
    pub fn with_config(binary: &[u8], config: Config) -> Result<Self> {
        // use a global engine by default
        lazy_static::lazy_static! {
            static ref ENGINE: Engine = Engine::default();
        }
        Self::with_config_engine(binary, config, &ENGINE)
    }

    /// Create a new UDF runtime from a WASM binary with a customized engine.
    fn with_config_engine(binary: &[u8], config: Config, engine: &Engine) -> Result<Self> {
        let module = Module::from_binary(engine, binary).context("failed to load wasm binary")?;

        let mut functions = HashSet::new();
        for export in module.exports() {
            let Some(encoded) = export.name().strip_prefix("arrowudf_") else {
                continue;
            };
            let name = base64_decode(encoded).context("invalid symbol")?;
            functions.insert(name);
        }

        Ok(Self {
            module,
            config,
            functions,
            instances: Mutex::new(vec![]),
        })
    }

    /// Return available functions.
    pub fn functions(&self) -> impl Iterator<Item = &str> {
        self.functions.iter().map(|s| s.as_str())
    }

    /// Call a function.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        if !self.functions.contains(name) {
            bail!("function not found: {name}");
        }

        // get an instance from the pool, or create a new one if the pool is empty
        let mut instance = if let Some(instance) = self.instances.lock().unwrap().pop() {
            instance
        } else {
            Instance::new(self)?
        };

        // call the function
        let output = instance.call(name, input);

        // put the instance back to the pool
        if output.is_ok() {
            self.instances.lock().unwrap().push(instance);
        }

        output
    }
}

impl Instance {
    /// Create a new instance.
    fn new(rt: &Runtime) -> Result<Self> {
        let module = &rt.module;
        let engine = module.engine();
        let mut linker = Linker::new(engine);
        wasmtime_wasi::add_to_linker(&mut linker, |(wasi, _)| wasi)?;

        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let wasi = WasiCtxBuilder::new().build();
        let limits = {
            let mut builder = StoreLimitsBuilder::new();
            if let Some(limit) = rt.config.memory_size_limit {
                builder = builder.memory_size(limit);
            }
            builder.build()
        };
        let mut store = Store::new(engine, (wasi, limits));
        store.limiter(|(_, limiter)| limiter);

        let instance = linker.instantiate(&mut store, module)?;
        let mut functions = HashMap::new();
        for export in module.exports() {
            let Some(encoded) = export.name().strip_prefix("arrowudf_") else {
                continue;
            };
            let name = base64_decode(encoded).context("invalid symbol")?;
            let func = instance.get_typed_func(&mut store, export.name())?;
            functions.insert(name, func);
        }
        let alloc = instance.get_typed_func(&mut store, "alloc")?;
        let dealloc = instance.get_typed_func(&mut store, "dealloc")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("no memory")?;

        Ok(Instance {
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

        // allocate memory for input buffer and output struct
        let alloc_len = u32::try_from(input.len() + 4 * 4).context("input too large")?;
        let alloc_ptr = self.alloc.call(&mut self.store, (alloc_len, 4))?;
        ensure!(alloc_ptr != 0, "failed to allocate for input");
        let in_ptr = alloc_ptr + 4 * 4;

        // write input to memory
        self.memory
            .write(&mut self.store, in_ptr as usize, &input)?;

        // call the function
        // The ABI of this function is:
        //   extern "C" fn(ptr: *const u8, len: usize) -> FFIResult
        // where FFIResult is defined as:
        //   #[repr(C)]
        //   struct FFIResult {
        //       out_ptr: *const u8,
        //       out_len: usize,
        //       err_ptr: *const u8,
        //       err_len: usize,
        //   }
        // According to the calling convention, the returned struct is written to the address
        // pointed to by the hidden first argument. So the actual function is:
        //   extern "C" fn(out: *mut FFIResult, ptr: *const u8, len: usize)
        func.call(&mut self.store, (alloc_ptr, in_ptr, input.len() as u32))?;

        // get return values
        let read_u32 = |ptr| {
            u32::from_le_bytes(
                self.memory.data(&self.store)[ptr as usize..(ptr + 4) as usize]
                    .try_into()
                    .unwrap(),
            )
        };
        let out_ptr = read_u32(alloc_ptr);
        let out_len = read_u32(alloc_ptr + 4);
        let err_ptr = read_u32(alloc_ptr + 8);
        let err_len = read_u32(alloc_ptr + 12);

        // read output and error from memory
        let output = if out_ptr != 0 {
            let bytes = self
                .memory
                .data(&self.store)
                .get(out_ptr as usize..(out_ptr + out_len) as usize)
                .context("output slice out of bounds")?;
            Some(decode_record_batch(bytes)?)
        } else {
            None
        };
        let error = if err_ptr != 0 {
            let bytes = self
                .memory
                .data(&self.store)
                .get(err_ptr as usize..(err_ptr + err_len) as usize)
                .context("error slice out of bounds")?;
            Some(anyhow!("{}", std::str::from_utf8(bytes)?))
        } else {
            None
        };

        // deallocate memory
        self.dealloc
            .call(&mut self.store, (alloc_ptr, alloc_len, 4))?;
        if out_ptr != 0 {
            self.dealloc.call(&mut self.store, (out_ptr, out_len, 1))?;
        }
        if err_ptr != 0 {
            self.dealloc.call(&mut self.store, (err_ptr, err_len, 1))?;
        }

        // TODO: support partial output
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(output.unwrap())
        }
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
