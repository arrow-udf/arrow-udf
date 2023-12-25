use anyhow::{bail, ensure, Context};
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Mutex;
use std::{collections::HashMap, path::PathBuf};
use wasmtime::*;
use wasmtime_wasi::{ambient_authority, sync::WasiCtxBuilder, Dir, WasiCtx};

/// The Python UDF WASM runtime.
///
/// This runtime contains an instance pool and can be shared by multiple threads.
pub struct Runtime {
    path: PathBuf,
    module: Module,
    /// Configurations.
    config: Config,
    /// Function name -> (code, return type).
    functions: HashMap<String, (String, DataType)>,
    /// Instance pool.
    instances: Mutex<Vec<Instance>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Memory size limit in bytes.
    pub memory_size_limit: Option<usize>,
}

struct Instance {
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    call: TypedFunc<(u32, u32, u32), u64>,
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
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_config(path, Config::default())
    }

    /// Create a new UDF runtime from a WASM binary with configuration.
    pub fn with_config(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        // use a global engine by default
        lazy_static::lazy_static! {
            static ref ENGINE: Engine = Engine::default();
        }
        Self::with_config_engine(path, config, &ENGINE)
    }

    /// Create a new UDF runtime from a WASM binary with a customized engine.
    fn with_config_engine(path: impl AsRef<Path>, config: Config, engine: &Engine) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let binary =
            std::fs::read(&path.join("bin/python.wasm")).context("failed to read wasm binary")?;
        let module = Module::from_binary(engine, &binary).context("failed to load wasm binary")?;
        Ok(Self {
            path,
            module,
            config,
            functions: HashMap::new(),
            instances: Mutex::new(vec![]),
        })
    }

    /// Add a function.
    pub fn add_function(&mut self, name: &str, return_type: DataType, code: &str) {
        self.functions
            .insert(name.to_string(), (code.to_string(), return_type));
    }

    /// Call a function.
    pub fn call(&self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        if !self.functions.contains_key(name) {
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
        let wasi = WasiCtxBuilder::new()
            .inherit_stdio()
            .preopened_dir(
                Dir::open_ambient_dir(&rt.path.join("usr"), ambient_authority())?,
                "/usr",
            )?
            .build();
        let limits = {
            let mut builder = StoreLimitsBuilder::new();
            if let Some(limit) = rt.config.memory_size_limit {
                builder = builder.memory_size(limit);
            }
            builder.build()
        };
        let mut store = Store::new(engine, (wasi, limits));
        store.limiter(|(_, limiter)| limiter);

        let instance = linker.instantiate(&mut store, &module)?;
        let alloc = instance.get_typed_func(&mut store, "alloc")?;
        let dealloc = instance.get_typed_func(&mut store, "dealloc")?;
        let add_function =
            instance.get_typed_func::<(u32, u32, u32, u32), u32>(&mut store, "add_function")?;
        let call = instance.get_typed_func(&mut store, "call")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("no memory")?;

        // add functions
        for (name, (code, return_type)) in &rt.functions {
            // encode return type
            let return_type_bytes = {
                let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "return",
                    return_type.clone(),
                    true,
                )]);
                let mut buf = vec![];
                let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut buf, &schema)?;
                writer.finish()?;
                drop(writer);
                buf
            };

            // allocate memory for arguments
            let name_ptr = alloc.call(&mut store, name.len() as u32 + 1)?;
            let code_ptr = alloc.call(&mut store, code.len() as u32 + 1)?;
            let return_type_len = return_type_bytes.len() as u32;
            let return_type_ptr = alloc.call(&mut store, return_type_len)?;
            ensure!(name_ptr != 0, "failed to alloc");
            ensure!(code_ptr != 0, "failed to alloc");
            ensure!(return_type_ptr != 0, "failed to alloc");

            // write arguments to memory
            memory.write_cstr(&mut store, name_ptr as usize, name)?;
            memory.write_cstr(&mut store, code_ptr as usize, code)?;
            memory.write(&mut store, return_type_ptr as usize, &return_type_bytes)?;

            // call `add_function`
            add_function.call(
                &mut store,
                (name_ptr, code_ptr, return_type_ptr, return_type_len),
            )?;

            // free memory
            dealloc.call(&mut store, (name_ptr, name.len() as u32 + 1))?;
            dealloc.call(&mut store, (code_ptr, code.len() as u32 + 1))?;
            dealloc.call(&mut store, (return_type_ptr, return_type_len))?;
        }

        Ok(Instance {
            alloc,
            dealloc,
            call,
            memory,
            store,
        })
    }

    /// Call a function.
    pub fn call(&mut self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
        // TODO: optimize data transfer
        // currently there are 3 copies in input path:
        //      host record batch -> host encoding -> wasm memory -> wasm record batch
        // and 2 copies in output path:
        //      wasm record batch -> wasm memory -> host record batch

        // encode input batch
        let input = encode_record_batch(input)?;
        let input_len = u32::try_from(input.len()).context("input too large")?;

        // allocate memory
        let name_ptr = self.alloc.call(&mut self.store, name.len() as u32 + 1)?;
        let input_ptr = self.alloc.call(&mut self.store, input_len)?;
        ensure!(name_ptr != 0, "failed to alloc");
        ensure!(input_ptr != 0, "failed to alloc");

        // write input to memory
        self.memory
            .write_cstr(&mut self.store, name_ptr as usize, name)?;
        self.memory
            .write(&mut self.store, input_ptr as usize, &input)?;

        // call function
        let output_slice = self
            .call
            .call(&mut self.store, (name_ptr, input_ptr, input_len))?;
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
        self.dealloc
            .call(&mut self.store, (name_ptr, name.len() as u32 + 1))?;
        self.dealloc.call(&mut self.store, (input_ptr, input_len))?;
        self.dealloc
            .call(&mut self.store, (output_ptr, output_len))?;

        Ok(output)
    }
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

trait MemoryExt {
    fn write_cstr<T>(&self, store: &mut Store<T>, offset: usize, s: &str) -> Result<()>;
}

impl MemoryExt for Memory {
    fn write_cstr<T>(&self, store: &mut Store<T>, offset: usize, s: &str) -> Result<()> {
        self.write(&mut *store, offset, s.as_bytes())?;
        self.write(&mut *store, offset + s.len(), &[0])?;
        Ok(())
    }
}
