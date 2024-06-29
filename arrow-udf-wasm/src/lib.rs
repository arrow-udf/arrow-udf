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

#![doc = include_str!("../README.md")]

use anyhow::{anyhow, bail, ensure, Context};
use arrow_array::RecordBatch;
use ram_file::{RamFile, RamFileRef};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Mutex;
use wasi_common::{sync::WasiCtxBuilder, WasiCtx};
use wasmtime::*;

#[cfg(feature = "build")]
pub mod build;
mod ram_file;

/// The WASM UDF runtime.
///
/// This runtime contains an instance pool and can be shared by multiple threads.
pub struct Runtime {
    module: Module,
    /// Configurations.
    config: Config,
    /// Function names.
    functions: HashSet<String>,
    /// User-defined types.
    types: HashMap<String, String>,
    /// Instance pool.
    instances: Mutex<Vec<Instance>>,
    /// ABI version. (major, minor)
    abi_version: (u8, u8),
}

/// Configurations.
#[derive(Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Memory size limit in bytes.
    pub memory_size_limit: Option<usize>,
    /// File size limit in bytes.
    pub file_size_limit: Option<usize>,
}

struct Instance {
    // extern "C" fn(len: usize, align: usize) -> *mut u8
    alloc: TypedFunc<(u32, u32), u32>,
    // extern "C" fn(ptr: *mut u8, len: usize, align: usize)
    dealloc: TypedFunc<(u32, u32, u32), ()>,
    // extern "C" fn(iter: *mut RecordBatchIter, out: *mut CSlice)
    record_batch_iterator_next: TypedFunc<(u32, u32), ()>,
    // extern "C" fn(iter: *mut RecordBatchIter)
    record_batch_iterator_drop: TypedFunc<u32, ()>,
    // extern "C" fn(ptr: *const u8, len: usize, out: *mut CSlice) -> i32
    functions: HashMap<String, TypedFunc<(u32, u32, u32), i32>>,
    memory: Memory,
    store: Store<(WasiCtx, StoreLimits)>,
    stdout: RamFileRef,
    stderr: RamFileRef,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("config", &self.config)
            .field("functions", &self.functions)
            .field("types", &self.types)
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
        static ENGINE: once_cell::sync::Lazy<Engine> = once_cell::sync::Lazy::new(Engine::default);
        Self::with_config_engine(binary, config, &ENGINE)
    }

    /// Create a new UDF runtime from a WASM binary with a customized engine.
    fn with_config_engine(binary: &[u8], config: Config, engine: &Engine) -> Result<Self> {
        let module = Module::from_binary(engine, binary).context("failed to load wasm binary")?;

        // check abi version
        let version = module
            .exports()
            .find_map(|e| e.name().strip_prefix("ARROWUDF_VERSION_"))
            .context("version not found")?;
        let (major, minor) = version.split_once('_').context("invalid version")?;
        let (major, minor) = (major.parse::<u8>()?, minor.parse::<u8>()?);
        ensure!(major <= 3, "unsupported abi version: {major}.{minor}");

        let mut functions = HashSet::new();
        let mut types = HashMap::new();
        for export in module.exports() {
            if let Some(encoded) = export.name().strip_prefix("arrowudf_") {
                let name = base64_decode(encoded).context("invalid symbol")?;
                functions.insert(name);
            } else if let Some(encoded) = export.name().strip_prefix("arrowudt_") {
                let meta = base64_decode(encoded).context("invalid symbol")?;
                let (name, fields) = meta.split_once('=').context("invalid type string")?;
                types.insert(name.to_string(), fields.to_string());
            }
        }

        Ok(Self {
            module,
            config,
            functions,
            types,
            instances: Mutex::new(vec![]),
            abi_version: (major, minor),
        })
    }

    /// Return available functions.
    pub fn functions(&self) -> impl Iterator<Item = &str> {
        self.functions.iter().map(|s| s.as_str())
    }

    /// Return available types.
    pub fn types(&self) -> impl Iterator<Item = (&str, &str)> {
        self.types.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Return the ABI version.
    pub fn abi_version(&self) -> (u8, u8) {
        self.abi_version
    }

    /// Given a function signature that inlines struct types, find the function name.
    ///
    /// # Example
    ///
    /// ```text
    /// types = { "KeyValue": "key:string,value:string" }
    /// input = "keyvalue(string, string) -> struct<key:string,value:string>"
    /// output = "keyvalue(string, string) -> struct KeyValue"
    /// ```
    pub fn find_function_by_inlined_signature(&self, s: &str) -> Option<&str> {
        self.functions
            .iter()
            .find(|f| self.inline_types(f) == s)
            .map(|f| f.as_str())
    }

    /// Inline types in function signature.
    ///
    /// # Example
    ///
    /// ```text
    /// types = { "KeyValue": "key:string,value:string" }
    /// input = "keyvalue(string, string) -> struct KeyValue"
    /// output = "keyvalue(string, string) -> struct<key:string,value:string>"
    /// ```
    fn inline_types(&self, s: &str) -> String {
        let mut inlined = s.to_string();
        loop {
            let replaced = inlined.clone();
            for (k, v) in self.types.iter() {
                inlined = inlined.replace(&format!("struct {k}"), &format!("struct<{v}>"));
            }
            if replaced == inlined {
                return inlined;
            }
        }
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
        let output = instance.call_scalar_function(name, input);

        // put the instance back to the pool
        if output.is_ok() {
            self.instances.lock().unwrap().push(instance);
        }

        output
    }

    /// Call a table function.
    pub fn call_table_function<'a>(
        &'a self,
        name: &'a str,
        input: &'a RecordBatch,
    ) -> Result<impl Iterator<Item = Result<RecordBatch>> + 'a> {
        use genawaiter::{sync::gen, yield_};
        if !self.functions.contains(name) {
            bail!("function not found: {name}");
        }

        // get an instance from the pool, or create a new one if the pool is empty
        let mut instance = if let Some(instance) = self.instances.lock().unwrap().pop() {
            instance
        } else {
            Instance::new(self)?
        };

        Ok(gen!({
            // call the function
            let iter = match instance.call_table_function(name, input) {
                Ok(iter) => iter,
                Err(e) => {
                    yield_!(Err(e));
                    return;
                }
            };
            for output in iter {
                yield_!(output);
            }
            // put the instance back to the pool
            // FIXME: if the iterator is not consumed, the instance will be dropped
            self.instances.lock().unwrap().push(instance);
        })
        .into_iter())
    }
}

impl Instance {
    /// Create a new instance.
    fn new(rt: &Runtime) -> Result<Self> {
        let module = &rt.module;
        let engine = module.engine();
        let mut linker = Linker::new(engine);
        wasi_common::sync::add_to_linker(&mut linker, |(wasi, _)| wasi)?;

        // Create a WASI context and put it in a Store; all instances in the store
        // share this context. `WasiCtxBuilder` provides a number of ways to
        // configure what the target program will have access to.
        let file_size_limit = rt.config.file_size_limit.unwrap_or(1024);
        let stdout = RamFileRef::new(RamFile::with_size_limit(file_size_limit));
        let stderr = RamFileRef::new(RamFile::with_size_limit(file_size_limit));
        let wasi = WasiCtxBuilder::new()
            .stdout(Box::new(stdout.clone()))
            .stderr(Box::new(stderr.clone()))
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
        let record_batch_iterator_next =
            instance.get_typed_func(&mut store, "record_batch_iterator_next")?;
        let record_batch_iterator_drop =
            instance.get_typed_func(&mut store, "record_batch_iterator_drop")?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("no memory")?;

        Ok(Instance {
            alloc,
            dealloc,
            record_batch_iterator_next,
            record_batch_iterator_drop,
            memory,
            store,
            functions,
            stdout,
            stderr,
        })
    }

    /// Call a scalar function.
    fn call_scalar_function(&mut self, name: &str, input: &RecordBatch) -> Result<RecordBatch> {
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
        let alloc_len = u32::try_from(input.len() + 4 * 2).context("input too large")?;
        let alloc_ptr = self.alloc.call(&mut self.store, (alloc_len, 4))?;
        ensure!(alloc_ptr != 0, "failed to allocate for input");
        let in_ptr = alloc_ptr + 4 * 2;

        // write input to memory
        self.memory
            .write(&mut self.store, in_ptr as usize, &input)?;

        // call the function
        let result = func.call(&mut self.store, (in_ptr, input.len() as u32, alloc_ptr));
        let errno = self.append_stdio(result)?;

        // get return values
        let out_ptr = self.read_u32(alloc_ptr)?;
        let out_len = self.read_u32(alloc_ptr + 4)?;

        // read output from memory
        let out_bytes = self
            .memory
            .data(&self.store)
            .get(out_ptr as usize..(out_ptr + out_len) as usize)
            .context("output slice out of bounds")?;
        let result = match errno {
            0 => Ok(decode_record_batch(out_bytes)?),
            _ => Err(anyhow!("{}", std::str::from_utf8(out_bytes)?)),
        };

        // deallocate memory
        self.dealloc
            .call(&mut self.store, (alloc_ptr, alloc_len, 4))?;
        self.dealloc.call(&mut self.store, (out_ptr, out_len, 1))?;

        result
    }

    /// Call a table function.
    fn call_table_function<'a>(
        &'a mut self,
        name: &str,
        input: &RecordBatch,
    ) -> Result<impl Iterator<Item = Result<RecordBatch>> + 'a> {
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
        let alloc_len = u32::try_from(input.len() + 4 * 2).context("input too large")?;
        let alloc_ptr = self.alloc.call(&mut self.store, (alloc_len, 4))?;
        ensure!(alloc_ptr != 0, "failed to allocate for input");
        let in_ptr = alloc_ptr + 4 * 2;

        // write input to memory
        self.memory
            .write(&mut self.store, in_ptr as usize, &input)?;

        // call the function
        let result = func.call(&mut self.store, (in_ptr, input.len() as u32, alloc_ptr));
        let errno = self.append_stdio(result)?;

        // get return values
        let out_ptr = self.read_u32(alloc_ptr)?;
        let out_len = self.read_u32(alloc_ptr + 4)?;

        // read output from memory
        let out_bytes = self
            .memory
            .data(&self.store)
            .get(out_ptr as usize..(out_ptr + out_len) as usize)
            .context("output slice out of bounds")?;

        let ptr = match errno {
            0 => out_ptr,
            _ => {
                let err = anyhow!("{}", std::str::from_utf8(out_bytes)?);
                // deallocate memory
                self.dealloc
                    .call(&mut self.store, (alloc_ptr, alloc_len, 4))?;
                self.dealloc.call(&mut self.store, (out_ptr, out_len, 1))?;

                return Err(err);
            }
        };

        struct RecordBatchIter<'a> {
            instance: &'a mut Instance,
            ptr: u32,
            alloc_ptr: u32,
            alloc_len: u32,
        }

        impl RecordBatchIter<'_> {
            /// Get the next record batch.
            fn next(&mut self) -> Result<Option<RecordBatch>> {
                self.instance
                    .record_batch_iterator_next
                    .call(&mut self.instance.store, (self.ptr, self.alloc_ptr))?;
                // get return values
                let out_ptr = self.instance.read_u32(self.alloc_ptr)?;
                let out_len = self.instance.read_u32(self.alloc_ptr + 4)?;

                if out_ptr == 0 {
                    // end of iteration
                    return Ok(None);
                }

                // read output from memory
                let out_bytes = self
                    .instance
                    .memory
                    .data(&self.instance.store)
                    .get(out_ptr as usize..(out_ptr + out_len) as usize)
                    .context("output slice out of bounds")?;
                let batch = decode_record_batch(out_bytes)?;

                // dealloc output
                self.instance
                    .dealloc
                    .call(&mut self.instance.store, (out_ptr, out_len, 1))?;

                Ok(Some(batch))
            }
        }

        impl Iterator for RecordBatchIter<'_> {
            type Item = Result<RecordBatch>;

            fn next(&mut self) -> Option<Self::Item> {
                let result = self.next();
                self.instance.append_stdio(result).transpose()
            }
        }

        impl Drop for RecordBatchIter<'_> {
            fn drop(&mut self) {
                _ = self.instance.dealloc.call(
                    &mut self.instance.store,
                    (self.alloc_ptr, self.alloc_len, 4),
                );
                _ = self
                    .instance
                    .record_batch_iterator_drop
                    .call(&mut self.instance.store, self.ptr);
            }
        }

        Ok(RecordBatchIter {
            instance: self,
            ptr,
            alloc_ptr,
            alloc_len,
        })
    }

    /// Read a `u32` from memory.
    fn read_u32(&mut self, ptr: u32) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.memory.data(&self.store)[ptr as usize..(ptr + 4) as usize]
                .try_into()
                .unwrap(),
        ))
    }

    /// Take stdout and stderr, append to the error context.
    fn append_stdio<T>(&self, result: Result<T>) -> Result<T> {
        let stdout = self.stdout.take();
        let stderr = self.stderr.take();
        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(e.context(format!(
                "--- stdout\n{}\n--- stderr\n{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr),
            ))),
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
