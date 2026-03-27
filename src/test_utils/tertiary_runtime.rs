//! Native tertiary indexer runtime for cargo test.
//!
//! Loads a compiled tertiary WASM module via wasmtime and provides
//! the same host function ABI as qubitcoin's TertiaryRuntime:
//! - Standard metashrew ABI: `__host_len`, `__load_input`, `__get_len`,
//!   `__get`, `__flush`, `__log`, `abort`
//! - Tertiary extensions: `__secondary_get_len`, `__secondary_get`
//!
//! This enables `cargo test` to exercise tertiary indexer logic against
//! real alkanes state from TestMetashrewRuntime.

use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use wasmtime::*;

/// Protobuf message for KeyValueFlush (same format as metashrew/qubitcoin).
mod proto {
    include!(concat!(env!("OUT_DIR"), "/metashrew.rs"));
}

/// Callback type for reading from a secondary indexer's storage.
pub type SecondaryGetFn = Arc<dyn Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync>;

/// Host state threaded through the wasmtime `Store`.
pub struct TertiaryState {
    pub input_data: Vec<u8>,
    pub pending_flush: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    /// This tertiary indexer's own KV store.
    pub own_storage: HashMap<Vec<u8>, Vec<u8>>,
    /// Named secondary indexer storage readers.
    pub secondary_storages: HashMap<String, SecondaryGetFn>,
    /// Runtime-provided configuration (JSON bytes).
    pub config_data: Vec<u8>,
    pub had_failure: bool,
    pub completed: bool,
    pub limits: StoreLimits,
}

/// A compiled tertiary indexer runtime using wasmtime.
pub struct NativeTertiaryRuntime {
    engine: Engine,
    module: Module,
}

impl NativeTertiaryRuntime {
    /// Compile a WASM module from bytes.
    pub fn new(wasm_bytes: &[u8]) -> Result<Self, String> {
        let mut config = Config::new();
        config.wasm_bulk_memory(true);
        config.wasm_multi_value(true);
        config.wasm_reference_types(true);
        config.wasm_simd(true);
        config.cranelift_nan_canonicalization(true);
        config.static_memory_maximum_size(0x100000000); // 4GB
        config.static_memory_guard_size(0x10000); // 64KB
        config.memory_init_cow(true);
        let engine = Engine::new(&config).map_err(|e| format!("wasmtime engine: {e}"))?;
        let module =
            Module::new(&engine, wasm_bytes).map_err(|e| format!("wasmtime compile: {e}"))?;
        Ok(NativeTertiaryRuntime { engine, module })
    }

    /// Run `_start()` for block processing.
    ///
    /// Returns the KV pairs flushed by the WASM module.
    pub fn run_block(
        &self,
        height: u32,
        block_data: &[u8],
        own_storage: &HashMap<Vec<u8>, Vec<u8>>,
        secondary_storages: &HashMap<String, SecondaryGetFn>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        self.run_block_with_config(height, block_data, own_storage, secondary_storages, &[])
    }

    /// Run `_start()` with runtime config.
    pub fn run_block_with_config(
        &self,
        height: u32,
        block_data: &[u8],
        own_storage: &HashMap<Vec<u8>, Vec<u8>>,
        secondary_storages: &HashMap<String, SecondaryGetFn>,
        config: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let mut input_data = Vec::with_capacity(4 + block_data.len());
        input_data.extend_from_slice(&height.to_le_bytes());
        input_data.extend_from_slice(block_data);

        let state = TertiaryState {
            input_data,
            pending_flush: None,
            own_storage: own_storage.clone(),
            secondary_storages: secondary_storages.clone(),
            config_data: config.to_vec(),
            had_failure: false,
            completed: false,
            limits: StoreLimitsBuilder::new()
                .memories(usize::MAX)
                .tables(usize::MAX)
                .instances(usize::MAX)
                .build(),
        };

        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);

        let instance = self.instantiate(&mut store)?;

        let start_fn = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .map_err(|e| format!("missing _start: {e}"))?;

        start_fn
            .call(&mut store, ())
            .map_err(|e| format!("_start failed: {e}"))?;

        let state = store.into_data();
        if state.had_failure {
            return Err("Tertiary WASM module aborted".into());
        }
        if !state.completed {
            return Err("Tertiary WASM module did not call __flush".into());
        }

        Ok(state.pending_flush.unwrap_or_default())
    }

    /// Call a view function on the tertiary indexer.
    ///
    /// Returns the raw bytes exported by the view function.
    pub fn call_view(
        &self,
        fn_name: &str,
        height: u32,
        payload: &[u8],
        own_storage: &HashMap<Vec<u8>, Vec<u8>>,
        secondary_storages: &HashMap<String, SecondaryGetFn>,
    ) -> Result<Vec<u8>, String> {
        self.call_view_with_config(fn_name, height, payload, own_storage, secondary_storages, &[])
    }

    /// Call a view function with runtime config.
    pub fn call_view_with_config(
        &self,
        fn_name: &str,
        height: u32,
        payload: &[u8],
        own_storage: &HashMap<Vec<u8>, Vec<u8>>,
        secondary_storages: &HashMap<String, SecondaryGetFn>,
        config: &[u8],
    ) -> Result<Vec<u8>, String> {
        let mut input_data = Vec::with_capacity(4 + payload.len());
        input_data.extend_from_slice(&height.to_le_bytes());
        input_data.extend_from_slice(payload);

        let state = TertiaryState {
            input_data,
            pending_flush: None,
            own_storage: own_storage.clone(),
            secondary_storages: secondary_storages.clone(),
            config_data: config.to_vec(),
            had_failure: false,
            completed: false,
            limits: StoreLimitsBuilder::new()
                .memories(usize::MAX)
                .tables(usize::MAX)
                .instances(usize::MAX)
                .build(),
        };

        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);

        let instance = self.instantiate(&mut store)?;

        let view_fn = instance
            .get_typed_func::<(), i32>(&mut store, fn_name)
            .map_err(|e| format!("missing view fn '{fn_name}': {e}"))?;

        let result_ptr = view_fn
            .call(&mut store, ())
            .map_err(|e| format!("view fn '{fn_name}' failed: {e}"))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or("no memory export")?;

        read_arraybuffer(&store, &memory, result_ptr)
    }

    /// Link host functions and instantiate.
    fn instantiate(&self, store: &mut Store<TertiaryState>) -> Result<Instance, String> {
        let mut linker = Linker::new(&self.engine);
        link_host_functions(&mut linker)?;
        linker
            .define_unknown_imports_as_traps(&self.module)
            .map_err(|e| format!("define unknown imports: {e}"))?;
        linker
            .instantiate(&mut *store, &self.module)
            .map_err(|e| format!("wasmtime instantiate: {e}"))
    }
}

// ---------------------------------------------------------------------------
// Host function linking
// ---------------------------------------------------------------------------

fn link_host_functions(linker: &mut Linker<TertiaryState>) -> Result<(), String> {
    // __host_len() -> i32
    linker
        .func_wrap("env", "__host_len", |caller: Caller<'_, TertiaryState>| -> i32 {
            caller.data().input_data.len() as i32
        })
        .map_err(|e| format!("link __host_len: {e}"))?;

    // __load_input(ptr: i32)
    linker
        .func_wrap(
            "env",
            "__load_input",
            |mut caller: Caller<'_, TertiaryState>, ptr: i32| {
                let data = caller.data().input_data.clone();
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                memory.write(&mut caller, ptr as usize, &data).ok();
            },
        )
        .map_err(|e| format!("link __load_input: {e}"))?;

    // __get_len(key_ptr: i32) -> i32  (reads from OWN storage)
    linker
        .func_wrap(
            "env",
            "__get_len",
            |mut caller: Caller<'_, TertiaryState>, key_ptr: i32| -> i32 {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let key = match read_arraybuffer(&caller, &memory, key_ptr) {
                    Ok(k) => k,
                    Err(_) => return 0,
                };
                match caller.data().own_storage.get(&key) {
                    Some(v) => v.len() as i32,
                    None => 0,
                }
            },
        )
        .map_err(|e| format!("link __get_len: {e}"))?;

    // __get(key_ptr: i32, value_ptr: i32)  (reads from OWN storage)
    linker
        .func_wrap(
            "env",
            "__get",
            |mut caller: Caller<'_, TertiaryState>, key_ptr: i32, value_ptr: i32| {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let key = match read_arraybuffer(&caller, &memory, key_ptr) {
                    Ok(k) => k,
                    Err(_) => return,
                };
                if let Some(value) = caller.data().own_storage.get(&key).cloned() {
                    memory.write(&mut caller, value_ptr as usize, &value).ok();
                }
            },
        )
        .map_err(|e| format!("link __get: {e}"))?;

    // __secondary_get_len(name_ptr: i32, key_ptr: i32) -> i32
    linker
        .func_wrap(
            "env",
            "__secondary_get_len",
            |mut caller: Caller<'_, TertiaryState>, name_ptr: i32, key_ptr: i32| -> i32 {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let name_bytes = match read_arraybuffer(&caller, &memory, name_ptr) {
                    Ok(b) => b,
                    Err(_) => return 0,
                };
                let name_str = match std::str::from_utf8(&name_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => return 0,
                };
                let key = match read_arraybuffer(&caller, &memory, key_ptr) {
                    Ok(k) => k,
                    Err(_) => return 0,
                };
                let getter = match caller.data().secondary_storages.get(&name_str) {
                    Some(g) => g.clone(),
                    None => return 0,
                };
                match getter(&key) {
                    Some(v) => v.len() as i32,
                    None => 0,
                }
            },
        )
        .map_err(|e| format!("link __secondary_get_len: {e}"))?;

    // __secondary_get(name_ptr: i32, key_ptr: i32, value_ptr: i32)
    linker
        .func_wrap(
            "env",
            "__secondary_get",
            |mut caller: Caller<'_, TertiaryState>,
             name_ptr: i32,
             key_ptr: i32,
             value_ptr: i32| {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let name_bytes = match read_arraybuffer(&caller, &memory, name_ptr) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let name_str = match std::str::from_utf8(&name_bytes) {
                    Ok(s) => s.to_string(),
                    Err(_) => return,
                };
                let key = match read_arraybuffer(&caller, &memory, key_ptr) {
                    Ok(k) => k,
                    Err(_) => return,
                };
                let getter = match caller.data().secondary_storages.get(&name_str) {
                    Some(g) => g.clone(),
                    None => return,
                };
                if let Some(value) = getter(&key) {
                    memory.write(&mut caller, value_ptr as usize, &value).ok();
                }
            },
        )
        .map_err(|e| format!("link __secondary_get: {e}"))?;

    // __host_config_len() -> i32
    linker
        .func_wrap("env", "__host_config_len", |caller: Caller<'_, TertiaryState>| -> i32 {
            caller.data().config_data.len() as i32
        })
        .map_err(|e| format!("link __host_config_len: {e}"))?;

    // __load_config(ptr: i32)
    linker
        .func_wrap(
            "env",
            "__load_config",
            |mut caller: Caller<'_, TertiaryState>, ptr: i32| {
                let data = caller.data().config_data.clone();
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                memory.write(&mut caller, ptr as usize, &data).ok();
            },
        )
        .map_err(|e| format!("link __load_config: {e}"))?;

    // __flush(data_ptr: i32)
    linker
        .func_wrap(
            "env",
            "__flush",
            |mut caller: Caller<'_, TertiaryState>, data_ptr: i32| {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let data = match read_arraybuffer(&caller, &memory, data_ptr) {
                    Ok(d) => d,
                    Err(_) => {
                        caller.data_mut().had_failure = true;
                        return;
                    }
                };

                let flush_msg = match proto::KeyValueFlush::decode(data.as_slice()) {
                    Ok(m) => m,
                    Err(_) => {
                        caller.data_mut().had_failure = true;
                        return;
                    }
                };

                let mut pairs = Vec::new();
                let list = &flush_msg.list;
                let mut i = 0;
                while i + 1 < list.len() {
                    pairs.push((list[i].to_vec(), list[i + 1].to_vec()));
                    i += 2;
                }

                // Also update own_storage so subsequent reads within the
                // same instance see the flushed values.
                for (k, v) in &pairs {
                    caller.data_mut().own_storage.insert(k.clone(), v.clone());
                }

                caller.data_mut().pending_flush = Some(pairs);
                caller.data_mut().completed = true;
            },
        )
        .map_err(|e| format!("link __flush: {e}"))?;

    // __log(ptr: i32)
    linker
        .func_wrap(
            "env",
            "__log",
            |mut caller: Caller<'_, TertiaryState>, ptr: i32| {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                if let Ok(msg_bytes) = read_arraybuffer(&caller, &memory, ptr) {
                    if let Ok(msg) = String::from_utf8(msg_bytes) {
                        eprintln!("[tertiary] {msg}");
                    }
                }
            },
        )
        .map_err(|e| format!("link __log: {e}"))?;

    // abort(msg_ptr, file_ptr, line, col)
    linker
        .func_wrap(
            "env",
            "abort",
            |mut caller: Caller<'_, TertiaryState>,
             msg_ptr: i32,
             _file: i32,
             line: i32,
             col: i32| {
                let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
                let msg = read_arraybuffer(&caller, &memory, msg_ptr)
                    .ok()
                    .map(|b| String::from_utf8_lossy(&b).to_string())
                    .unwrap_or_else(|| "<unreadable>".into());
                eprintln!("[tertiary] WASM abort at {line}:{col}: {msg}");
                caller.data_mut().had_failure = true;
            },
        )
        .map_err(|e| format!("link abort: {e}"))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// ArrayBuffer helper
// ---------------------------------------------------------------------------

/// Read an AssemblyScript ArrayBuffer from WASM memory.
///
/// Layout: 4-byte LE length at `(ptr - 4)`, then `length` bytes at `ptr`.
fn read_arraybuffer(
    store: impl AsContext,
    memory: &Memory,
    ptr: i32,
) -> Result<Vec<u8>, String> {
    if ptr < 4 {
        return Err("invalid arraybuffer pointer".into());
    }

    let mem_data = memory.data(store.as_context());
    let len_offset = (ptr - 4) as usize;

    if len_offset + 4 > mem_data.len() {
        return Err("arraybuffer length out of bounds".into());
    }

    let len = u32::from_le_bytes([
        mem_data[len_offset],
        mem_data[len_offset + 1],
        mem_data[len_offset + 2],
        mem_data[len_offset + 3],
    ]) as usize;

    let data_offset = ptr as usize;
    if data_offset + len > mem_data.len() {
        return Err("arraybuffer data out of bounds".into());
    }

    Ok(mem_data[data_offset..data_offset + len].to_vec())
}
