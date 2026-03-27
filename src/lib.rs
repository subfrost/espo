#[cfg(not(target_arch = "wasm32"))]
pub mod consts;
#[cfg(not(target_arch = "wasm32"))]
pub mod schemas;
#[cfg(not(target_arch = "wasm32"))]
pub mod utils;

// Modules that require non-WASM dependencies (tokio, rocksdb, etc.)
#[cfg(not(target_arch = "wasm32"))]
pub mod alkanes;
#[cfg(not(target_arch = "wasm32"))]
pub mod bitcoind_flexible;
#[cfg(not(target_arch = "wasm32"))]
pub mod config;
#[cfg(not(target_arch = "wasm32"))]
pub mod core;
#[cfg(not(target_arch = "wasm32"))]
pub mod debug;
#[cfg(not(target_arch = "wasm32"))]
pub mod explorer;
#[cfg(not(target_arch = "wasm32"))]
pub mod modules;
#[cfg(not(target_arch = "wasm32"))]
pub mod runtime;

// Test utilities available for testing
// Always compiled for non-wasm to support both unit tests and integration tests
// Heavy dependencies (metashrew-runtime, wasmtime) are gated behind test-utils feature internally
#[cfg(not(target_arch = "wasm32"))]
pub mod test_utils;

// Tertiary indexer module — only compiled for WASM tertiary builds
#[cfg(all(target_arch = "wasm32", feature = "tertiary"))]
pub mod tertiary;

// WASM tests module
#[cfg(all(test, target_arch = "wasm32"))]
pub mod tests;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::OnceLock;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::AtomicU32;

// Shared ESPO height cell (used by config helpers and the indexer).
#[cfg(not(target_arch = "wasm32"))]
pub static ESPO_HEIGHT: OnceLock<Arc<AtomicU32>> = OnceLock::new();

// Last known safe tip height fetched from the block source.
#[cfg(not(target_arch = "wasm32"))]
pub static SAFE_TIP: OnceLock<Arc<AtomicU32>> = OnceLock::new();
