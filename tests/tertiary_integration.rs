//! Integration tests for espo's tertiary indexer WASM.
//!
//! Tests load the compiled espo.wasm (built with `--features tertiary --target wasm32-unknown-unknown`)
//! through the NativeTertiaryRuntime, feeding it alkanes state from TestMetashrewRuntime.

use espo::test_utils::metashrew_runtime::TestMetashrewRuntime;
use espo::test_utils::tertiary_runtime::{NativeTertiaryRuntime, SecondaryGetFn};
use std::collections::HashMap;
use std::sync::Arc;

/// Path to the pre-built espo.wasm tertiary indexer.
/// Build with: cargo build --lib --target wasm32-unknown-unknown --features tertiary --release
fn load_espo_wasm() -> Vec<u8> {
    let paths = [
        "target/wasm32-unknown-unknown/release/espo.wasm",
        "target/wasm32-unknown-unknown/debug/espo.wasm",
    ];
    for path in &paths {
        if let Ok(bytes) = std::fs::read(path) {
            return bytes;
        }
    }
    panic!(
        "espo.wasm not found. Build it first with:\n  \
         cargo build --lib --target wasm32-unknown-unknown --features tertiary --release"
    );
}

/// Create a secondary storage reader backed by a RocksDB instance.
fn make_alkanes_reader(db: &Arc<rocksdb::DB>) -> SecondaryGetFn {
    let db = db.clone();
    Arc::new(move |key: &[u8]| -> Option<Vec<u8>> { db.get(key).ok().flatten() })
}

/// Helper to build secondary storages map with alkanes reader.
fn secondary_storages(reader: &SecondaryGetFn) -> HashMap<String, SecondaryGetFn> {
    let mut map = HashMap::new();
    map.insert("alkanes".to_string(), reader.clone());
    map
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tertiary_wasm_loads_and_starts() {
    let wasm = load_espo_wasm();
    let rt = NativeTertiaryRuntime::new(&wasm).expect("Failed to compile espo.wasm");

    // Create metashrew runtime for alkanes state
    let metashrew = TestMetashrewRuntime::new().expect("Failed to create TestMetashrewRuntime");
    let reader = make_alkanes_reader(metashrew.db());
    let secondaries = secondary_storages(&reader);

    // Create a minimal regtest block and index it through alkanes
    let block = espo::test_utils::ChainBuilder::new().add_blocks(1).build();
    let first_block = &block[0];
    metashrew
        .index_block(first_block, 0)
        .expect("Failed to index block 0");

    // Run the block through espo's tertiary indexer
    let block_bytes = bitcoin::consensus::serialize(first_block);
    let own_storage = HashMap::new();
    let pairs = rt
        .run_block(0, &block_bytes, &own_storage, &secondaries)
        .expect("_start() failed");

    // The block processing should complete without error.
    // It may or may not produce KV pairs depending on whether there are traces.
    eprintln!("Block 0: {} KV pairs flushed", pairs.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tertiary_ping_view() {
    let wasm = load_espo_wasm();
    let rt = NativeTertiaryRuntime::new(&wasm).expect("Failed to compile espo.wasm");

    let own_storage = HashMap::new();
    let secondaries = HashMap::new();

    // Call the ping view function
    let result = rt
        .call_view("ping", 0, &[], &own_storage, &secondaries)
        .expect("ping view failed");

    let json_str = String::from_utf8(result).expect("ping returned non-UTF8");
    eprintln!("ping response: {json_str}");
    assert!(json_str.contains("pong") || json_str.contains("ok") || !json_str.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tertiary_get_block_height_view() {
    let wasm = load_espo_wasm();
    let rt = NativeTertiaryRuntime::new(&wasm).expect("Failed to compile espo.wasm");

    // Create metashrew runtime and index a few blocks
    let metashrew = TestMetashrewRuntime::new().expect("Failed to create TestMetashrewRuntime");
    let reader = make_alkanes_reader(metashrew.db());
    let secondaries = secondary_storages(&reader);

    let blocks = espo::test_utils::ChainBuilder::new().add_blocks(5).build();
    let mut own_storage = HashMap::new();

    for (i, block) in blocks.iter().enumerate() {
        metashrew
            .index_block(block, i as u32)
            .expect("Failed to index block");

        let block_bytes = bitcoin::consensus::serialize(block);
        let pairs = rt
            .run_block(i as u32, &block_bytes, &own_storage, &secondaries)
            .expect("_start() failed");

        // Apply flushed pairs to own storage
        for (k, v) in pairs {
            own_storage.insert(k, v);
        }
    }

    // Call get_block_height view
    let result = rt
        .call_view("get_block_height", 4, &[], &own_storage, &secondaries)
        .expect("get_block_height view failed");

    let json_str = String::from_utf8(result).expect("non-UTF8 response");
    eprintln!("get_block_height response: {json_str}");
    // Should report a non-zero height after indexing 5 blocks
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("invalid JSON");
    let height = parsed["height"].as_u64().expect("missing height field");
    assert!(height > 0, "Expected non-zero height, got: {json_str}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tertiary_get_alkanes_by_address_empty() {
    let wasm = load_espo_wasm();
    let rt = NativeTertiaryRuntime::new(&wasm).expect("Failed to compile espo.wasm");

    let metashrew = TestMetashrewRuntime::new().expect("Failed to create TestMetashrewRuntime");
    let reader = make_alkanes_reader(metashrew.db());
    let secondaries = secondary_storages(&reader);

    // Index a block through both alkanes and espo
    let blocks = espo::test_utils::ChainBuilder::new().add_blocks(1).build();
    metashrew
        .index_block(&blocks[0], 0)
        .expect("Failed to index block");

    let block_bytes = bitcoin::consensus::serialize(&blocks[0]);
    let mut own_storage = HashMap::new();
    let pairs = rt
        .run_block(0, &block_bytes, &own_storage, &secondaries)
        .expect("_start() failed");
    for (k, v) in pairs {
        own_storage.insert(k, v);
    }

    // Query for a random address that has no balances
    let address = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080";
    let result = rt
        .call_view(
            "get_alkanes_by_address",
            0,
            address.as_bytes(),
            &own_storage,
            &secondaries,
        )
        .expect("get_alkanes_by_address view failed");

    let json_str = String::from_utf8(result).expect("non-UTF8 response");
    eprintln!("get_alkanes_by_address (empty): {json_str}");
    // Should return empty array for address with no alkanes
    assert_eq!(json_str, "[]");
}
