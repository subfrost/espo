//! WASM entry point for tertiary indexer block processing.

use qubitcoin_tertiary_support::{flush, initialize, input, set};
use std::sync::Arc;

/// Block processing entry point.
///
/// Reads execution traces from alkanes secondary storage for this block,
/// classifies contract interactions into activity events, and stores
/// them in espo's own persistent storage for fast view queries.
#[unsafe(no_mangle)]
pub extern "C" fn _start() {
    initialize();
    let data = input();
    if data.len() < 4 {
        flush();
        return;
    }
    let height = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);

    // Process traces for this block — read from alkanes secondary, classify, store
    let records = super::trace::process_block_traces(height);
    if !records.is_empty() {
        super::trace::store_activity_records(&records);
    }

    // Store current height
    set(
        Arc::new(b"__espo_height__".to_vec()),
        Arc::new(height.to_le_bytes().to_vec()),
    );
    flush();
}
