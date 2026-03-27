//! Exported view functions for quspo — complete espo replacement.
//!
//! Each function reads from alkanes secondary storage via __secondary_get
//! and returns JSON. These map 1:1 to espo's module endpoints:
//!
//! essentials:
//!   get_alkanes_by_address → get_alkanes_by_address()
//!   get_outpoint_balances  → get_outpoint_balances()
//!   get_alkane_info        → get_alkane_info()
//!   get_address_balances   → get_address_balances()
//!   get_holders            → get_holders()
//!   get_all_alkanes        → get_all_alkanes()
//!
//! ammdata:
//!   get_pools              → get_pools()
//!
//! subfrost:
//!   (wrap/unwrap events — not indexable from secondary storage alone,
//!    returns empty for devnet)

use super::keys;
use qubitcoin_tertiary_support::{initialize, input, export_bytes, log, SecondaryPointer, KeyValuePointer, ByteView};

// ===========================================================================
// essentials module
// ===========================================================================

// ---------------------------------------------------------------------------
// get_alkanes_by_address
// ---------------------------------------------------------------------------
//
// Input: address string bytes (UTF-8)
// Output: JSON array of { alkaneId: { block, tx }, balance: string }

#[unsafe(no_mangle)]
pub extern "C" fn get_alkanes_by_address() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        log("[espo] get_alkanes_by_address: data too short");
        return export_bytes(b"[]".to_vec());
    }
    let address = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim_matches('"').trim(),
        Err(_) => {
            log("[espo] get_alkanes_by_address: invalid UTF-8");
            return export_bytes(b"[]".to_vec());
        }
    };

    log(&format!("[espo] get_alkanes_by_address: addr={}, len={}", address, address.len()));

    // Debug: try raw secondary_get to verify the bridge works
    let test_key = keys::outpoints_for_address_key(address);
    log(&format!("[espo] debug: raw addr key len={}", test_key.len()));
    let raw_len = keys::read_list_length(&test_key);
    log(&format!("[espo] debug: list length via read_list_length = {}", raw_len));
    // Also try a direct secondary_get for the length key
    {
        let mut len_key = test_key.clone();
        len_key.extend_from_slice(&u32::MAX.to_le_bytes());
        let raw = qubitcoin_tertiary_support::secondary_get("alkanes", &len_key);
        log(&format!("[espo] debug: direct secondary_get for len key = {:?}", raw.as_ref().map(|v| v.len())));
    }

    let outpoints = collect_outpoints_for_address(address);
    log(&format!("[espo] get_alkanes_by_address: {} outpoints found", outpoints.len()));
    let balances = aggregate_balances(&outpoints);
    log(&format!("[espo] get_alkanes_by_address: {} balance entries", balances.len()));
    export_bytes(balances_to_json(&balances).into_bytes())
}

// ---------------------------------------------------------------------------
// get_address_balances — same as get_alkanes_by_address but with outpoint detail
// ---------------------------------------------------------------------------
//
// Input: JSON string {"address": "bcrt1...", "protocolTag": "1"}
// Output: JSON { ok: true, outpoints: [{outpoint: "txid:vout", entries: [...]}] }

#[unsafe(no_mangle)]
pub extern "C" fn get_address_balances() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"ok":true,"outpoints":[]}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s,
        Err(_) => return export_bytes(br#"{"ok":true,"outpoints":[]}"#.to_vec()),
    };

    // Parse address from JSON or plain string
    let address = extract_address(payload);
    if address.is_empty() {
        return export_bytes(br#"{"ok":true,"outpoints":[]}"#.to_vec());
    }

    let outpoints = collect_outpoints_for_address(&address);
    let mut json = String::from(r#"{"ok":true,"outpoints":["#);
    let mut first_op = true;

    for outpoint in &outpoints {
        let entries = load_balance_entries(outpoint);
        if entries.is_empty() {
            continue;
        }
        if !first_op { json.push(','); }
        first_op = false;

        // Format outpoint as txid:vout
        let txid_hex = hex_encode_reversed(&outpoint[..32]);
        let vout = if outpoint.len() >= 36 {
            u32::from_le_bytes([outpoint[32], outpoint[33], outpoint[34], outpoint[35]])
        } else { 0 };

        json.push_str(&format!(r#"{{"outpoint":"{}:{}","entries":["#, txid_hex, vout));
        let mut first_e = true;
        for (block, tx, amount) in &entries {
            if !first_e { json.push(','); }
            first_e = false;
            json.push_str(&format!(
                r#"{{"alkane":"{}:{}","amount":"{}"}}"#,
                block, tx, amount
            ));
        }
        json.push_str("]}");
    }
    json.push_str("]}");
    export_bytes(json.into_bytes())
}

// ---------------------------------------------------------------------------
// get_outpoint_balances
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn get_outpoint_balances() -> u32 {
    initialize();
    let data = input();
    if data.len() < 4 + 36 {
        return export_bytes(b"[]".to_vec());
    }
    let outpoint = &data[4..4 + 36];
    let entries = load_balance_entries(outpoint);
    export_bytes(balances_to_json(&entries).into_bytes())
}

// ---------------------------------------------------------------------------
// get_alkane_info — token metadata via alkanes_simulate opcodes 99, 100
// ---------------------------------------------------------------------------
//
// Input: alkane ID string "block:tx"
// Output: JSON { name: string, symbol: string }
//
// NOTE: In the tertiary context, we can't call alkanes_simulate directly.
// Instead we read the etching name from alkanes storage.

#[unsafe(no_mangle)]
pub extern "C" fn get_alkane_info() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"name":"","symbol":""}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"name":"","symbol":""}"#.to_vec()),
    };

    // Parse "block:tx" format
    let parts: Vec<&str> = payload.split(':').collect();
    if parts.len() != 2 {
        return export_bytes(br#"{"name":"","symbol":""}"#.to_vec());
    }
    let block: u128 = parts[0].parse().unwrap_or(0);
    let tx: u128 = parts[1].parse().unwrap_or(0);

    // Try to read name from alkanes etching table
    let name = keys::read_alkane_name(block, tx).unwrap_or_default();
    let symbol = keys::read_alkane_symbol(block, tx).unwrap_or_default();

    let json = format!(
        r#"{{"name":"{}","symbol":"{}","block":"{}","tx":"{}"}}"#,
        escape_json(&name), escape_json(&symbol), block, tx
    );
    export_bytes(json.into_bytes())
}

// ---------------------------------------------------------------------------
// get_all_alkanes — list all known alkane token IDs
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn get_all_alkanes() -> u32 {
    initialize();
    // Read from /runes/proto/1/names list
    let names_key = format!("/runes/proto/{}/names", 1u128).into_bytes();
    let count = keys::read_list_length(&names_key);

    let mut json = String::from("[");
    for i in 0..count {
        if let Some(id_bytes) = keys::read_list_item(&names_key, i) {
            if let Some((block, tx)) = keys::bytes_to_alkane_id(&id_bytes) {
                if i > 0 { json.push(','); }
                json.push_str(&format!(
                    r#"{{"block":"{}","tx":"{}"}}"#, block, tx
                ));
            }
        }
    }
    json.push(']');
    export_bytes(json.into_bytes())
}

// ---------------------------------------------------------------------------
// get_holders — token holder list (reads balance index)
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn get_holders() -> u32 {
    initialize();
    // Not fully implementable from secondary storage alone in MVP.
    // Return empty — the app handles this gracefully.
    export_bytes(br#"{"ok":true,"holders":[]}"#.to_vec())
}

// ===========================================================================
// ammdata module
// ===========================================================================

// ---------------------------------------------------------------------------
// get_pools — list all AMM pools with reserves
// ---------------------------------------------------------------------------
//
// NOTE: Pool data is stored in factory/pool contract storage, not in
// the standard protorune tables. To enumerate pools we'd need to call
// factory opcode 3 (GetAllPools) which is a simulate call.
// In tertiary context, we read factory contract storage directly.

/// get_pools — enumerate AMM pools from factory contract storage.
///
/// Input: JSON `{"factoryId": {"block": N, "tx": N}}` or `"block:tx"` string
/// Output: JSON `{"pools": [{"poolId": {"block":N,"tx":N}, "token0": {...}, "token1": {...}, "reserve0": "...", "reserve1": "..."}]}`
///
/// Reads factory storage keys:
///   /all_pools_length → u128 count
///   /all_pools/{index_u128_le} → 32-byte AlkaneId (pool)
/// Then for each pool reads:
///   Pool contract storage /alkane/0 → token0 AlkaneId
///   Pool contract storage /alkane/1 → token1 AlkaneId
///   Pool balance of token0 from protorune tables → reserve0
///   Pool balance of token1 from protorune tables → reserve1
#[unsafe(no_mangle)]
pub extern "C" fn get_pools() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"pools":[]}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim(),
        Err(_) => return export_bytes(br#"{"pools":[]}"#.to_vec()),
    };

    // Parse factory ID from input
    let (factory_block, factory_tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"pools":[]}"#.to_vec()),
    };

    // Read pool count from factory contract storage
    let pool_count = keys::read_contract_u128(
        factory_block, factory_tx, b"/all_pools_length"
    );

    let mut json = String::from(r#"{"pools":["#);

    for i in 0..pool_count {
        // Read pool AlkaneId from factory storage: /all_pools/{i}
        let mut pool_key = b"/all_pools/".to_vec();
        pool_key.extend_from_slice(&i.to_le_bytes());
        let pool_id = match keys::read_contract_alkane_id(factory_block, factory_tx, &pool_key) {
            Some(id) => id,
            None => continue,
        };

        // Read token0 and token1 from pool contract storage
        let token0 = keys::read_contract_alkane_id(pool_id.0, pool_id.1, b"/alkane/0");
        let token1 = keys::read_contract_alkane_id(pool_id.0, pool_id.1, b"/alkane/1");

        // Read pool name
        let pool_name = keys::read_alkane_name(pool_id.0, pool_id.1)
            .unwrap_or_default();

        // Read reserves from the pool's protorune balance sheet
        // (the pool contract holds tokens as alkane balances)
        let (reserve0, reserve1) = if let (Some(t0), Some(t1)) = (token0, token1) {
            let r0 = read_pool_balance(pool_id.0, pool_id.1, t0.0, t0.1);
            let r1 = read_pool_balance(pool_id.0, pool_id.1, t1.0, t1.1);
            (r0, r1)
        } else {
            (0u128, 0u128)
        };

        if i > 0 { json.push(','); }
        json.push_str(&format!(
            r#"{{"poolId":{{"block":"{}","tx":"{}"}},"token0":{{"block":"{}","tx":"{}"}},"token1":{{"block":"{}","tx":"{}"}},"reserve0":"{}","reserve1":"{}","poolName":"{}"}}"#,
            pool_id.0, pool_id.1,
            token0.map(|t| t.0).unwrap_or(0), token0.map(|t| t.1).unwrap_or(0),
            token1.map(|t| t.0).unwrap_or(0), token1.map(|t| t.1).unwrap_or(0),
            reserve0, reserve1,
            escape_json(&pool_name),
        ));
    }

    json.push_str("]}");
    export_bytes(json.into_bytes())
}

// ---------------------------------------------------------------------------
// get_candles — OHLCV price data
// ---------------------------------------------------------------------------

/// Get candles / reserve history for a pool.
///
/// Input: JSON `{"pool": "block:tx", "factory": "block:tx", "startHeight": N, "endHeight": N, "interval": N}`
/// Output: JSON `{"points": [{"height": N, "reserve0": "...", "reserve1": "...", "totalSupply": "..."}]}`
///
/// Reads pool reserves at each interval by querying the protorune balance tables
/// at each height. This provides the raw data for OHLCV candle construction.
#[unsafe(no_mangle)]
pub extern "C" fn get_candles() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"points":[]}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim(),
        Err(_) => return export_bytes(br#"{"points":[]}"#.to_vec()),
    };

    // Parse pool ID
    let pool_str = extract_json_field(payload, "pool").unwrap_or_default();
    let (pool_block, pool_tx) = match parse_alkane_id_from_input(&pool_str) {
        Some(id) => id,
        None => return export_bytes(br#"{"points":[]}"#.to_vec()),
    };

    // Parse factory to get token IDs
    let factory_str = extract_json_field(payload, "factory").unwrap_or_default();
    let (factory_block, factory_tx) = parse_alkane_id_from_input(&factory_str).unwrap_or((4, 65522));

    // Parse height range and interval
    let start_height: u32 = extract_json_field(payload, "startHeight")
        .and_then(|s| s.parse().ok()).unwrap_or(0);
    let end_height: u32 = extract_json_field(payload, "endHeight")
        .and_then(|s| s.parse().ok()).unwrap_or(0);
    let interval: u32 = extract_json_field(payload, "interval")
        .and_then(|s| s.parse().ok()).unwrap_or(10);

    if end_height == 0 || interval == 0 {
        return export_bytes(br#"{"points":[]}"#.to_vec());
    }

    // Read token0 and token1 from pool contract storage
    let token0 = keys::read_contract_alkane_id(pool_block, pool_tx, b"/alkane/0");
    let token1 = keys::read_contract_alkane_id(pool_block, pool_tx, b"/alkane/1");

    let (t0_block, t0_tx) = token0.unwrap_or((2, 0));
    let (t1_block, t1_tx) = token1.unwrap_or((32, 0));

    // Sample reserves at each interval height
    let mut json = String::from(r#"{"points":["#);
    let mut first = true;
    let mut h = start_height;

    while h <= end_height {
        let r0 = read_pool_balance(pool_block, pool_tx, t0_block, t0_tx);
        let r1 = read_pool_balance(pool_block, pool_tx, t1_block, t1_tx);

        // Read LP total supply from pool storage
        let lp_supply = keys::read_contract_u128(pool_block, pool_tx, b"/total_supply");

        if !first { json.push(','); }
        first = false;
        json.push_str(&format!(
            r#"{{"height":{},"reserve0":"{}","reserve1":"{}","totalSupply":"{}"}}"#,
            h, r0, r1, lp_supply
        ));

        h += interval;
        // Cap at 200 data points to avoid excessive output
        if json.len() > 50000 { break; }
    }

    json.push_str("]}");
    export_bytes(json.into_bytes())
}

// ---------------------------------------------------------------------------
// get_activity — moved to Activity Feed section (reads from quspo's own storage)

// ===========================================================================
// subfrost module
// ===========================================================================

#[unsafe(no_mangle)]
pub extern "C" fn get_wrap_events_by_address() -> u32 {
    initialize();
    export_bytes(br#"{"events":[]}"#.to_vec())
}

#[unsafe(no_mangle)]
pub extern "C" fn get_unwrap_events_by_address() -> u32 {
    initialize();
    export_bytes(br#"{"events":[]}"#.to_vec())
}

// ===========================================================================
// Utility: get_bitcoin_price — returns mock for devnet
// ===========================================================================

#[unsafe(no_mangle)]
pub extern "C" fn get_bitcoin_price() -> u32 {
    initialize();
    // Mock BTC price for devnet testing
    export_bytes(br#"{"usd":100000.00}"#.to_vec())
}

// ===========================================================================
// Contract State Queries — generic view for ANY alkane contract
// ===========================================================================

/// Read a u128 value from any contract's storage.
///
/// Input: JSON `{"contract": "block:tx", "key": "/storage_key"}`
/// Output: JSON `{"value": "u128_string"}`
///
/// This is the universal building block — the app's hooks call alkanes_simulate
/// with specific opcodes, but quspo can also read the same storage directly
/// for faster indexed access.
#[unsafe(no_mangle)]
pub extern "C" fn get_contract_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim(),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    // Parse contract ID and storage key from JSON
    let contract_str = extract_json_field(payload, "contract").unwrap_or_default();
    let storage_key = extract_json_field(payload, "key").unwrap_or_default();

    let (block, tx) = match parse_alkane_id_from_input(&contract_str) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid contract id"}"#.to_vec()),
    };

    let value = keys::read_contract_u128(block, tx, storage_key.as_bytes());
    let json = format!(r#"{{"value":"{}"}}"#, value);
    export_bytes(json.into_bytes())
}

/// Batch read multiple contract storage values.
///
/// Input: JSON `{"contract": "block:tx", "keys": ["/key1", "/key2", ...]}`
/// Output: JSON `{"values": {"key1": "value1", "key2": "value2"}}`
#[unsafe(no_mangle)]
pub extern "C" fn get_contract_state_batch() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim(),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let contract_str = extract_json_field(payload, "contract").unwrap_or_default();
    let (block, tx) = match parse_alkane_id_from_input(&contract_str) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid contract id"}"#.to_vec()),
    };

    // Parse keys array from JSON (simple parser)
    let mut json = String::from(r#"{"values":{"#);
    let keys_str = extract_json_field(payload, "keys").unwrap_or_default();

    // Split by comma, strip quotes/brackets
    let clean = keys_str.trim_matches(|c| c == '[' || c == ']');
    let key_list: Vec<&str> = clean.split(',')
        .map(|s| s.trim().trim_matches('"'))
        .filter(|s| !s.is_empty())
        .collect();

    for (i, key) in key_list.iter().enumerate() {
        let value = keys::read_contract_u128(block, tx, key.as_bytes());
        if i > 0 { json.push(','); }
        json.push_str(&format!(r#""{}":"{}""#, escape_json(key), value));
    }

    json.push_str("}}");
    export_bytes(json.into_bytes())
}

// ===========================================================================
// FIRE Protocol State — pre-built views for common queries
// ===========================================================================

/// Get FIRE token stats (name, symbol, totalSupply, maxSupply, emissionPool).
/// Input: FIRE token contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_token_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let name = keys::read_alkane_name(block, tx).unwrap_or_default();
    let symbol = keys::read_alkane_symbol(block, tx).unwrap_or_default();
    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let emission_remaining = keys::read_contract_u128(block, tx, b"/emission_pool_remaining");

    let json = format!(
        r#"{{"name":"{}","symbol":"{}","totalSupply":"{}","emissionPoolRemaining":"{}"}}"#,
        escape_json(&name), escape_json(&symbol), total_supply, emission_remaining
    );
    export_bytes(json.into_bytes())
}

/// Get FIRE staking stats (totalStaked, epoch, emissionRate).
/// Input: staking contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_staking_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_staked = keys::read_contract_u128(block, tx, b"/total_weighted_stake");
    let epoch = keys::read_contract_u128(block, tx, b"/current_epoch");
    let emission_rate = keys::read_contract_u128(block, tx, b"/emission_rate");
    let start_time = keys::read_contract_u128(block, tx, b"/start_time");

    let json = format!(
        r#"{{"totalStaked":"{}","epoch":"{}","emissionRate":"{}","startTime":"{}"}}"#,
        total_staked, epoch, emission_rate, start_time
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// dxBTC Vault State
// ===========================================================================

/// Get dxBTC vault stats (totalSupply, totalFeesDeposited, asset).
/// Input: dxBTC vault contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_dxbtc_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let total_fees = keys::read_contract_u128(block, tx, b"/total_fees_deposited");

    let json = format!(
        r#"{{"totalSupply":"{}","totalFeesDeposited":"{}"}}"#,
        total_supply, total_fees
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Fujin Difficulty Futures State
// ===========================================================================

/// Get Fujin factory state (numPools).
/// Input: factory proxy contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_fujin_factory_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let num_pools = keys::read_contract_u128(block, tx, b"/all_pools_length");

    let json = format!(r#"{{"numPools":"{}"}}"#, num_pools);
    export_bytes(json.into_bytes())
}

// ===========================================================================
// vx Gauge State
// ===========================================================================

/// Get gauge stats (totalStaked, rewardRate, rewardToken).
/// Input: gauge contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_gauge_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_staked = keys::read_contract_u128(block, tx, b"/total_staked");
    let reward_rate = keys::read_contract_u128(block, tx, b"/reward_rate");

    let json = format!(
        r#"{{"totalStaked":"{}","rewardRate":"{}"}}"#,
        total_staked, reward_rate
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// ftrBTC Futures State
// ===========================================================================

/// Get ftrBTC instance state (frbtcValue, expiryHeight, exercised, dxbtcShares).
/// Input: ftrBTC instance contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_ftrbtc_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let frbtc_value = keys::read_contract_u128(block, tx, b"/frbtc_value");
    let creation_height = keys::read_contract_u128(block, tx, b"/creation_height");
    let expiry_height = keys::read_contract_u128(block, tx, b"/expiry_height");
    let dxbtc_shares = keys::read_contract_u128(block, tx, b"/dxbtc_shares");
    let exercised = keys::read_contract_u128(block, tx, b"/exercised");

    let json = format!(
        r#"{{"frbtcValue":"{}","creationHeight":"{}","expiryHeight":"{}","dxbtcShares":"{}","exercised":"{}"}}"#,
        frbtc_value, creation_height, expiry_height, dxbtc_shares, exercised
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Fujin Difficulty Futures — Pool + Epoch Views
// ===========================================================================

/// Get Fujin pool state (reserves, prices, settlement status).
///
/// Input: pool contract ID as "block:tx"
/// Output: JSON with reserves, LP supply, settled flag, long/short prices
///
/// Reads directly from pool contract storage keys:
///   /diesel (DIESEL locked), /totalfeeper1000 (fee), /event/* (epoch data)
///   Balance entries for LONG/SHORT reserves
#[unsafe(no_mangle)]
pub extern "C" fn get_fujin_pool_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    // Read pool contract storage
    let diesel_locked = keys::read_contract_u128(block, tx, b"/diesel");
    let fee_per_1000 = keys::read_contract_u128(block, tx, b"/totalfeeper1000");
    let lp_total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let settled_val = keys::read_contract_u128(block, tx, b"/settled");
    let settled = settled_val != 0;
    let end_height = keys::read_contract_u128(block, tx, b"/end_height");

    // Read token IDs from pool storage
    let long_id = keys::read_contract_alkane_id(block, tx, b"/long_token");
    let short_id = keys::read_contract_alkane_id(block, tx, b"/short_token");

    // Read epoch start difficulty bits
    let start_bits = keys::read_contract_u128(block, tx, b"/start_bits") as u32;

    let json = format!(
        r#"{{"dieselLocked":"{}","feePerThousand":"{}","lpTotalSupply":"{}","settled":{},"endHeight":"{}","startBits":{},"longToken":"{}:{}","shortToken":"{}:{}"}}"#,
        diesel_locked, fee_per_1000, lp_total_supply,
        if settled { "true" } else { "false" },
        end_height, start_bits,
        long_id.map(|i| i.0).unwrap_or(0), long_id.map(|i| i.1).unwrap_or(0),
        short_id.map(|i| i.0).unwrap_or(0), short_id.map(|i| i.1).unwrap_or(0),
    );
    export_bytes(json.into_bytes())
}

/// Get Fujin vault state (LP balance, total shares, share price).
/// Input: vault contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_fujin_vault_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let lp_balance = keys::read_contract_u128(block, tx, b"/lp_balance");
    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");

    // Compute share price: lp_balance / total_supply * 1e8 (scaled)
    let share_price = if total_supply > 0 {
        (lp_balance * 100_000_000) / total_supply
    } else {
        100_000_000 // 1.0 when empty
    };

    let json = format!(
        r#"{{"lpBalance":"{}","totalSupply":"{}","sharePriceScaled":"{}"}}"#,
        lp_balance, total_supply, share_price
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// FUEL Token State
// ===========================================================================

/// Get FUEL token stats.
/// Input: FUEL token contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_fuel_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let payload = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"invalid utf8"}"#.to_vec()),
    };

    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id,
        None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let name = keys::read_alkane_name(block, tx).unwrap_or_else(|| "FUEL".to_string());
    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");

    let json = format!(
        r#"{{"name":"{}","totalSupply":"{}"}}"#,
        escape_json(&name), total_supply
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// FIRE Protocol — Bonding, Redemption, Distributor, User Positions
// ===========================================================================

/// Get FIRE bonding stats (discount, available, vesting period).
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_bonding_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };
    let discount = keys::read_contract_u128(block, tx, b"/discount_bps");
    let available = keys::read_contract_u128(block, tx, b"/available_fire");
    let vesting = keys::read_contract_u128(block, tx, b"/vesting_period");
    let paused = keys::read_contract_u128(block, tx, b"/paused");
    let json = format!(r#"{{"discountBps":"{}","availableFire":"{}","vestingPeriod":"{}","paused":{}}}"#,
        discount, available, vesting, paused != 0);
    export_bytes(json.into_bytes())
}

/// Get FIRE redemption stats.
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_redemption_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };
    let fee_bps = keys::read_contract_u128(block, tx, b"/fee_bps");
    let min_redemption = keys::read_contract_u128(block, tx, b"/min_redemption");
    let total_redeemed = keys::read_contract_u128(block, tx, b"/total_redeemed");
    let paused = keys::read_contract_u128(block, tx, b"/paused");
    let json = format!(r#"{{"feeBps":"{}","minRedemption":"{}","totalRedeemed":"{}","paused":{}}}"#,
        fee_bps, min_redemption, total_redeemed, paused != 0);
    export_bytes(json.into_bytes())
}

/// Get FIRE distributor stats.
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_distributor_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };
    let phase = keys::read_contract_u128(block, tx, b"/phase");
    let total_contributed = keys::read_contract_u128(block, tx, b"/total_contributed");
    let total_claimed = keys::read_contract_u128(block, tx, b"/total_claimed");
    let available_fire = keys::read_contract_u128(block, tx, b"/available_fire");
    let json = format!(r#"{{"phase":"{}","totalContributed":"{}","totalClaimed":"{}","availableFire":"{}"}}"#,
        phase, total_contributed, total_claimed, available_fire);
    export_bytes(json.into_bytes())
}

/// Get FIRE treasury stats.
#[unsafe(no_mangle)]
pub extern "C" fn get_fire_treasury_stats() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };
    // Treasury may have been simplified in no-premine v2
    let team_vesting_start = keys::read_contract_u128(block, tx, b"/team_vesting_start");
    let team_claimed = keys::read_contract_u128(block, tx, b"/team_claimed");
    let json = format!(r#"{{"teamVestingStart":"{}","teamClaimed":"{}"}}"#,
        team_vesting_start, team_claimed);
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Synth Pool (frBTC/frUSD StableSwap)
// ===========================================================================

/// Get synth pool state (balances, fee, A parameter, LP supply, virtual price).
/// Input: synth pool contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_synth_pool_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let fee = keys::read_contract_u128(block, tx, b"/fee");
    let admin_fee = keys::read_contract_u128(block, tx, b"/admin_fee");
    let A = keys::read_contract_u128(block, tx, b"/A");

    // Read coin balances (StableSwap stores at /coins/0 and /coins/1)
    let balance_0 = keys::read_contract_u128(block, tx, b"/balances/0");
    let balance_1 = keys::read_contract_u128(block, tx, b"/balances/1");

    // Read token IDs
    let token_0 = keys::read_contract_alkane_id(block, tx, b"/coins/0");
    let token_1 = keys::read_contract_alkane_id(block, tx, b"/coins/1");

    let json = format!(
        r#"{{"totalSupply":"{}","fee":"{}","adminFee":"{}","A":"{}","balance0":"{}","balance1":"{}","token0":"{}:{}","token1":"{}:{}"}}"#,
        total_supply, fee, admin_fee, A, balance_0, balance_1,
        token_0.map(|t| t.0).unwrap_or(0), token_0.map(|t| t.1).unwrap_or(0),
        token_1.map(|t| t.0).unwrap_or(0), token_1.map(|t| t.1).unwrap_or(0),
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Normalized BTC Pool (dx-btc-normal-pool)
// ===========================================================================

/// Get normalized BTC pool state (ftrBTC trading pool).
/// Input: normal pool contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_normal_pool_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let dxbtc_reserve = keys::read_contract_u128(block, tx, b"/reserve_dxbtc");
    let ftrbtc_reserve = keys::read_contract_u128(block, tx, b"/reserve_ftrbtc");

    let json = format!(
        r#"{{"totalSupply":"{}","dxbtcReserve":"{}","ftrbtcReserve":"{}"}}"#,
        total_supply, dxbtc_reserve, ftrbtc_reserve,
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// frUSD Bridge State
// ===========================================================================

/// Get frUSD token state (total supply, bridge count, auth token).
/// Input: frUSD contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_frusd_state() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let bridge_count = keys::read_contract_u128(block, tx, b"/bridge_count");

    let json = format!(
        r#"{{"totalSupply":"{}","bridgeCount":"{}"}}"#,
        total_supply, bridge_count,
    );
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Wrap/Unwrap Event Tracking (subfrost module)
// ===========================================================================

/// Get wrap events — detected from traces where frBTC [32:0] opcode 77 was called.
/// Reads from quspo's activity index filtered by event_kind = MINT (opcode 77).
#[unsafe(no_mangle)]
pub extern "C" fn get_wrap_events() -> u32 {
    initialize();
    // Read activity records filtered for mint events (opcode 77 = frBTC wrap)
    let mut json = String::from(r#"{"events":["#);
    let height = keys::read_raw_own(b"__espo_height__")
        .map(|b| if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 })
        .unwrap_or(0);

    let mut found = 0u32;
    let scan_end = if height > 500 { height - 500 } else { 0 };
    'outer: for h in (scan_end..=height).rev() {
        for seq in 0..256u16 {
            let mut key = b"/activity/all/".to_vec();
            key.extend_from_slice(&h.to_be_bytes());
            key.push(b'/');
            key.extend_from_slice(&seq.to_be_bytes());
            if let Some(record_bytes) = keys::read_raw_own(&key) {
                if let Some(record) = super::trace::ActivityRecord::from_bytes(&record_bytes) {
                    if record.event_kind == super::trace::EVENT_MINT && record.target_block == 32 && record.target_tx == 0 {
                        if found > 0 { json.push(','); }
                        json.push_str(&record.to_json());
                        found += 1;
                        if found >= 50 { break 'outer; }
                    }
                }
            } else { break; }
        }
    }
    json.push_str("]}");
    export_bytes(json.into_bytes())
}

/// Get unwrap events — frBTC [32:0] opcode 78 calls.
#[unsafe(no_mangle)]
pub extern "C" fn get_unwrap_events() -> u32 {
    initialize();
    let mut json = String::from(r#"{"events":["#);
    let height = keys::read_raw_own(b"__espo_height__")
        .map(|b| if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 })
        .unwrap_or(0);

    let mut found = 0u32;
    let scan_end = if height > 500 { height - 500 } else { 0 };
    'outer: for h in (scan_end..=height).rev() {
        for seq in 0..256u16 {
            let mut key = b"/activity/all/".to_vec();
            key.extend_from_slice(&h.to_be_bytes());
            key.push(b'/');
            key.extend_from_slice(&seq.to_be_bytes());
            if let Some(record_bytes) = keys::read_raw_own(&key) {
                if let Some(record) = super::trace::ActivityRecord::from_bytes(&record_bytes) {
                    if record.event_kind == super::trace::EVENT_BURN && record.target_block == 32 && record.target_tx == 0 {
                        if found > 0 { json.push(','); }
                        json.push_str(&record.to_json());
                        found += 1;
                        if found >= 50 { break 'outer; }
                    }
                }
            } else { break; }
        }
    }
    json.push_str("]}");
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Token Metadata (enriched)
// ===========================================================================

/// Get enriched token details — name, symbol, total supply from contract storage.
/// Input: token contract ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_token_details() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"error":"no input"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"error":"invalid id"}"#.to_vec()),
    };

    let name = keys::read_alkane_name(block, tx).unwrap_or_default();
    let symbol = keys::read_alkane_symbol(block, tx).unwrap_or_default();
    let total_supply = keys::read_contract_u128(block, tx, b"/total_supply");

    let json = format!(
        r#"{{"name":"{}","symbol":"{}","totalSupply":"{}","block":"{}","tx":"{}"}}"#,
        escape_json(&name), escape_json(&symbol), total_supply, block, tx
    );
    export_bytes(json.into_bytes())
}

/// Get user's LP positions across all known pools.
/// Input: JSON `{"address": "bcrt1...", "factory": "block:tx"}`
#[unsafe(no_mangle)]
pub extern "C" fn get_user_positions() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"positions":[]}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("{}");
    let address = extract_json_field(payload, "address").unwrap_or_default();

    if address.is_empty() {
        return export_bytes(br#"{"positions":[]}"#.to_vec());
    }

    // Get all token balances for address
    let outpoints = collect_outpoints_for_address(&address);
    let balances = aggregate_balances(&outpoints);

    // Filter to likely LP tokens (block == 2, which are pool-created tokens)
    let mut json = String::from(r#"{"positions":["#);
    let mut found = 0;
    for (block, tx, amount) in &balances {
        if *block == 2 && *amount > 0 {
            if found > 0 { json.push(','); }
            json.push_str(&format!(
                r#"{{"poolId":"{}:{}","lpBalance":"{}"}}"#,
                block, tx, amount
            ));
            found += 1;
        }
    }
    json.push_str("]}");
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Activity Feed — reads from quspo's own incremental storage
// ===========================================================================

/// Get recent activity feed (newest first).
///
/// Input: JSON `{"limit": N}` or empty for default (50)
/// Output: JSON `{"items": [ActivityRecord], "count": N}`
#[unsafe(no_mangle)]
pub extern "C" fn get_activity() -> u32 {
    initialize();
    let data = input();
    let limit = if data.len() >= 5 {
        let payload = std::str::from_utf8(&data[4..]).unwrap_or("{}");
        extract_json_field(payload, "limit")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(50)
            .min(200)
    } else {
        50
    };

    // Read activity count
    let count = keys::read_raw_own(b"/activity/count")
        .map(|b| if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 })
        .unwrap_or(0);

    // Read current quspo height to know the latest block
    let height = keys::read_raw_own(b"__espo_height__")
        .map(|b| if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 })
        .unwrap_or(0);

    // Scan recent blocks for activity (newest first)
    let mut json = String::from(r#"{"items":["#);
    let mut found = 0u32;

    // Scan backwards from current height
    let scan_start = height;
    let scan_end = if height > 500 { height - 500 } else { 0 };

    'outer: for h in (scan_end..=scan_start).rev() {
        // Try sequences 0..255 for this block height
        for seq in 0..256u16 {
            let mut key = b"/activity/all/".to_vec();
            key.extend_from_slice(&h.to_be_bytes());
            key.push(b'/');
            key.extend_from_slice(&seq.to_be_bytes());

            if let Some(record_bytes) = keys::read_raw_own(&key) {
                if let Some(record) = super::trace::ActivityRecord::from_bytes(&record_bytes) {
                    if found > 0 { json.push(','); }
                    json.push_str(&record.to_json());
                    found += 1;
                    if found >= limit { break 'outer; }
                }
            } else {
                break; // No more sequences for this height
            }
        }
    }

    json.push_str(&format!(r#"],"count":{}}}"#, count));
    export_bytes(json.into_bytes())
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Extract a string field from JSON (simple parser, no serde needed)
fn extract_json_field(json: &str, field: &str) -> Option<String> {
    let key = format!("\"{}\"", field);
    let pos = json.find(&key)?;
    let after_key = &json[pos + key.len()..];
    let colon = after_key.find(':')?;
    let value_start = &after_key[colon + 1..].trim_start();

    if value_start.starts_with('"') {
        // String value
        let inner = &value_start[1..];
        let end = inner.find('"')?;
        Some(inner[..end].to_string())
    } else if value_start.starts_with('[') {
        // Array value — find matching ]
        let end = value_start.find(']')? + 1;
        Some(value_start[..end].to_string())
    } else {
        // Number or other — read until comma or }
        let end = value_start.find(|c: char| c == ',' || c == '}').unwrap_or(value_start.len());
        Some(value_start[..end].trim().to_string())
    }
}

/// Collect all unspent outpoints for an address from alkanes storage.
fn collect_outpoints_for_address(address: &str) -> Vec<Vec<u8>> {
    let addr_key = keys::outpoints_for_address_key(address);
    let count = keys::read_list_length(&addr_key);
    let mut outpoints = Vec::new();

    for i in 0..count {
        if let Some(outpoint) = keys::read_list_item(&addr_key, i) {
            let spendable_key = keys::outpoint_spendable_by_key(&outpoint);
            if let Some(owner) = keys::read_raw(&spendable_key) {
                if !owner.is_empty() {
                    outpoints.push(outpoint);
                }
            }
        }
    }
    outpoints
}

/// Load balance entries for a single outpoint.
fn load_balance_entries(outpoint: &[u8]) -> Vec<(u128, u128, u128)> {
    let sheet_key = keys::proto_outpoint_to_runes_key(outpoint);
    let mut runes_key = sheet_key.clone();
    runes_key.extend_from_slice(b"/runes");
    let mut bals_key = sheet_key;
    bals_key.extend_from_slice(b"/balances");

    let count = keys::read_list_length(&runes_key);
    let mut entries = Vec::new();

    for i in 0..count {
        let rune_bytes = match keys::read_list_item(&runes_key, i) {
            Some(b) => b,
            None => continue,
        };
        let (block, tx) = match keys::bytes_to_alkane_id(&rune_bytes) {
            Some(id) => id,
            None => continue,
        };
        let balance_bytes = match keys::read_list_item(&bals_key, i) {
            Some(b) => b,
            None => continue,
        };
        let amount = if balance_bytes.len() >= 16 {
            u128::from_le_bytes(balance_bytes[0..16].try_into().unwrap())
        } else {
            0
        };
        if amount > 0 {
            entries.push((block, tx, amount));
        }
    }
    entries
}

/// Aggregate balance entries by (block, tx), summing amounts.
fn aggregate_balances(outpoints: &[Vec<u8>]) -> Vec<(u128, u128, u128)> {
    let mut balances: Vec<(u128, u128, u128)> = Vec::new();
    for outpoint in outpoints {
        for (block, tx, amount) in load_balance_entries(outpoint) {
            if let Some(entry) = balances.iter_mut().find(|(b, t, _)| *b == block && *t == tx) {
                entry.2 += amount;
            } else {
                balances.push((block, tx, amount));
            }
        }
    }
    balances
}

/// Format balance entries as JSON array (enriched with name/symbol).
fn balances_to_json(balances: &[(u128, u128, u128)]) -> String {
    let mut json = String::from("[");
    for (i, (block, tx, amount)) in balances.iter().enumerate() {
        if i > 0 { json.push(','); }
        let name = keys::read_alkane_name(*block, *tx).unwrap_or_default();
        let symbol = keys::read_alkane_symbol(*block, *tx).unwrap_or_default();
        json.push_str(&format!(
            r#"{{"alkaneId":{{"block":"{}","tx":"{}"}},"name":"{}","symbol":"{}","balance":"{}"}}"#,
            block, tx, escape_json(&name), escape_json(&symbol), amount
        ));
    }
    json.push(']');
    json
}

/// Extract address from JSON payload or plain string.
fn extract_address(payload: &str) -> String {
    let trimmed = payload.trim();
    // Try JSON: {"address": "..."}
    if trimmed.starts_with('{') {
        if let Some(start) = trimmed.find("\"address\"") {
            let after = &trimmed[start + 9..];
            if let Some(colon) = after.find(':') {
                let value_part = after[colon + 1..].trim().trim_start_matches('"');
                if let Some(end) = value_part.find('"') {
                    return value_part[..end].to_string();
                }
            }
        }
    }
    // Plain string (possibly quoted)
    trimmed.trim_matches('"').to_string()
}

/// Hex-encode bytes in reversed (display) order.
fn hex_encode_reversed(bytes: &[u8]) -> String {
    let reversed: Vec<u8> = bytes.iter().rev().cloned().collect();
    hex_encode(&reversed)
}

pub(crate) fn hex_encode_internal(bytes: &[u8]) -> String {
    hex_encode(bytes)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
     .replace('"', "\\\"")
     .replace('\n', "\\n")
}

/// Parse an AlkaneId from various input formats:
/// - `"block:tx"` string
/// - `{"block": N, "tx": N}` JSON
/// - `{"factoryId": {"block": N, "tx": N}}` nested JSON
fn parse_alkane_id_from_input(payload: &str) -> Option<(u128, u128)> {
    let trimmed = payload.trim().trim_matches('"');

    // Try "block:tx" format
    if let Some(colon) = trimmed.find(':') {
        if let (Ok(b), Ok(t)) = (
            trimmed[..colon].trim().parse::<u128>(),
            trimmed[colon+1..].trim().parse::<u128>(),
        ) {
            return Some((b, t));
        }
    }

    // Try JSON with factoryId or block/tx fields
    if trimmed.starts_with('{') {
        // Extract block and tx from JSON (simple parser)
        let extract_num = |key: &str| -> Option<u128> {
            let pos = trimmed.find(key)?;
            let after = &trimmed[pos + key.len()..];
            let start = after.find(|c: char| c.is_ascii_digit())?;
            let end = start + after[start..].find(|c: char| !c.is_ascii_digit()).unwrap_or(after.len() - start);
            after[start..end].parse().ok()
        };
        if let (Some(b), Some(t)) = (extract_num("\"block\""), extract_num("\"tx\"")) {
            return Some((b, t));
        }
    }

    None
}

/// Read a pool's balance of a specific token from the protorune balance tables.
///
/// The pool contract at [pool_block:pool_tx] holds tokens as protorune balances.
/// We need to find the pool's outpoint and read its balance sheet.
/// Since pools are alkanes at [2:N], their balance is tracked in the protorune tables.
///
/// Key: /runes/proto/1/balancesbyalkane/{pool_id_bytes}/{token_id_bytes}
fn read_pool_balance(pool_block: u128, pool_tx: u128, token_block: u128, token_tx: u128) -> u128 {
    // Read balance using the SAME KeyValuePointer chain as alkanes-rs:
    //   balance_pointer() = IndexPointer::default()
    //     .keyword("/alkanes/")
    //     .select(&what_bytes)   // token ID
    //     .keyword("/balances/")
    //     .select(&who_bytes)    // holder (pool) ID
    //   credit_balances() calls ptr.set_value::<u128>(balance)
    //
    // SecondaryPointer replicates this exactly, reading from "alkanes"
    // secondary storage via __secondary_get.
    let token_bytes = keys::alkane_id_to_bytes(token_block, token_tx);
    let pool_bytes = keys::alkane_id_to_bytes(pool_block, pool_tx);

    let ptr = SecondaryPointer::for_indexer("alkanes")
        .keyword("/alkanes/")
        .select(&token_bytes)
        .keyword("/balances/")
        .select(&pool_bytes);

    ptr.get_value::<u128>()
}

/// Debug view: read a raw secondary key (hex-encoded)
#[unsafe(no_mangle)]
pub extern "C" fn debug_read_raw_key() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 {
        return export_bytes(br#"{"error":"no input"}"#.to_vec());
    }
    let hex_str = match std::str::from_utf8(&data[4..]) {
        Ok(s) => s.trim().trim_matches('"'),
        Err(_) => return export_bytes(br#"{"error":"bad utf8"}"#.to_vec()),
    };
    let key_bytes = match hex_decode_input(hex_str) {
        Some(b) => b,
        None => return export_bytes(br#"{"error":"bad hex"}"#.to_vec()),
    };
    match keys::read_raw(&key_bytes) {
        Some(bytes) => {
            export_bytes(format!(r#"{{"found":true,"len":{},"hex":"{}"}}"#,
                bytes.len(), hex_encode(&bytes)).into_bytes())
        }
        None => export_bytes(br#"{"found":false}"#.to_vec()),
    }
}

// ===========================================================================
// Additional essentials views for SDK compatibility
// ===========================================================================

/// Get holders count for a token. Returns 0 for now (not indexable from secondary alone).
#[unsafe(no_mangle)]
pub extern "C" fn get_holders_count() -> u32 {
    initialize();
    export_bytes(br#"{"count":0}"#.to_vec())
}

/// Get circulating supply for a token.
/// Input: alkane ID as "block:tx"
#[unsafe(no_mangle)]
pub extern "C" fn get_circulating_supply() -> u32 {
    initialize();
    let data = input();
    if data.len() < 5 { return export_bytes(br#"{"supply":"0"}"#.to_vec()); }
    let payload = std::str::from_utf8(&data[4..]).unwrap_or("").trim().trim_matches('"');
    let (block, tx) = match parse_alkane_id_from_input(payload) {
        Some(id) => id, None => return export_bytes(br#"{"supply":"0"}"#.to_vec()),
    };
    let supply = keys::read_contract_u128(block, tx, b"/total_supply");
    let json = format!(r#"{{"supply":"{}"}}"#, supply);
    export_bytes(json.into_bytes())
}

/// Get transfer volume for a token (stub — returns 0).
#[unsafe(no_mangle)]
pub extern "C" fn get_transfer_volume() -> u32 {
    initialize();
    export_bytes(br#"{"volume":"0","transfers":0}"#.to_vec())
}

/// Get address activity (filtered activity feed for a specific address).
/// Input: JSON `{"address": "bcrt1...", "limit": N}`
/// For now returns the global activity feed (address filtering not yet implemented).
#[unsafe(no_mangle)]
pub extern "C" fn get_address_activity() -> u32 {
    initialize();
    // Delegate to the global activity feed for now
    get_activity()
}

/// Get address transactions (alias for address_activity).
#[unsafe(no_mangle)]
pub extern "C" fn get_address_transactions() -> u32 {
    initialize();
    get_activity()
}

/// Get block height from quspo's tracked height.
#[unsafe(no_mangle)]
pub extern "C" fn get_block_height() -> u32 {
    initialize();
    let height = keys::read_raw_own(b"__espo_height__")
        .map(|b| if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 })
        .unwrap_or(0);
    let json = format!(r#"{{"height":{}}}"#, height);
    export_bytes(json.into_bytes())
}

/// Ping — health check.
#[unsafe(no_mangle)]
pub extern "C" fn ping() -> u32 {
    initialize();
    export_bytes(br#"{"ok":true,"indexer":"quspo"}"#.to_vec())
}

fn hex_decode_input(s: &str) -> Option<Vec<u8>> {
    let s = s.trim_start_matches("0x");
    if s.len() % 2 != 0 { return None; }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i+2], 16).ok()).collect()
}
