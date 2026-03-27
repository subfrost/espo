//! Alkanes secondary storage key construction.
//!
//! Replicates the key layout from `protorune/src/tables.rs` and
//! `metashrew-support/src/index_pointer.rs` so we can read alkanes state
//! via `secondary_get("alkanes", key)`.
//!
//! Key construction rules (metashrew IndexPointer):
//! - `IndexPointer::from_keyword(s)` → key = s.as_bytes()
//! - `.keyword(s)` → key = parent_key ++ s.as_bytes()
//! - `.select(bytes)` → key = parent_key ++ bytes
//! - `.select_index(i)` → key = parent_key ++ "/{i}".as_bytes()
//! - `.length()` reads from key ++ "/length"
//!
//! List storage:
//! - `{ptr}/length` → u32 LE count
//! - `{ptr}/{index}` → element bytes

use qubitcoin_tertiary_support::secondary_get;

const ALKANES: &str = "alkanes";

/// Protocol tag for alkanes (protorune tag = 1).
const PROTO_TAG: u128 = 1;

// -- Key prefix builders --

/// `/outpoint/byaddress/{address_bytes}`
pub fn outpoints_for_address_key(address: &str) -> Vec<u8> {
    let mut key = b"/outpoint/byaddress/".to_vec();
    key.extend_from_slice(address.as_bytes());
    key
}

/// `/outpoint/spendableby/{outpoint_bytes}`
pub fn outpoint_spendable_by_key(outpoint: &[u8]) -> Vec<u8> {
    let mut key = b"/outpoint/spendableby/".to_vec();
    key.extend_from_slice(outpoint);
    key
}

/// `/runes/proto/1/byoutpoint/{outpoint_bytes}`
pub fn proto_outpoint_to_runes_key(outpoint: &[u8]) -> Vec<u8> {
    let prefix = format!("/runes/proto/{}/byoutpoint/", PROTO_TAG);
    let mut key = prefix.into_bytes();
    key.extend_from_slice(outpoint);
    key
}

/// `/output/byoutpoint/{outpoint_bytes}`
pub fn outpoint_to_output_key(outpoint: &[u8]) -> Vec<u8> {
    let mut key = b"/output/byoutpoint/".to_vec();
    key.extend_from_slice(outpoint);
    key
}

// -- List reading helpers --

/// Read the length of a list at `key` from alkanes storage.
/// Metashrew/alkanes stores list length at `{key}/length`.
pub fn read_list_length(key: &[u8]) -> u32 {
    let mut len_key = Vec::with_capacity(key.len() + 7);
    len_key.extend_from_slice(key);
    len_key.extend_from_slice(b"/length");
    match secondary_get(ALKANES, &len_key) {
        Some(bytes) if bytes.len() >= 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
        _ => 0,
    }
}

/// Read item at `index` from a list at `key` from alkanes storage.
/// Metashrew/alkanes stores items at `{key}/{index}` (string format).
pub fn read_list_item(key: &[u8], index: u32) -> Option<Vec<u8>> {
    let suffix = format!("/{}", index);
    let mut item_key = Vec::with_capacity(key.len() + suffix.len());
    item_key.extend_from_slice(key);
    item_key.extend_from_slice(suffix.as_bytes());
    secondary_get(ALKANES, &item_key)
}

/// Read a raw value from alkanes storage.
pub fn read_raw(key: &[u8]) -> Option<Vec<u8>> {
    secondary_get(ALKANES, key)
}

/// Read a raw value from quspo's OWN tertiary storage.
pub fn read_raw_own(key: &[u8]) -> Option<Vec<u8>> {
    let data = qubitcoin_tertiary_support::get(std::sync::Arc::new(key.to_vec()));
    if data.is_empty() { None } else { Some(data.as_ref().clone()) }
}

// -- AlkaneId helpers --

/// Encode an AlkaneId (block, tx) as 32 bytes: block_u128_le ++ tx_u128_le.
pub fn alkane_id_to_bytes(block: u128, tx: u128) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.extend_from_slice(&block.to_le_bytes());
    v.extend_from_slice(&tx.to_le_bytes());
    v
}

/// Decode an AlkaneId from 32 bytes.
pub fn bytes_to_alkane_id(bytes: &[u8]) -> Option<(u128, u128)> {
    if bytes.len() < 32 {
        return None;
    }
    let block = u128::from_le_bytes(bytes[0..16].try_into().ok()?);
    let tx = u128::from_le_bytes(bytes[16..32].try_into().ok()?);
    Some((block, tx))
}

/// Read a u128 value from alkanes storage at the given key.
pub fn read_u128(key: &[u8]) -> u128 {
    match secondary_get(ALKANES, key) {
        Some(bytes) if bytes.len() >= 16 => {
            u128::from_le_bytes(bytes[0..16].try_into().unwrap())
        }
        _ => 0,
    }
}

/// Read an alkane's etching name from the proto rune table.
/// Key: /runes/proto/1/etching/byruneid/{alkane_id_bytes}
pub fn read_alkane_name(block: u128, tx: u128) -> Option<String> {
    let id_bytes = alkane_id_to_bytes(block, tx);
    let prefix = format!("/runes/proto/{}/etching/byruneid/", PROTO_TAG);
    let mut key = prefix.into_bytes();
    key.extend_from_slice(&id_bytes);
    let data = secondary_get(ALKANES, &key)?;
    String::from_utf8(data).ok()
}

/// Read an alkane's symbol from the proto rune table.
/// Key: /runes/proto/1/symbol/{alkane_id_bytes}
pub fn read_alkane_symbol(block: u128, tx: u128) -> Option<String> {
    let id_bytes = alkane_id_to_bytes(block, tx);
    let prefix = format!("/runes/proto/{}/symbol/", PROTO_TAG);
    let mut key = prefix.into_bytes();
    key.extend_from_slice(&id_bytes);
    let data = secondary_get(ALKANES, &key)?;
    String::from_utf8(data).ok()
}

// -- Contract storage access --
//
// Alkanes contract storage is at:
//   /alkanes/{alkane_id_32bytes}/storage/{contract_key_bytes}
//
// This lets us read factory pool registry, pool reserves, etc.

/// Build a contract storage key.
/// Path: /alkanes/{block_u128_le}{tx_u128_le}/storage/{storage_key}
pub fn contract_storage_key(block: u128, tx: u128, storage_key: &[u8]) -> Vec<u8> {
    let mut key = b"/alkanes/".to_vec();
    key.extend_from_slice(&alkane_id_to_bytes(block, tx));
    key.extend_from_slice(b"/storage/");
    key.extend_from_slice(storage_key);
    key
}

/// Read a u128 from contract storage.
pub fn read_contract_u128(block: u128, tx: u128, storage_key: &[u8]) -> u128 {
    let key = contract_storage_key(block, tx, storage_key);
    read_u128(&key)
}

/// Read raw bytes from contract storage.
pub fn read_contract_raw(block: u128, tx: u128, storage_key: &[u8]) -> Option<Vec<u8>> {
    let key = contract_storage_key(block, tx, storage_key);
    secondary_get(ALKANES, &key)
}

/// Read an AlkaneId from contract storage.
pub fn read_contract_alkane_id(block: u128, tx: u128, storage_key: &[u8]) -> Option<(u128, u128)> {
    let data = read_contract_raw(block, tx, storage_key)?;
    bytes_to_alkane_id(&data)
}

/// Read a contract storage list length.
/// StoragePointer lists use `{key}/length` with u128 LE count.
pub fn read_contract_list_length(block: u128, tx: u128, storage_key: &[u8]) -> u128 {
    let mut len_key = storage_key.to_vec();
    len_key.extend_from_slice(b"/length");
    read_contract_u128(block, tx, &len_key)
}

/// Read a contract storage list item.
/// StoragePointer lists use `{key}/{index_u128_le}` for items.
pub fn read_contract_list_item(block: u128, tx: u128, storage_key: &[u8], index: u128) -> Option<Vec<u8>> {
    let mut item_key = storage_key.to_vec();
    item_key.extend_from_slice(b"/");
    item_key.extend_from_slice(&index.to_le_bytes());
    read_contract_raw(block, tx, &item_key)
}
